# -*- coding: utf-8 -*-
import re
import copy
from datetime import datetime
from operator import attrgetter
from itertools import chain, groupby
from functools import partial, reduce

from minerva.util import k, if_set


enclose = partial(str.format, "{0[0]}{1}{0[1]}")

parenthesize = partial(enclose, '()')

quote = partial(enclose, '""')


identifier_regex = re.compile("^[a-z][a-z_0-9]*$")


def iter_concat(iterables):
    return list(chain(*iterables))


reserved_keywords = set(["end", "timestamp"])


def smart_quote(name):
    """
    Only quote references that require quoting
    """
    if identifier_regex.match(name) and name not in reserved_keywords:
        return name
    else:
        return quote(name)


class Sql:
    def curry(self, *args, **kwargs):
        get_name = attrgetter("name")
        sorted_arguments = sorted(self.arguments(), key=get_name)

        arguments = dict(
            (key, list(it))
            for key, it in groupby(sorted_arguments, get_name)
        )

        pos_arguments = arguments.get(None, [])

        for argument, a in zip(pos_arguments, args):
            argument.value = a

        for keyword, value in kwargs.iteritems():
            for a in arguments.get(keyword, []):
                a.value = value

        return self

    def render(self, *args, **kwargs):
        """
        Render and return SQL as string
        """
        if len(args) > 0 or len(kwargs.keys()):
            self.curry(*args, **kwargs)

        return self._render()

    def _render(self):
        raise NotImplemented()

    def references(self):
        """
        Return iterable of all references (tables, columns, etc.)
        """
        return tuple()

    def arguments(self):
        """
        Return iterable of all unset arguments
        """
        return tuple()


class Call(Sql):
    def __init__(self, function, *args):
        if isinstance(function, Function):
            self.function = function
        elif isinstance(function, str):
            self.function = Function(function)

        self.args = list(map(ensure_sql, args))

    def render(self):
        args_part = ", ".join(a.render() for a in self.args)

        return "{}({})".format(self.function.render(), args_part)


class SchemaObject(Sql):
    def references(self):
        return self,


class Schema(SchemaObject):
    def __init__(self, name):
        self.name = name

    def _render(self):
        return smart_quote(self.name)


class Script:
    def __init__(self, statements):
        self.statements = statements

    def execute(self, cursor):
        for statement in self.statements:
            cursor.execute(statement)


class Function(SchemaObject):
    def __init__(self, *args, **kwargs):
        self.arguments = kwargs.get("arguments", [])
        self.name = args[-1]

        if len(args) > 1:
            schema = args[-2]

            if isinstance(schema, Schema):
                self.schema = schema
            elif isinstance(schema, str):
                self.schema = Schema(schema)
            else:
                raise Exception("invalid schema '{}'".format(schema))
        else:
            self.schema = None

    def _render(self):
        if self.schema:
            return "{}.{}".format(self.schema.render(), smart_quote(self.name))
        else:
            return smart_quote(self.name)

    def call(self, *args):
        return Call(self, *args)


class Table(SchemaObject):
    def __init__(self, *args, **kwargs):
        self.columns = kwargs.get("columns", [])
        self.name = args[-1]

        if len(args) > 1:
            schema = args[-2]

            if isinstance(schema, Schema):
                self.schema = schema
            elif isinstance(schema, str):
                self.schema = Schema(schema)
            else:
                raise Exception("invalid schema '{}'".format(schema))
        else:
            self.schema = None

    def _render(self):
        if self.schema:
            return "{}.{}".format(self.schema.render(), smart_quote(self.name))
        else:
            return smart_quote(self.name)

    def create(self):
        col_specs = ", ".join([c.definition() for c in self.columns])
        statement = "CREATE TABLE {}({});".format(self.render(), col_specs)

        return Script([statement])

    def insert(self, columns=[]):
        return Insert(self, columns)

    def select(self, columns=[], **kwargs):
        return Select(columns, from_=self, **kwargs)

    def truncate(self, *args, **kwargs):
        return Truncate(self, *args, **kwargs)

    def drop(self, *args, **kwargs):
        return Drop(self, *args, **kwargs)


class SqlType(Sql):
    def __init__(self, name):
        self.name = name

    def _render(self):
        return self.name


class Column(SchemaObject):
    def __init__(self, *args, **kwargs):
        self.name = args[-1]

        if len(args) > 1:
            table = args[-2]

            if isinstance(table, Table):
                self.table = table
            elif isinstance(table, str):
                self.table = Table(table)
            elif isinstance(table, As):
                self.table = Table(table.alias)
            else:
                raise Exception("invalid table '{}'".format(table))
        else:
            self.table = None

        self.type_ = ensure_sql_type(kwargs.get("type_", SqlType("integer")))
        self.nullable = kwargs.get("nullable", False)

    def _render(self):
        if self.table:
            return "{}.{}".format(self.table.render(), smart_quote(self.name))
        else:
            return smart_quote(self.name)

    def definition(self):
        parts = [self.name, self.type_.render()]

        if not self.nullable:
            parts.append("NOT NULL")

        return " ".join(parts)

    def as_(self, alias):
        return As(self, alias)

    def __eq__(self, other):
        return Eq(self, other)

    def __lt__(self, other):
        return Lt(self, other)

    def __le__(self, other):
        return LtEq(self, other)

    def __gt__(self, other):
        return Gt(self, other)

    def __ge__(self, other):
        return GtEq(self, other)


class FormattedValue(Sql):
    def __init__(self, formatter, value):
        self.formatter = formatter
        self.value = value

    def _render(self):
        return self.formatter(self.value)


def format_bool(b):
    if b is True:
        return 'true'
    else:
        return 'false'


def format_datetime(dt):
    return "'{}'".format(dt.isoformat())


class Value(FormattedValue):
    def __init__(self, value):
        if isinstance(value, bool):
            formatter = format_bool
        elif isinstance(value, int):
            formatter = str
        elif isinstance(value, datetime):
            formatter = format_datetime
        else:
            formatter = partial(str.format, "'{}'")

        FormattedValue.__init__(self, formatter, value)


class Literal(Sql):
    def __init__(self, value):
        self._value = value

    def _render(self):
        return self._value


class Argument(Sql):
    def __init__(self, name=None, value=None):
        self.name = name
        self.value = value

    def _render(self):
        if self.value:
            return self.value.render()
        else:
            if self.name:
                return "%({})s".format(self.name)
            else:
                return "%s"

    def references(self):
        if self.value is None:
            return tuple()
        else:
            return self.value.references()

    def arguments(self):
        if self.value is None:
            return self,
        else:
            return tuple()


class Operator(Sql):
    def _render(self):
        raise Exception("not implemented")


class BinaryOperator(Operator):
    def __init__(self, _l=None, r=None):
        if _l is None:
            self.r = Argument()
        elif isinstance(_l, Sql):
            self.l_ = _l
        else:
            self.l_ = Value(_l)

        if r is None:
            self.r = Argument()
        elif isinstance(r, Sql):
            self.r = r
        else:
            self.r = Value(r)

    def references(self):
        return chain(self.l_.references(), self.r.references())

    def arguments(self):
        return chain(self.l_.arguments(), self.r.arguments())


class UnaryOperator(Operator):
    def __init__(self, x=None):
        if x is None:
            self.x = Argument()
        elif isinstance(x, Sql):
            self.x = x
        else:
            self.x = Value(x)


class Any(UnaryOperator):
    def _render(self):
        return "ANY({})".format(self.x.render())


def is_unset_argument(x):
    return isinstance(x, Argument) and x.value is None


class Parenthesis(Sql):
    def __init__(self, expression):
        self.expression = expression

    def _render(self):
        return "({})".format(self.expression.render())


class And(BinaryOperator):
    def _render(self):
        return "{} AND {}".format(self.l_.render(), self.r.render())


ands = partial(reduce, And)


class Or(BinaryOperator):
    def _render(self):
        return "{} OR {}".format(self.l_.render(), self.r.render())


ors = partial(reduce, Or)


class Eq(BinaryOperator):
    def _render(self):
        return "{} = {}".format(self.l_.render(), self.r.render())


class Lt(BinaryOperator):
    def _render(self):
        return "{} < {}".format(self.l_.render(), self.r.render())


class Gt(BinaryOperator):
    def _render(self):
        return "{} > {}".format(self.l_.render(), self.r.render())


class LtEq(BinaryOperator):
    def _render(self):
        return "{} <= {}".format(self.l_.render(), self.r.render())


class GtEq(BinaryOperator):
    def _render(self):
        return "{} >= {}".format(self.l_.render(), self.r.render())


class ArrayContains(BinaryOperator):
    def _render(self):
        return "{} @> {}".format(self.l_.render(), self.r.render())


class ArrayIsContainedBy(BinaryOperator):
    def _render(self):
        return "{} <@ {}".format(self.l_.render(), self.r.render())


class In(BinaryOperator):
    def _render(self):
        return "{} IN {}".format(self.l_.render(), self.r.render())


class As(Sql):
    def __init__(self, source, alias):
        self.source = source
        self.alias = alias

    def _render(self):
        return "{} AS {}".format(self.source.render(), smart_quote(self.alias))

    def references(self):
        if isinstance(self.source, (Table, Column)):
            return self.source,
        else:
            return tuple()


class SqlQuery(Sql):
    def _render(self):
        raise NotImplementedError()

    def execute(self, cursor, args=tuple()):
        sql = self.render()

        return cursor.execute(sql, args)


class LiteralQuery(SqlQuery):
    def __init__(self, query):
        self.query = query

    def _render(self):
        return self.query


def is_iterable(obj):
    return hasattr(obj, "__iter__")


def ensure_iterable(obj):
    if is_iterable(obj):
        return obj
    else:
        return [obj]


def ensure_sql(obj):
    if isinstance(obj, Sql):
        return obj
    else:
        return Value(obj)


def ensure_sql_type(obj):
    if isinstance(obj, SqlType):
        return obj
    else:
        return SqlType(obj)


class Copy:
    def __init__(self, table, columns=None):
        self.table = table
        self._columns = columns
        self.stream = None
        self._exec_fn = None

    def from_(self, stream):
        self.stream = stream
        self._exec_fn = self.exec_from
        return self

    def to(self, stream):
        self.stream = stream
        self._exec_fn = self.exec_to

    def columns(self, columns):
        self._columns = columns

        return self

    def column_names(self):
        get_name = attrgetter("name")

        return if_set(self._columns, partial(map, get_name))

    def execute(self, cursor):
        return self._exec_fn(cursor)

    def exec_from(self, cursor):
        columns = self.column_names()

        cursor.copy_from(self.stream, self.table.render(), columns=columns)

    def exec_to(self, cursor):
        columns = self.column_names()

        cursor.copy_to(self.stream, self.table.render(), columns=columns)


class FromItem(Sql):
    def __init__(self, table):
        self.table = table

    def _render(self):
        return self.table.render()

    def references(self):
        return self.table.references()

    def join(self, *args, **kwargs):
        return Join(self, *args, **kwargs)

    def left_join(self, *args, **kwargs):
        kwargs["join_type"] = "LEFT"

        return Join(self, *args, **kwargs)

    def right_join(self, *args, **kwargs):
        kwargs["join_type"] = "RIGHT"

        return Join(self, *args, **kwargs)


class Join(FromItem):
    def __init__(self, left, right, on=None, join_type=None):
        self.left = ensure_from(left)
        self.right = ensure_from(right)
        self.on = on
        self.join_type = join_type

    def _render(self):
        if self.join_type:
            join = "{} JOIN".format(self.join_type)
        else:
            join = "JOIN"

        return "{} {} {} ON {}".format(
            self.left.render(), join, self.right.render(), self.on.render()
        )

    def as_(self, alias):
        return As(self, alias)

    def on(self, on):
        self.on = on

        return self


def ensure_from(obj):
    if isinstance(obj, FromItem):
        return obj
    else:
        return FromItem(obj)


class WithQuery(SqlQuery):
    def __init__(self, name, columns=[], query=None):
        self.name = name
        self.columns = ensure_iterable(columns)
        self.query = query

    def _render(self):
        if self.columns:
            columns_part = ", ".join([c.render() for c in self.columns])
            return "{} {} AS ({})".format(self.name, columns_part)
        else:
            return "{} AS ({})".format(self.name, self.query.render())


class Select(SqlQuery):
    def __init__(self, expressions, with_query=None, from_=None, where_=None,
                 group_by_=None, limit=None):
        self.expressions = list(map(ensure_sql, ensure_iterable(expressions)))
        self.with_query = with_query
        self.sources = list(map(ensure_from, ensure_iterable(from_)))
        self.requirements = where_
        self._limit = limit

        if group_by_ is None:
            self.group_by = None
        else:
            self.group_by = ensure_iterable(group_by_)

    def clone(self):
        return copy.copy(self)

    def from_(self, sources):
        select = self.clone()
        select.sources = list(map(ensure_from, ensure_iterable(sources)))

        return select

    def where_(self, requirements):
        select = self.clone()
        select.requirements = requirements

        return select

    def group_by_(self, group_by):
        select = self.clone()
        select.group_by = ensure_iterable(group_by)

        return select

    def _render(self):
        expressions_part = ", ".join(e.render() for e in self.expressions)

        parts = []

        if self.with_query:
            with_part = "WITH {}".format(self.with_query.render())
            parts.append(with_part)

        parts.append("SELECT")

        parts.append(expressions_part)

        if self.sources:
            sources_part = "FROM {}".format(
                ", ".join(s.render() for s in self.sources))

            parts.append(sources_part)

        if self.requirements:
            requirements_part = "WHERE {}".format(self.requirements.render())

            parts.append(requirements_part)

        if self.group_by:
            group_by_part = "GROUP BY {}".format(
                ", ".join(g.render() for g in self.group_by))

            parts.append(group_by_part)

        if self._limit:
            parts.append("LIMIT {0:d}".format(self._limit))

        return " ".join(parts)

    def references(self):
        idents = []

        idents.append(chain(*[e.references() for e in self.expressions]))

        if self.sources:
            idents.append(chain(*[s.references() for s in self.sources]))

        if self.requirements:
            idents.append(self.requirements.references())

        return iter_concat(idents)

    def arguments(self):
        arguments = []

        arguments.append(chain(*[e.arguments() for e in self.expressions]))

        if self.sources:
            arguments.append(chain(*[s.arguments() for s in self.sources]))

        if self.requirements:
            arguments.append(self.requirements.arguments())

        return iter_concat(arguments)

    def limit(self, n):
        select = self.clone()
        select._limit = n

        return select


class Insert(SqlQuery):
    def __init__(self, into=None, columns=[]):
        self.into = into
        self._returning = None
        self.columns = columns

    def into(self, table):
        self.into = table

        return self

    def returning(self, column_name):
        self._returning = column_name

        return self

    def render(self):
        columns_part = ", ".join(c.render() for c in self.columns)

        args_part = ", ".join(map(k("%s"), self.columns))

        query = (
            "INSERT INTO {}({}) "
            "VALUES ({})").format(self.into.render(), columns_part, args_part)

        if self._returning:
            query += " RETURNING {}".format(self._returning)

        return query


class Truncate(SqlQuery):
    def __init__(self, table, cascade=False):
        self.table = table
        self._cascade = cascade

    def render(self):
        if self._cascade:
            q = "TRUNCATE {} CASCADE"
        else:
            q = "TRUNCATE {}"

        return q.format(self.table.render())

    def cascade(self):
        self._cascade = True

        return self


class Drop(SqlQuery):
    def __init__(self, schema_obj, if_exists=False):
        self.schema_obj = schema_obj
        self._if_exists = if_exists

    def render(self):
        if isinstance(self.schema_obj, Table):
            if self._if_exists:
                sql = "DROP TABLE IF EXISTS {}".format(
                    self.schema_obj.render())
            else:
                sql = "DROP TABLE {}".format(self.schema_obj.render())

            return sql
        else:
            msg = "Cannot drop object of type {}"

            raise Exception(msg.format(type(self.schema_obj)))

    def if_exists(self):
        self._if_exists = True

        return self


def is_table(ident):
    return isinstance(ident, Table)


def filter_tables(references):
    return list(filter(is_table, references))


def table_exists(cursor, table):
    criterion = And(
        Eq(Column("relname")),
        Eq(Column("relkind"))
    )

    query = Select(1, from_=Table("pg_class"), where_=criterion)

    args = table.name, "r"

    query.execute(cursor, args)

    return cursor.rowcount > 0


def column_exists(cursor, table, column):
    query = (
        "SELECT 1 "
        "FROM pg_class c, pg_attribute a, pg_namespace n "
        "WHERE c.relname = %s "
        "AND n.nspname = %s "
        "AND a.attname = %s "
        "AND a.attrelid = c.oid "
        "AND c.relnamespace = n.oid"
    )

    args = table.name, table.schema.name, column

    cursor.execute(query, args)

    return cursor.rowcount > 0


def extract_tables(references):
    tables = []

    for ref in references:
        if isinstance(ref, Table):
            tables.append(ref)
        elif isinstance(ref, Column) and ref.table:
            tables.append(ref.table)

    return tables
