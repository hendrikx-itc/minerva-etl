# -*- coding: utf-8 -*-
"""
Unit tests for the storing of data packages.
"""
from nose.tools import eq_
from datetime import datetime

import pytz

from minerva.db.query import smart_quote, Schema, Table, Column, Value, \
    And, ands, Or, ors, Parenthesis, Eq, Argument, Select, As, filter_tables, \
    extract_tables, Call, Insert, Function, FromItem, Join


def test_smart_quote():
    """
    Names that require quoting for PostgreSQL should be quoted.
    """
    eq_('name', smart_quote('name'))

    eq_('"Name"', smart_quote('Name'))

    eq_('"name-with-dashes"', smart_quote('name-with-dashes'))


def test_value_int():
    eq_(Value(42).render(), '42')


def test_value_bool():
    eq_(Value(True).render(), 'true')


def test_value_datetime():
    dt = datetime(2013, 1, 5, 17, 0, 0)

    eq_(Value(dt).render(), "'2013-01-05T17:00:00'")


def test_value_datetime_with_timezone():
    tzinfo = pytz.timezone("Europe/Amsterdam")
    dt = tzinfo.localize(datetime(2013, 1, 5, 17, 0, 0))

    eq_(Value(dt).render(), "'2013-01-05T17:00:00+01:00'")


def test_column():
    column = Column("name")

    eq_(column.render(), "name")

    eq_(type(column.as_("other")), As)

    r = column == 45

    eq_(type(r), Eq)

    eq_(r.render(), "name = 45")


def test_and():
    query_part = And(Value(True), Value(False))

    sql = query_part.render()

    eq_(sql, "true AND false")


def test_ands():
    query_part = ands([Value(True), Value(True), Value(False)])

    sql = query_part.render()

    eq_(sql, "true AND true AND false")


def test_or():
    query_part = Or(Value(True), Value(False))

    sql = query_part.render()

    eq_(sql, "true OR false")


def test_or_references():
    query_part = Or(Eq(Column('id'), Value(42)), Value(False))

    references = list(query_part.references())

    eq_(len(references), 1)

    eq_(references[0].name, 'id')


def test_ors():
    query_part = ors([Value(False), Value(True), Value(False)])

    sql = query_part.render()

    eq_(sql, "false OR true OR false")


def test_parenthesis():
    query_part = Parenthesis(And(Value(True), And(Value(True), Value(False))))

    sql = query_part.render()

    eq_(sql, "(true AND true AND false)")


def test_eq():
    query_part = Eq(Value(42), Value(42))

    sql = query_part.render()

    eq_(sql, "42 = 42")


def test_argument():
    query_part = Eq(Value(42), Argument('testarg'))

    sql = query_part.render()

    eq_(sql, "42 = %(testarg)s")


def test_argument_preset():
    query_part = Eq(Value(42), Argument('testarg', Value(55)))

    sql = query_part.render()

    eq_(sql, "42 = 55")


def test_select_fq():
    test_schema = Schema("test")
    dummy_table = Table(test_schema, "dummy")
    columns = [
        Column(dummy_table, "id"),
        Column(dummy_table, "name")]
    query = Select(columns, from_=[dummy_table])

    sql = query.render()

    eq_(sql, 'SELECT test.dummy.id, test.dummy.name FROM test.dummy')


def test_select():
    dummy_table = Table("dummy")

    columns = [
        Column("id"),
        Column("name")]

    query = Select(columns, from_=[dummy_table])

    sql = query.render()

    eq_(sql, 'SELECT id, name FROM dummy')


def test_select_with_alias():
    dummy_table = Table("dummy")
    table_alias = As(dummy_table, "d")

    columns = [
        Column(table_alias, "id"),
        Column(table_alias, "name")]

    query = Select(columns, from_=[table_alias])

    sql = query.render()

    eq_(sql, 'SELECT d.id, d.name FROM dummy AS d')


def test_select_with_where():
    dummy_table = Table("dummy")

    columns = [
        Column("id"),
        Column("name")]

    query = Select(columns, from_=[dummy_table], where_=Eq(Column(
        "id"), Value(25)))

    sql = query.render()

    eq_(sql, 'SELECT id, name FROM dummy WHERE id = 25')


def test_select_with_where_arg():
    dummy_table = Table("dummy")

    columns = [
        Column("id"),
        Column("name")]

    query = Select(columns, from_=[dummy_table], where_=Eq(Column(
        "id"), Argument()))

    sql = query.render()

    eq_(sql, 'SELECT id, name FROM dummy WHERE id = %s')


def test_select_with_group_by():
    dummy_table = Table("dummy")

    col_id = Column("id")
    col_name = Column("name")

    columns = [
        col_id,
        col_name]

    query = Select(columns, from_=[dummy_table], where_=Eq(
        col_name, Argument()), group_by_=[col_name])

    sql = query.render()

    eq_(sql, 'SELECT id, name FROM dummy WHERE name = %s GROUP BY name')


def test_select_chained():
    dummy_table = Table("dummy")

    col_id = Column("id")
    col_name = Column("name")

    columns = [
        col_id,
        col_name]

    query = Select(columns).from_([dummy_table]).where_(Eq(
        col_name, Argument())).group_by_([col_name])

    sql = query.render()

    eq_(sql, 'SELECT id, name FROM dummy WHERE name = %s GROUP BY name')


def test_select_with_join():
    dummy_table_a = Table("dummy_a")
    col_id_a = Column(dummy_table_a, "id")
    col_name = Column(dummy_table_a, "name")
    dummy_table_b = Table("dummy_b")
    col_id_b = Column(dummy_table_b, "id")
    col_amount = Column(dummy_table_b, "amount")

    columns = [col_name, col_amount]

    from_part = Join(dummy_table_a, dummy_table_b, Eq(col_id_a, col_id_b))

    query = Select(columns).from_(from_part)

    sql = query.render()

    eq_(sql, 'SELECT dummy_a.name, dummy_b.amount FROM \
            dummy_a JOIN dummy_b ON dummy_a.id = dummy_b.id')


def test_select_with_left_join():
    dummy_table_a = Table("dummy_a")
    col_id_a = Column(dummy_table_a, "id")
    col_name = Column(dummy_table_a, "name")
    dummy_table_b = Table("dummy_b")
    col_id_b = Column(dummy_table_b, "id")
    col_amount = Column(dummy_table_b, "amount")
    columns = [col_name, col_amount]

    from_part = FromItem(dummy_table_a).left_join(
        dummy_table_b, Eq(col_id_a, col_id_b))

    query = Select(columns).from_(from_part)

    sql = query.render()

    eq_(sql, 'SELECT dummy_a.name, dummy_b.amount FROM \
            dummy_a LEFT JOIN dummy_b ON dummy_a.id = dummy_b.id')


def test_extract_references():
    dummy_table = Table("dummy")

    col_id = Column("id")
    col_name = Column("name")

    columns = [
        col_id,
        col_name]

    query = Select(columns).from_(
        [dummy_table]
    ).where_(Eq(col_name, Argument())).group_by_([col_name])

    references = query.references()

    eq_(len(references), 4)


def test_filter_tables():
    dummy_table = Table("dummy")

    col_id = Column("id")
    col_name = Column("name")

    columns = [
        col_id,
        col_name]

    query = Select(columns).from_([dummy_table]).where_(Eq(
        col_name, Argument())).group_by_([col_name])

    tables = filter_tables(query.references())

    eq_(len(tables), 1)

    eq_(tables[0].name, "dummy")


def test_extract_tables():
    dummy_table = Table("dummy")

    col_id = Column(dummy_table, "id")
    col_name = Column(dummy_table, "name")

    columns = [
        col_id,
        col_name]

    tables = extract_tables(columns)

    eq_(len(tables), 2)

    eq_(tables[0].name, "dummy")


def test_arguments():
    dummy_table = Table("dummy")

    col_id = Column("id")
    col_name = Column("name")

    columns = [
        col_id,
        col_name]

    query = Select(columns).from_([dummy_table]).where_(Eq(
        col_name, Argument())).group_by_([col_name])

    arguments = query.arguments()

    eq_(len(arguments), 1)


def test_call():
    col_a = Column("a")
    col_b = Column("b")

    call = Call("greatest", col_a, col_b)

    eq_(call.render(), "greatest(a, b)")


def test_insert():
    dummy_table = Table("dummy")

    col_id = Column("id")
    col_name = Column("name")

    insert = Insert(dummy_table, (col_id, col_name))

    eq_(insert.render(), "INSERT INTO dummy(id, name) VALUES (%s, %s)")


def test_function():
    dummy_function = Function("add_many")

    col_a = Column("counter_a")
    col_b = Column("counter_b")

    call = dummy_function.call(col_a, col_b)

    eq_(call.render(), "add_many(counter_a, counter_b)")
