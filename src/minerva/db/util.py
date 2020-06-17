# -*- coding: utf-8 -*-
from functools import partial
from contextlib import closing
from io import StringIO
from typing import Iterable

from minerva.util.tabulate import render_table
from minerva.db.query import Table
from minerva.util import zip_apply, compose


def exec_sql(conn, *args, **kwargs):
    with closing(conn.cursor()) as cursor:
        cursor.execute(*args, **kwargs)


def stored_procedure(name, conn, *args):
    with closing(conn.cursor()) as cursor:
        cursor.callproc(name, args)


quote_ident = partial(str.format, '"{}"')


def create_copy_from_query(table, columns):
    columns_part = ",".join(map(quote_ident, columns))

    return "COPY {0}({1}) FROM STDIN".format(table, columns_part)


def create_copy_from_lines(tuples, formats):
    format_tuple = zip_apply(
        partial(str.format, "{:" + f + "}")
        for f in formats
    )

    return (
        "{}\n".format("\t".join(format_tuple(tup)))
        for tup in tuples
    )


def create_file(lines: Iterable[str]):
    copy_from_file = StringIO()

    copy_from_file.writelines(lines)

    copy_from_file.seek(0)

    return copy_from_file


create_copy_from_file = compose(create_file, create_copy_from_lines)


def render_result(cursor):
    column_names = [c.name for c in cursor.description]
    column_align = ">" * len(column_names)
    column_sizes = ["max"] * len(column_names)
    rows = cursor.fetchall()

    return render_table(column_names, column_align, column_sizes, rows)


def get_column_names(conn, schema_name, table_name):
    """
    Return list of column names of specified schema and table
    """
    query = (
        "SELECT a.attname FROM pg_attribute a "
        "JOIN pg_class c ON c.oid = a.attrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s "
        "AND a.attnum > 0 AND not attisdropped"
    )

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (schema_name, table_name))
        return [name for name, in cursor.fetchall()]


def table_exists(cursor, schema, table):
    query = "SELECT public.table_exists(%s, %s)"
    args = schema, table

    cursor.execute(query, args)

    (exists, ) = cursor.fetchone()

    return exists


def column_exists(conn, schema, table, column):
    with closing(conn.cursor()) as cursor:
        query = (
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = %s "
            "AND table_name = %s "
            "AND column_name = %s"
        )

        args = schema, table, column

        cursor.execute(query, args)

        (num, ) = cursor.fetchone()

        return num > 0


def create_temp_table_from(cursor, table):
    """
    Create a temporary table that is like `table` and return the temporary
    table name.
    """
    tmp_table = Table("tmp_{0}".format(table.name))

    query = (
        "CREATE TEMPORARY TABLE {0} (LIKE {1}) "
        "ON COMMIT DROP"
    ).format(tmp_table.render(), table.render())

    cursor.execute(query)

    return tmp_table


def is_unique():
    raise NotImplementedError()
