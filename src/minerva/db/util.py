# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from functools import partial
from contextlib import closing
import StringIO
import re

from minerva.util import zipapply
from minerva.util.tabulate import render_table
from minerva.db.query import Table


def drop_table(conn, name):
    sql = "DROP TABLE IF EXISTS {}".format(name)

    exec_sql(conn, sql)


def create_temp_table(conn, name, columns):
    columns_part = ",".join(columns)

    sql = "CREATE TEMP TABLE {} ({})".format(name, columns_part)

    exec_sql(conn, sql)


def create_index(conn, table, columns):
    name = "ix_{0}_{1}".format(table, columns[0])

    sql = "CREATE INDEX {0} ON {0} ({2})".format(
        name, table, ",".join(columns)
    )

    exec_sql(conn, sql)


def create_unique_index(conn, table, columns):
    name = "ix_{0}_{1}".format(table, columns[0])

    sql = "CREATE UNIQUE INDEX {0} ON {1} ({2})".format(
        name, table, ",".join(columns))

    exec_sql(conn, sql)


def exec_sql(conn, *args, **kwargs):
    with closing(conn.cursor()) as cursor:
        cursor.execute(*args, **kwargs)


def enquote_column_name(name):
    """
    Add quotes to column name to make sure PostgreSQL understands it.
    :param name: column name (may contain functions, e.g.
    'schema.function_name(col1, col2)' results in
    '"schema"."function_name"("col1", "col2")')
    """
    def enquote(matchobj):
        item = matchobj.group(0)
        try:
            int(item)
        except:
            return "\"{0}\"".format(matchobj.group(0))
        else:
            return item

    return re.sub('([-\w]+)[^ ().,]', enquote, name)


def quote_ident(ident):
    if isinstance(ident, str):
        return quote(ident)
    elif hasattr(ident, "__iter__"):
        return ".".join(map(quote, ident))


quote = partial(str.format, '"{}"')


def create_copy_from_query(table, columns):
    columns_part = ",".join(map(enquote_column_name, columns))

    return "COPY {0}({1}) FROM STDIN".format(table, columns_part)


def create_copy_from_file(tuples, formats):
    formatters = create_formatters(*formats)

    format_tuple = partial(zipapply, formatters)

    copy_from_file = StringIO.StringIO()

    copy_from_file.writelines(
        "{}\n".format("\t".join(format_tuple(tup)))
        for tup in tuples
    )

    copy_from_file.seek(0)

    return copy_from_file


def create_formatters(*args):
    return [partial(str.format, "{:" + f + "}") for f in args]


def stored_procedure(name, conn, *args):
    with closing(conn.cursor()) as cursor:
        cursor.callproc(name, args)


def is_unique(conn, schema, table_name, column):
    """
    Returns True if column contains unique values, False otherwise
    """
    query = (
        'SELECT MAX(count) FROM (SELECT COUNT(*) AS count '
        'FROM "{0}"."{1}" '
        'GROUP BY "{2}") AS foo '
    ).format(schema, table_name, column)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)
        c, = cursor.fetchone()

    return c == 1


def render_result(cursor):
    column_names = [c.name for c in cursor.description]
    column_align = ">" * len(column_names)
    column_sizes = ["max"] * len(column_names)
    rows = cursor.fetchall()

    return render_table(column_names, column_align, column_sizes, rows)


def prepare_statements(conn, statements):
    """
    Prepare statements for the specified connection.
    """
    with closing(conn.cursor()) as cursor:
        for statement in statements:
            cursor.execute("PREPARE {0:s}".format(statement))


def disable_transactions(conn):
    """
    Set isolation level to ISOLATION_LEVEL_AUTOCOMMIT so that every query is
    automatically committed.
    """
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)


def full_table_name(name, schema=None):
    if schema is not None:
        return '"{0:s}"."{1:s}"'.format(schema, name)
    else:
        return '"{0}"'.format(name)


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


def grant(conn, object_type, privileges, object_name, group):
    """
    @param privileges = "CREATE"|"USAGE"|"ALL"
    """

    if hasattr(privileges, "__iter__"):
        privileges = ",".join(privileges)

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute("GRANT {1:s} ON {0:s} {2:s} TO GROUP {3:s}".format(
                object_type, privileges, object_name, group))
        except psycopg2.InternalError as exc:
            conn.rollback()

            # TODO: Find a more elegant way to avoid internal errors in 'GRANT'
            # actions.
            if exc.pgcode == psycopg2.errorcodes.INTERNAL_ERROR:
                pass
        else:
            conn.commit()


def drop_table(conn, schema, table):
    with closing(conn.cursor()) as cursor:
        cursor.execute("DROP TABLE {0:s}".format(
            full_table_name(table, schema)))

    conn.commit()


def drop_all_tables(conn, schema):
    with closing(conn.cursor()) as cursor:
        query = (
            "SELECT table_name FROM information_schema.tables WHERE "
            "table_schema = '{0:s}'"
        ).format(schema)

        cursor.execute(query)

        rows = cursor.fetchall()

        for (table_name, ) in rows:
            cursor.execute(
                "DROP TABLE {0:s} CASCADE".format(
                    full_table_name(table_name, schema)
                )
            )

    conn.commit()


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


def schema_exists(conn, name):
    with closing(conn.cursor()) as cursor:
        query = (
            "SELECT COUNT(*) FROM information_schema.schemata WHERE "
            "schema_name = '{0:s}'"
        ).format(name)

        cursor.execute(query)

        (num, ) = cursor.fetchone()

        return num > 0


def create_schema(conn, name, owner):
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM pg_catalog.pg_namespace "
            "WHERE nspname = '{0:s}';".format(name)
        )

        (num, ) = cursor.fetchone()

        if num == 0:
            cursor.execute(
                "CREATE SCHEMA \"{0:s}\" "
                "AUTHORIZATION {1:s}".format(name, owner)
            )
            conn.commit()
        else:
            raise ExistsError("Schema {0:s} already exists".format(name))


def alter_table_owner(conn, table, owner):
    with closing(conn.cursor()) as cursor:
        cursor.execute("ALTER TABLE {0} OWNER TO {1}".format(table, owner))

    conn.commit()


def create_user(conn, name, password=None, groups=None):
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM pg_catalog.pg_shadow "
            "WHERE usename = '{0:s}';".format(name)
        )

        (num, ) = cursor.fetchone()

        if num == 0:
            if password is None:
                query = "CREATE USER {0:s}".format(name)
            else:
                query = "CREATE USER {0:s} LOGIN PASSWORD '{1:s}'".format(
                    name, password)

            cursor.execute(query)

            if groups is not None:
                if hasattr(groups, "__iter__"):
                    for group in groups:
                        query = "GRANT {0:s} TO {1:s}".format(group, name)
                        cursor.execute(query)
                else:
                    query = "GRANT {0:s} TO {1:s}".format(groups, name)
                    cursor.execute(query)

            conn.commit()


def create_group(conn, name, group=None):
    """
    @param group: parent group
    """
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM pg_catalog.pg_group "
            "WHERE groname = '{0:s}';".format(name)
        )

        (num, ) = cursor.fetchone()

        if num == 0:
            cursor.execute('CREATE ROLE "{0:s}"'.format(name))

        if group is not None:
            query = "GRANT {0:s} TO {1:s}".format(group, name)
            cursor.execute(query)

        conn.commit()


def create_db(conn, name, owner):
    """
    Create a new database using template0 with UTF8 encoding.
    """
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM pg_catalog.pg_database "
            "WHERE datname = '{0:s}';".format(name)
        )

        (num, ) = cursor.fetchone()

        if num == 0:
            cursor.execute(
                'CREATE DATABASE "{0:s}" WITH ENCODING=\'UTF8\' '
                'OWNER="{1:s}" TEMPLATE template0'.format(name, owner)
            )

    conn.commit()


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
