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
        "SELECT MAX(count) FROM (SELECT COUNT(*) AS count "
        "FROM \"{0}\".\"{1}\" "
        "GROUP BY \"{2}\") AS foo "
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
