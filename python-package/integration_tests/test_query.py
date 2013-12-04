# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from StringIO import StringIO
from contextlib import closing

from nose.tools import eq_

from minerva.test import with_conn
from minerva.db.query import Table, table_exists, Column, Copy, Eq


def with_cursor(conn, f):
    with closing(conn.cursor()) as cursor:
        return f(cursor)


@with_conn()
def test_table_create(conn):
    table = Table("test_table")

    query = table.create()

    with_cursor(conn, query.execute)

    with closing(conn.cursor()) as cursor:
        eq_(table_exists(cursor, table), True)


@with_conn()
def test_table_drop(conn):
    table = Table("test_table")

    create_query = table.create()
    drop_query = table.drop()

    with closing(conn.cursor()) as cursor:
        create_query.execute(cursor)

        eq_(table_exists(cursor, table), True)

        drop_query.execute(cursor)

        eq_(table_exists(cursor, table), False)


@with_conn()
def test_copy_from(conn):
    data = (
        "1\tfirst\n"
        "2\tsecond\n"
        "3\tthird\n")

    stream = StringIO(data)
    columns = [
        Column("id", type_="integer"),
        Column("name", type_="character varying")]

    table = Table("test_table", columns=columns)

    create_query = table.create()

    copy_action = Copy(table).columns(columns).from_(stream)

    with closing(conn.cursor()) as cursor:
        create_query.execute(cursor)

        copy_action.execute(cursor)

        select = table.select([Column("id"), Column("name")])

        query_by_id = select.where_(Eq(Column("id")))
        query_by_name = select.where_(Eq(Column("name")))

        query_by_id.execute(cursor, (2,))

        id, name = cursor.fetchone()

        eq_(name, "second")

        query_by_name.execute(cursor, ("third",))

        id, name = cursor.fetchone()

        eq_(id, 3)
