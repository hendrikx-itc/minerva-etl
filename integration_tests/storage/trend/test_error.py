# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing

import psycopg2
from nose.tools import raises

from minerva.db.error import NoSuchTable, \
        NoSuchColumnError, DuplicateTable, DataTypeMismatch, \
        translate_postgresql_exception, translate_postgresql_exceptions

from minerva_db import connect


@raises(NoSuchTable)
def test_translate_postgresql_exception():
    """
    The translated exception should be NoSuchTable
    """
    query = 'SELECT 1 FROM "non-existing-table"'

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            try:
                cursor.execute(query)
            except psycopg2.DatabaseError as exc:
                raise translate_postgresql_exception(exc)


@raises(NoSuchTable)
@translate_postgresql_exceptions
def test_translate_postgresql_exception_decorated():
    """
    The translate decorator should do the some as plain translation calls
    """
    query = 'SELECT 1 FROM "non-existing-table"'

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query)


@raises(NoSuchColumnError)
@translate_postgresql_exceptions
def test_no_such_column_error():
    create_table_query = (
        "CREATE TABLE test("
        "id integer, "
        "name text)"
    )

    select_query = (
        'SELECT "non-existing-column" '
        'FROM test'
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(create_table_query)
            cursor.execute(select_query)


@raises(DataTypeMismatch)
@translate_postgresql_exceptions
def test_data_type_mismatch_error():
    create_table_query = (
        "CREATE TABLE test("
        "id integer, "
        "name text)"
    )

    insert_query = (
        'INSERT INTO test(id, name) '
        'VALUES (%s, %s)'
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(create_table_query)
            cursor.execute(insert_query, ("first", "bob"))


@raises(DuplicateTable)
@translate_postgresql_exceptions
def test_duplicate_table_error():
    create_table_query = (
        "CREATE TABLE test("
        "id integer, "
        "name text)"
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(create_table_query)
            cursor.execute(create_table_query)
