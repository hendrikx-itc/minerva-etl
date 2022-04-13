# -*- coding: utf-8 -*-
from contextlib import closing
import unittest

import psycopg2

from minerva.test import connect
from minerva.db.error import (
    NoSuchTable,
    NoSuchColumnError,
    DuplicateTable,
    DataTypeMismatch,
    translate_postgresql_exception,
    translate_postgresql_exceptions,
)


class TestError(unittest.TestCase):
    def test_translate_postgresql_exception(self):
        """
        The translated exception should be NoSuchTable
        """
        query = 'SELECT 1 FROM "non-existing-table"'

        with closing(connect()) as conn:
            with closing(conn.cursor()) as cursor:
                with self.assertRaises(NoSuchTable):
                    try:
                        cursor.execute(query)
                    except psycopg2.DatabaseError as exc:
                        raise translate_postgresql_exception(exc)

    def test_translate_postgresql_exception_decorated(self):
        """
        The translate decorator should do the some as plain translation calls
        """

        @translate_postgresql_exceptions
        def f():
            query = 'SELECT 1 FROM "non-existing-table"'

            with closing(connect()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(query)

        with self.assertRaises(NoSuchTable):
            f()

    def test_no_such_column_error(self):
        @translate_postgresql_exceptions
        def f():
            create_table_query = "CREATE TABLE test(id integer, name text)"

            select_query = 'SELECT "non-existing-column" FROM test'

            with closing(connect()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(create_table_query)
                    cursor.execute(select_query)

        with self.assertRaises(NoSuchColumnError):
            f()

    def test_data_type_mismatch_error(self):
        @translate_postgresql_exceptions
        def f():
            create_table_query = "CREATE TABLE test(id integer, name text)"

            insert_query = "INSERT INTO test(id, name) VALUES (%s, %s)"

            with closing(connect()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(create_table_query)
                    cursor.execute(insert_query, ("first", "bob"))

        with self.assertRaises(DataTypeMismatch):
            f()

    def test_duplicate_table_error(self):
        @translate_postgresql_exceptions
        def f():
            create_table_query = "CREATE TABLE test(id integer, name text)"

            with closing(connect()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(create_table_query)
                    cursor.execute(create_table_query)

        with self.assertRaises(DuplicateTable):
            f()
