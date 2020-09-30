import logging
from contextlib import closing, contextmanager
from functools import wraps

import psycopg2.extras

from minerva.util.debug import log_call_basic


def connect():
    conn = psycopg2.connect(
        '', connection_factory=psycopg2.extras.LoggingConnection, connect_timeout=3
    )

    conn.initialize(logging.root)

    conn.commit = log_call_basic(conn.commit)
    conn.rollback = log_call_basic(conn.rollback)

    return conn


def with_conn(*setup_functions):
    def dec_fn(f):
        """
        Decorator for functions that require a database connection:

        @with_conn
        def some_function(conn):
            ...
        """
        @wraps(f)
        def wrapper(*args, **kwargs):
            with closing(connect()) as conn:
                for setup_fn in setup_functions:
                    setup_fn(conn)

                return f(conn, *args, **kwargs)

        return wrapper

    return dec_fn


def clear_database(conn):
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM trend_directory.table_trend CASCADE")
        cursor.execute(
            "SELECT trend_directory.delete_trend_store(id) "
            "FROM trend_directory.trend_store"
        )
        cursor.execute("DELETE FROM trend_directory.trend_store CASCADE")
        cursor.execute("DELETE FROM directory.data_source CASCADE")
        remove_entity_types(conn)
        cursor.execute("DELETE FROM directory.entity_type CASCADE")
        cursor.execute("DELETE FROM directory.tag CASCADE")

    return conn


def remove_entity_types(conn):
    with conn.cursor() as cursor:
        cursor.execute('SELECT name FROM directory.entity_type')

        names = [name for name, in cursor.fetchall()]

        for name in names:
            cursor.execute(
                'DROP FUNCTION IF EXISTS entity."{}"'.format(f'get_{name}')
            )
            cursor.execute(
                'DROP FUNCTION IF EXISTS entity."{}"'.format(f'create_{name}')
            )
            cursor.execute(
                'DROP FUNCTION IF EXISTS entity."{}"'.format(f'to_{name}')
            )


@contextmanager
def with_data_context(conn, test_set):
    data = test_set()

    data.load(conn)

    yield data


def row_count(cursor, table):
    cursor.execute("SELECT COUNT(*) FROM {}".format(table.render()))

    count, = cursor.fetchone()

    return count


