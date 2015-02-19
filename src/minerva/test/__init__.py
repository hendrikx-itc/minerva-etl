import logging
from contextlib import closing
from functools import wraps

import psycopg2.extras

from minerva.util.debug import log_call_basic


def eq_(a, b):
    assert a == b, '{} != {}'.format(a, b)


def ok_(e):
    assert e is True, '{} is not True'.format(e)


def assert_not_equal(a, b):
    assert a != b, '{} == {}'.format(a, b)


def raises(exception_type):
    def fn(f):
        def wrapped(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Exception as exc:
                eq_(type(exc), exception_type)

        return wrapped()

    return fn


def connect():
    conn = psycopg2.connect('', connection_factory=psycopg2.extras.LoggingConnection)

    conn.initialize(logging.root)

    conn.commit = log_call_basic(conn.commit)
    conn.rollback = log_call_basic(conn.rollback)

    return conn


def with_conn(*setup_functions):
    def dec_fn(f):
        """
        Decorator for functions that require a database connection:

        @with_conn
        def somefunction(conn):
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


def with_dataset(dataset):
    def dec_fn(f):
        @wraps(f)
        def wrapper(conn, *args, **kwargs):
            with closing(conn.cursor()) as cursor:
                d = dataset()
                d.load(cursor)

            conn.commit()

            return f(conn, d, *args, **kwargs)

        return wrapper

    return dec_fn


def clear_database(conn):
    with closing(conn.cursor()) as cursor:
        cursor.execute("DELETE FROM trend_directory.trend CASCADE")
        cursor.execute("DELETE FROM trend_directory.trend_store CASCADE")
        cursor.execute("DELETE FROM directory.data_source CASCADE")
        cursor.execute("DELETE FROM directory.entity_type CASCADE")
