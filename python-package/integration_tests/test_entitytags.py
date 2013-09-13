import os
import time
import logging
from contextlib import closing
from datetime import datetime, timedelta

from pytz import timezone
from nose.tools import eq_, raises, assert_not_equal

from minerva.storage.generic import extract_data_types

from minerva_db import connect, clear_database, get_tables, with_connection


@with_connection
def test_create_alias(conn):
    cursor = conn.cursor()
    clear_database(cursor)

    schema = 'directory'

    query = (
        "INSERT INTO {0}.{1} (id, name, description) VALUES "
        "(1, 'test_type', 'test entity type')").format(schema, "entitytype")

    cursor.execute(query)

    cursor.execute("""INSERT INTO {0}.{1} (id, first_appearance,
    "name", entitytype_id, dn, parent_id) VALUES
    (DEFAULT, now(), 'test_entity', 1, 'type=12345', DEFAULT )
    """.format(schema, "entity"))

    conn.commit()
    eq_(row_count(conn, "directory", "alias"), 1)


@with_connection
def test_create_tag(conn):
    cursor = conn.cursor()
    clear_database(cursor)

    schema = 'directory'

    cursor.execute("""INSERT INTO {0}.{1} (id, name, description) VALUES
    (1, 'test_type', 'test entity type')""".format(schema, "entitytype"))

    cursor.execute("""INSERT INTO {0}.{1} (id, first_appearance,
    "name", entitytype_id, dn, parent_id) VALUES
    (DEFAULT, now(), 'test_entity', 1, 'type=12345', DEFAULT )
    """.format(schema, "entity"))

    conn.commit()
    eq_(row_count(conn, "directory", "tag"), 1)


@with_connection
def test_create_entitytaglink(conn):
    with closing(conn.cursor()) as cursor:
        clear_database(cursor)

        schema = 'directory'

        cursor.execute("""INSERT INTO {0}.{1} (id, name, description) VALUES
        (1, 'test_type', 'test entity type')""".format(schema, "entitytype"))

        cursor.execute("""INSERT INTO {0}.{1} (id, first_appearance,
        "name", entitytype_id, dn, parent_id) VALUES
        (DEFAULT, now(), 'test_entity', 1, 'type=12345', DEFAULT )
        """.format(schema, "entity"))

        conn.commit()
        eq_(row_count(conn, "directory", "entitytaglink"), 1)


def row_count(conn, schema, table):
    with closing(conn.cursor()) as cursor:
        cursor.execute("SELECT COUNT(*) FROM {0}.{1}".format(schema, table))

        count, = cursor.fetchone()

        return count
