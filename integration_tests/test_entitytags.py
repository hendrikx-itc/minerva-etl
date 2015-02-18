from contextlib import closing

from nose.tools import eq_

from minerva.test import with_conn, clear_database


@with_conn(clear_database)
def test_create_alias(conn):
    with closing(conn.cursor()) as cursor:
        schema = 'directory'

        query = (
            "INSERT INTO {0}.{1} (id, name, description) VALUES "
            "(1, 'test_type', 'test entity type')"
        ).format(schema, "entitytype")

        cursor.execute(query)

        query = (
            "INSERT INTO {0}.{1} "
            "(id, first_appearance, \"name\", entitytype_id, dn, parent_id) "
            "VALUES"
            "(DEFAULT, now(), 'test_entity', 1, 'type=12345', DEFAULT)"
        ).format(schema, "entity")

        cursor.execute(query)

        conn.commit()
        eq_(row_count(conn, "directory", "alias"), 1)


@with_conn(clear_database)
def test_create_tag(conn):
    with closing(conn.cursor()) as cursor:
        schema = 'directory'

        cursor.execute("""INSERT INTO {0}.{1} (id, name, description) VALUES
        (1, 'test_type', 'test entity type')""".format(schema, "entitytype"))

        cursor.execute("""INSERT INTO {0}.{1} (id, first_appearance,
        "name", entitytype_id, dn, parent_id) VALUES
        (DEFAULT, now(), 'test_entity', 1, 'type=12345', DEFAULT )
        """.format(schema, "entity"))

        conn.commit()
        eq_(row_count(conn, "directory", "tag"), 1)


@with_conn(clear_database)
def test_create_entitytaglink(conn):
    with closing(conn.cursor()) as cursor:
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
