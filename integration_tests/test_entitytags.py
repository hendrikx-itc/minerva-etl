from contextlib import closing

from minerva.test import with_conn, clear_database, eq_


@with_conn(clear_database)
def test_create_alias(conn):
    with closing(conn.cursor()) as cursor:
        query = (
            "INSERT INTO directory.entity_type (id, name, description) VALUES "
            "(1, 'test_type', 'test entity type')"
        )

        cursor.execute(query)

        query = (
            "INSERT INTO directory.entity "
            "(id, created, \"name\", entity_type_id, dn, parent_id) "
            "VALUES"
            "(DEFAULT, now(), 'test_entity', 1, 'type=12345', DEFAULT)"
        )

        cursor.execute(query)

        conn.commit()
        eq_(row_count(conn, "directory", "alias"), 1)


@with_conn(clear_database)
def test_create_tag(conn):
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            "INSERT INTO directory.entity_type (id, name, description) "
            "VALUES (1, 'test_type', 'test entity type')"
        )

        cursor.execute(
            "INSERT INTO directory.entity ("
            "id, created, name, entity_type_id, dn, parent_id"
            ") VALUES ("
            "DEFAULT, now(), 'test_entity', 1, 'type=12345', DEFAULT"
            ")"
        )

        conn.commit()
        eq_(row_count(conn, "directory", "tag"), 1)


@with_conn(clear_database)
def test_create_entity_tag_link(conn):
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            "INSERT INTO directory.entity_type (id, name, description) "
            "VALUES (1, 'test_type', 'test entity type')"
        )

        cursor.execute(
            "INSERT INTO directory.entity ("
            "id, created, name, entity_type_id, dn, parent_id"
            ") "
            "VALUES (DEFAULT, now(), 'test_entity', 1, 'type=12345', DEFAULT)"
        )

        conn.commit()

        eq_(row_count(conn, "directory", "entity_tag_link"), 1)


def row_count(conn, schema, table):
    with closing(conn.cursor()) as cursor:
        cursor.execute("SELECT COUNT(*) FROM {0}.{1}".format(schema, table))

        count, = cursor.fetchone()

        return count
