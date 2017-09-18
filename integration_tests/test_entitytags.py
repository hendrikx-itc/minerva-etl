from contextlib import closing
import unittest

from minerva.test import connect, clear_database


class TestEntityTags(unittest.TestCase):
    def setUp(self):
        self.conn = clear_database(connect())

    def tearDown(self):
        self.conn.close()

    def test_create_alias(self):
        with closing(self.conn.cursor()) as cursor:
            query = (
                "INSERT INTO directory.entity_type (id, name, description) VALUES "
                "(1, 'test_type', 'test entity type')"
            )

            cursor.execute(query)

            query = (
                "INSERT INTO directory.entity "
                "(id, created, \"name\", entity_type_id, dn) "
                "VALUES"
                "(DEFAULT, now(), 'test_entity', 1, 'type=12345')"
            )

            cursor.execute(query)

            self.conn.commit()
            self.assertEqual(row_count(self.conn, "directory", "alias"), 1)

    def test_create_tag(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO directory.entity_type (id, name, description) "
                "VALUES (1, 'test_type', 'test entity type')"
            )

            cursor.execute(
                "INSERT INTO directory.entity ("
                "id, created, name, entity_type_id, dn"
                ") VALUES ("
                "DEFAULT, now(), 'test_entity', 1, 'type=12345'"
                ")"
            )

            self.conn.commit()
            self.assertEqual(row_count(self.conn, "directory", "tag"), 1)

    def test_create_entity_tag_link(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO directory.entity_type (id, name, description) "
                "VALUES (1, 'test_type', 'test entity type')"
            )

            cursor.execute(
                "INSERT INTO directory.entity ("
                "id, created, name, entity_type_id, dn"
                ") "
                "VALUES (DEFAULT, now(), 'test_entity', 1, 'type=12345')"
            )

            self.conn.commit()

            self.assertEqual(row_count(self.conn, "directory", "entity_tag_link"), 1)


def row_count(conn, schema, table):
    with closing(conn.cursor()) as cursor:
        cursor.execute("SELECT COUNT(*) FROM {0}.{1}".format(schema, table))

        count, = cursor.fetchone()

        return count
