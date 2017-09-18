# -*- coding: utf-8 -*-
"""
Unit tests for functions provided by the minerva.directory.helpers module.
"""
from datetime import datetime
from threading import Thread
from contextlib import closing
from functools import partial
import unittest
import unittest.mock

import psycopg2

from minerva.directory.basetypes import EntityType, Entity
from minerva.directory import helpers
from minerva.test import connect, clear_database
from minerva.db.error import UniqueViolation


class TestHelpers(unittest.TestCase):
    def setUp(self):
        self.conn = clear_database(connect())

    def test_create_entitytype(self):
        entitytype_id = 42
        entitytype_name = "Dummy1"
        entitytype_descr = "Description of Dummy1"

        mock_conn = unittest.mock.Mock()
        mock_cursor = unittest.mock.Mock()
        mock_cursor.fetchone.return_value = (entitytype_id, )
        mock_conn.cursor.return_value = mock_cursor

        entitytype = helpers.create_entity_type(mock_conn, entitytype_name,
                                                entitytype_descr)

        assert entitytype.id == entitytype_id
        assert entitytype.name == entitytype_name
        assert entitytype.description == entitytype_descr

    def test_create_entity_empty_dn(self):
        dn = ""

        mock_conn = unittest.mock.MagicMock()

        with self.assertRaises(Exception):
            helpers.create_entity(mock_conn, dn)

    @unittest.mock.patch('minerva.directory.helpers.get_entity')
    @unittest.mock.patch('minerva.directory.helpers.get_entitytype')
    def test_create_entity(mock_get_entitytype, mock_get_entity):
        dn = "Network=One,Element=Two"

        entity_type = EntityType(33, "testtype", "")
        parent_entity = Entity(42, "One", 18, "Network=One", None)

        mock_conn = unittest.mock.Mock()
        mock_cursor = unittest.mock.Mock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.return_value = (
            1,               # id
            datetime.now(),  # first_appearance
            'Two',           # name
            33,              # entitytype_id
            dn,              # dn
            42               # parent_id
        )

        mock_get_entitytype.return_value = entity_type
        mock_get_entity.return_value = parent_entity

        entity = helpers.create_entity(mock_conn, dn)

        assert entity.name == "Two"
        eq_(entity.entitytype_id, entity_type.id)
        assert entity.dn == dn
        assert entity.parent_id == parent_entity.id

    def test_create_entity_type_existing(self):
        """
        When psycopg2 raises an IntegrityError, create_entity_type should in
        turn raise a UniqueViolation exception.
        """
        entity_type_name = "Dummy1"
        entity_type_descr = "Description of Dummy1"

        mock_conn = unittest.mock.Mock()
        mock_cursor = unittest.mock.Mock()

        exc = psycopg2.Error()
        exc.pgcode = psycopg2.errorcodes.UNIQUE_VIOLATION

        mock_cursor.execute.side_effect = exc
        mock_conn.cursor.return_value = mock_cursor

        with self.assertRaises(UniqueViolation):
            helpers.create_entity_type(
                mock_conn, entity_type_name, entity_type_descr
            )

    def test_get_entitytype(self):
        """
        Check normal functioning of get_entity_type, with an existing
        entity_type.
        """
        entity_type_id = 42
        entity_type_name = "Dummy1"
        entity_type_descr = "Description of Dummy1"

        mock_conn = unittest.mock.MagicMock()
        mock_cursor = unittest.mock.MagicMock()
        mock_cursor.fetchone.return_value = (entity_type_id, entity_type_name,
                                             entity_type_descr)
        mock_conn.cursor.return_value = mock_cursor

        entity_type = helpers.get_entity_type(mock_conn, entity_type_name)

        assert entity_type.id == entity_type_id
        assert entity_type.name == entity_type_name
        assert entity_type.description == entity_type_descr

    def test_get_entitytype_by_id(self):
        """
        Check normal functioning of get_entity_type_by_id, with an existing
        entity_type.
        """
        entity_type_id = 42
        entity_type_name = "Dummy1"
        entity_type_descr = "Description of Dummy1"

        mock_conn = unittest.mock.Mock()
        mock_cursor = unittest.mock.Mock()
        mock_cursor.fetchone.return_value = (entity_type_name, entity_type_descr)
        mock_conn.cursor.return_value = mock_cursor

        entity_type = helpers.get_entity_type_by_id(mock_conn, entity_type_id)

        assert entity_type.id == entity_type_id
        assert entity_type.name == entity_type_name
        assert entity_type.description == entity_type_descr

    def test_dns_to_entity_ids(self):
        dns = [
            "Network=TL,Node=001",
            "Network=TL,Node=002",
            "Network=TL,Node=003"]

        with closing(self.conn.cursor()) as cursor:
            entity_ids = helpers.dns_to_entity_ids(cursor, dns)

        assert len(entity_ids) == 3

    def run_dns_to_entity_ids(self, amount=100):
        dns = ["Network=TL,Node={}".format(i) for i in range(amount)]

        with closing(self.conn.cursor()) as cursor:
            entity_ids = helpers.dns_to_entity_ids(cursor, dns)

        self.conn.commit()

        assert len(entity_ids) == amount

    def test_dns_to_entity_ids_concurrent(self):
        """
        Concurrent execution of dns_to_entity_ids for the same previously
        non-existing Distinguished Names should result in a UniqueViolation
        exception.
        """
        tasks = [
            partial(run_dns_to_entity_ids, 10),
            partial(raises(UniqueViolation)(run_dns_to_entity_ids), 10)]

        threads = [Thread(target=task) for task in tasks]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

    def test_dn_to_entity(self):
        dn = "Network=TL,Node=001"

        with closing(self.conn.cursor()) as cursor:
            entity = helpers.dn_to_entity(cursor, dn)

        assert not entity.id is None
        assert not entity.entitytype_id is None
        assert not entity.parent_id is None
        assert entity.name == "001"

    def test_create_entity(self):
        dn = "Network=TL,Node=001"

        with closing(self.conn.cursor()) as cursor:
            entity = helpers.create_entity(cursor, dn)

        assert not entity is None
        assert entity.name == "001"

    def test_create_datasource(self):
        with closing(self.conn.cursor()) as cursor:
            datasource = helpers.create_data_source(
                cursor, "test-create-datasource", "short description",
                "Europe/Amsterdam"
            )

        self.assertIsNotNone(datasource.id)

    def test_name_to_datasource(self):
        with closing(self.conn.cursor()) as cursor:
            datasource = helpers.name_to_data_source(
                cursor, "test_name_to_datasource"
            )

        self.assertIsNotNone(datasource.id)

    def test_create_entitytype(self):
        with closing(self.conn.cursor()) as cursor:
            entitytype = helpers.create_entity_type(
                cursor, "test_create_entitytype", "short description of type"
            )

        self.assertIsNotNone(entitytype.id)
        self.assertEqual(entitytype.name, "test_create_entitytype")

    def test_get_entitytype_by_id(self):
        with closing(self.conn.cursor()) as cursor:
            new_entitytype = helpers.create_entity_type(
                cursor, "test_get_entitytype_by_id",
                "short description of type"
            )

            entitytype = helpers.get_entity_type_by_id(cursor, new_entitytype.id)

        self.assertEqual(entitytype.id, new_entitytype.id)
        self.assertEqual(entitytype.name, "test_get_entitytype_by_id")

    def test_get_entitytype(self):
        with closing(self.conn.cursor()) as cursor:
            new_entitytype = helpers.create_entity_type(
                cursor, "test_get_entitytype", "short description of type"
            )

            entitytype = helpers.get_entity_type(cursor, "test_get_entitytype")

        self.assertEqual(entitytype.id, new_entitytype.id)
        self.assertEqual(entitytype.name, "test_get_entitytype")

    def test_name_to_entitytype(self):
        with closing(self.conn.cursor()) as cursor:
            entitytype = helpers.name_to_entity_type(
                cursor, "test_name_to_entitytype"
            )

        self.assertIsNotNone(entitytype)
        self.assertEqual(entitytype.name, "test_name_to_entitytype")
