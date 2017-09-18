# -*- coding: utf-8 -*-
import time
import pytz
from datetime import datetime
from contextlib import closing
import unittest

from minerva.storage import datatype
from minerva.test import connect
from minerva.directory.datasource import DataSource
from minerva.directory.entitytype import EntityType

from .minerva_db import clear_database

from minerva.storage.attribute.datapackage import DataPackage
from minerva.storage.attribute.attributestore import AttributeStore, \
    AttributeStoreDescriptor
from minerva.storage.attribute.attribute import AttributeDescriptor
from minerva.storage.attribute.retrieve import retrieve


class TestRetrieve(unittest.TestCase):
    def setUp(self):
        self.conn = clear_database(connect())

    def tearDown(self):
        self.conn.close()

    def test_retrieve(self):
        with closing(self.conn.cursor()) as cursor:
            time1 = pytz.utc.localize(datetime.utcnow())
            trend_names = ['CellID', 'CCR', 'Drops']
            data_rows = [
                (10023, time1, ('10023', '0.9919', '17')),
                (10047, time1, ('10047', '0.9963', '18'))
            ]
            data_package = DataPackage(trend_names, data_rows)

            entity_type = EntityType.from_name("UtranCell")(cursor)
            data_source = DataSource.from_name("integration-test")(cursor)

            data_types = [
                datatype.registry[name]
                for name in ["text", "real", "smallint"]
            ]

            attribute_descriptors = [
                AttributeDescriptor(name, data_type, '')
                for name, data_type in zip(trend_names, data_types)
            ]

            attribute_store_descr = AttributeStoreDescriptor(
                data_source, entity_type, attribute_descriptors
            )
            attribute_store = AttributeStore.create(
                attribute_store_descr
            )(cursor)

            attribute_store.store(data_package)(self.conn)
            time.sleep(5)

            time2 = pytz.utc.localize(datetime.utcnow())
            update_data_rows = [(10023, time2, ('10023', '0.9919', '18'))]
            update_data_package = DataPackage(trend_names, update_data_rows)
            attribute_store.store(update_data_package)(self.conn)
            self.conn.commit()

            data = retrieve(
                self.conn, attribute_store.table, trend_names, [10023]
            )
            self.assertNotEqual(data, None)
