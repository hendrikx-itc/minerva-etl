# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import time
import pytz
from datetime import datetime
from contextlib import closing

from nose.tools import assert_not_equal

from minerva.test import with_conn
from minerva.directory import DataSource, EntityType

from .minerva_db import clear_database

from minerva.storage.attribute.datapackage import DataPackage
from minerva.storage.attribute.attributestore import AttributeStore, AttributeStoreDescriptor
from minerva.storage.attribute.attribute import AttributeDescriptor
from minerva.storage.attribute.retrieve import retrieve
from minerva.storage import datatype


@with_conn(clear_database)
def test_retrieve(conn):
    with closing(conn.cursor()) as cursor:
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
            datatype.DataTypeText,
            datatype.DataTypeReal,
            datatype.DataTypeSmallInt
        ]

        attribute_descriptors = [
            AttributeDescriptor(name, data_type, '')
            for name, data_type in zip(trend_names, data_types)
        ]

        attribute_store = AttributeStore.create(AttributeStoreDescriptor(
            data_source, entity_type, attribute_descriptors
        ))(cursor)

        attribute_store.store_txn(data_package).run(conn)
        time.sleep(5)

        time2 = pytz.utc.localize(datetime.utcnow())
        update_data_rows = [(10023, time2, ('10023', '0.9919', '18'))]
        update_data_package = DataPackage(trend_names, update_data_rows)
        attribute_store.store_txn(update_data_package).run(conn)
        conn.commit()

        data = retrieve(conn, attribute_store.table, trend_names, [10023])
        assert_not_equal(data, None)
