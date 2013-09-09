# -*- coding: utf-8 -*-
"""Test functionality of AttributeStore class."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from datetime import datetime
from contextlib import closing

from nose.tools import eq_
from minerva.test import with_conn
from minerva.directory.helpers_v4 import name_to_datasource, name_to_entitytype

from minerva_storage_attribute import schema
from minerva_storage_attribute.attribute import Attribute
from minerva_storage_attribute.attributestore import AttributeStore
from minerva_storage_attribute.datapackage import DataPackage

from minerva_db import clear_database


@with_conn(clear_database)
def test_create(conn):
    """Test creation of a new attribute store."""
    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, "integration-test")
        entitytype = name_to_entitytype(cursor, "UtranCell")

        attributes = [
            Attribute("cntr1", "integer", "description for this attribute"),
            Attribute("cntr2", "integer", "description for this attribute")]

        attributestore = AttributeStore(datasource, entitytype, attributes
                                        ).create(cursor)

        expected_table_name = "integration-test_UtranCell"

        eq_(attributestore.table_name(), expected_table_name)

        conn.commit()

        query = (
            "SELECT {0.name}.to_table_name(attributestore) "
            "FROM {0.name}.attributestore "
            "WHERE id = %s").format(schema)

        args = attributestore.id,

        cursor.execute(query, args)

        table_name, = cursor.fetchone()

    eq_(table_name, expected_table_name)


@with_conn(clear_database)
def test_store_batch(conn):
    """Test batch wise storing using staging table."""
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CCR', 'Drops']
        data_rows = [(10023 + i, ('0.9919', '17')) for i in range(100)]

        datasource = name_to_datasource(cursor, "integration-test")
        entitytype = name_to_entitytype(cursor, "UtranCell")

        timestamp = datasource.tzinfo.localize(datetime.now())
        datapackage = DataPackage(timestamp, attribute_names, data_rows)

        attributes = datapackage.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_batch(cursor, datapackage)

        query = (
            "SELECT timestamp, \"Drops\" "
            "FROM {0}").format(attributestore.table.render())

        cursor.execute(query)
        eq_(cursor.rowcount, len(datapackage.rows), "Row count should be the "
            "same as the stored batch size")

        timestamp, drops = cursor.fetchone()

        eq_(timestamp, datapackage.timestamp, "Timestamp should be the same "
            "as the stored batch timestamp")
        eq_(drops, 17)


@with_conn(clear_database)
def test_store_batch_update(conn):
    """Test batch wise storing with updates using staging table."""
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CCR', 'Drops']
        data_rows = [(10023 + i, ('0.9919', '17')) for i in range(100)]
        update_data_rows = [(10023 + i, ('0.9918', '18')) for i in range(100)]

        datasource = name_to_datasource(cursor, "integration-test")
        entitytype = name_to_entitytype(cursor, "UtranCell")

        timestamp = datasource.tzinfo.localize(datetime.now())
        datapackage = DataPackage(timestamp, attribute_names, data_rows)
        update_datapackage = DataPackage(timestamp, attribute_names,
                                         update_data_rows)

        attributes = datapackage.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_batch(cursor, datapackage)
        conn.commit()
        modified_query = (
            "SELECT modified FROM {0} "
            "WHERE entity_id = 10023").format(attributestore.table.render())

        cursor.execute(modified_query)
        modified_a, = cursor.fetchone()

        attributestore.store_batch(cursor, update_datapackage)
        conn.commit()

        cursor.execute(modified_query)
        modified_b, = cursor.fetchone()

        assert modified_b > modified_a

        query = (
            "SELECT timestamp, \"Drops\" "
            "FROM {0}").format(attributestore.table.render())

        cursor.execute(query)
        eq_(cursor.rowcount, len(datapackage.rows), "Row count should be the "
            "same as the stored batch size")

        timestamp, drops = cursor.fetchone()

        eq_(timestamp, datapackage.timestamp, "Timestamp should be the same "
            "as the stored batch timestamp")
        eq_(drops, 18)


@with_conn(clear_database)
def test_compact(conn):
    """Test batch wise storing with updates using staging table."""
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CCR', 'Drops']
        data_rows = [(10023 + i, ('0.9919', '17')) for i in range(100)]

        datasource = name_to_datasource(cursor, "integration-test")
        entitytype = name_to_entitytype(cursor, "UtranCell")

        datapackage_a = DataPackage(
            timestamp=datasource.tzinfo.localize(datetime.now()),
            attribute_names=attribute_names,
            rows=data_rows
        )
        datapackage_b = DataPackage(
            timestamp=datasource.tzinfo.localize(datetime.now()),
            attribute_names=attribute_names,
            rows=data_rows
        )

        attributes = datapackage_a.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_batch(cursor, datapackage_a)
        conn.commit()

        attributestore.store_batch(cursor, datapackage_b)
        conn.commit()

        count_query = (
            "SELECT count(*) "
            "FROM {0}").format(attributestore.table.render())

        cursor.execute(count_query)

        c, = cursor.fetchone()
        eq_(c, len(datapackage_b.rows) + len(datapackage_a.rows), "Row count should be "
            "the same as the stored batch sizes summed")

        attributestore.compact(cursor)
        conn.commit()

        cursor.execute(count_query)

        c, = cursor.fetchone()
        eq_(c, len(datapackage_a.rows), "Row count should be "
            "the same as the first stored batch size")
