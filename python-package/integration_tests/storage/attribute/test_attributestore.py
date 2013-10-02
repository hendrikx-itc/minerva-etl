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
from datetime import datetime, timedelta
from contextlib import closing

from nose.tools import eq_
from minerva.test import with_conn
from minerva.directory.basetypes import DataSource, EntityType

from minerva.storage.attribute import schema
from minerva.storage.attribute.attribute import Attribute
from minerva.storage.attribute.attributestore import AttributeStore
from minerva.storage.attribute.datapackage import DataPackage

from .minerva_db import clear_database


@with_conn(clear_database)
def test_create(conn):
    """Test creation of a new attribute store."""
    with closing(conn.cursor()) as cursor:
        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

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
def test_from_attributes(conn):
    """Test creation of a new attribute store from attributes."""
    with closing(conn.cursor()) as cursor:
        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

        attributes = [
            Attribute("cntr1", "integer", "description for this attribute"),
            Attribute("cntr2", "integer", "description for this attribute")]

        attributestore = AttributeStore.from_attributes(
            cursor, datasource, entitytype, attributes)

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
def test_store_batch_simple(conn):
    """Test batch wise storing using staging table."""
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CCR', 'Drops']
        data_rows = [(10023 + i, ('0.9919', '17')) for i in range(100)]

        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

        timestamp = datasource.tzinfo.localize(datetime.now())
        datapackage = DataPackage(timestamp, attribute_names, data_rows)

        attributes = datapackage.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_batch(cursor, datapackage)
        conn.commit()

        query = (
            "SELECT timestamp, \"Drops\" "
            "FROM {0}").format(attributestore.table.render())

        cursor.execute(query)
        # Row count should be the same as the stored batch size
        eq_(cursor.rowcount, len(datapackage.rows))

        timestamp, drops = cursor.fetchone()
        # Timestamp should be the same as the stored batch timestamp
        eq_(timestamp, datapackage.timestamp)
        eq_(drops, 17)


@with_conn(clear_database)
def test_store_batch_with_list(conn):
    """Test batch wise storing using staging table."""
    with closing(conn.cursor()) as cursor:
        attribute_names = ['height', 'refs']
        data_rows = [(10023 + i, ('19.5', ['r34', 'r23', 'r33']))
                     for i in range(100)]

        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

        timestamp = datasource.tzinfo.localize(datetime.now())
        datapackage = DataPackage(timestamp, attribute_names, data_rows)

        attributes = datapackage.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_batch(cursor, datapackage)
        conn.commit()

        query = (
            "SELECT timestamp, height "
            "FROM {0}").format(attributestore.table.render())

        cursor.execute(query)
        # Row count should be the same as the stored batch size
        eq_(cursor.rowcount, len(datapackage.rows))

        timestamp, height = cursor.fetchone()
        # Timestamp should be the same as the stored batch timestamp
        eq_(timestamp, datapackage.timestamp)
        eq_(height, 19.5)


@with_conn(clear_database)
def test_store_txn_with_list(conn):
    """Test batch wise storing using staging table."""
    with closing(conn.cursor()) as cursor:

        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

        datapackage = DataPackage(
            timestamp=datasource.tzinfo.localize(datetime.now()),
            attribute_names=['height', 'refs'],
            rows=[
                (10023 + i, ('19.5', ['e=r34,c=1', 'e=r23,c=1', 'e=r33,c=1']))
                for i in range(100)
            ]
        )

        attributestore = AttributeStore(datasource, entitytype)
        attributestore.create(cursor)
        conn.commit()

        txn = attributestore.store_txn(datapackage)
        txn.execute(conn)


@with_conn(clear_database)
def test_store_batch_update(conn):
    """Test batch wise storing with updates using staging table."""
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CCR', 'Drops']
        data_rows = [(10023 + i, ('0.9919', '17')) for i in range(100)]
        update_data_rows = [(10023 + i, ('0.9918', '18')) for i in range(100)]

        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

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
            "WHERE entity_id = 10023").format(
            attributestore.history_table.render())

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
        # Row count should be the same as the stored batch size
        eq_(cursor.rowcount, len(datapackage.rows))

        timestamp, drops = cursor.fetchone()

        # Timestamp should be the same as the stored batch timestamp
        eq_(timestamp, datapackage.timestamp)
        eq_(drops, 18)


@with_conn(clear_database)
def test_store_empty(conn):
    """Test storing of empty datapackage."""
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CCR', 'Drops']
        data_rows = []

        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

        timestamp = datasource.tzinfo.localize(datetime.now())
        datapackage = DataPackage(timestamp, attribute_names, data_rows)

        attributes = datapackage.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_txn(datapackage).run(conn)
        conn.commit()


@with_conn(clear_database)
def test_compact(conn):
    """Test batch wise storing with updates using staging table."""
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CCR', 'Drops']
        data_rows = [(10023 + i, ('0.9919', '17')) for i in range(100)]

        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

        timestamp = datasource.tzinfo.localize(datetime.now())

        datapackage_a = DataPackage(
            timestamp=timestamp,
            attribute_names=attribute_names,
            rows=data_rows
        )
        datapackage_b = DataPackage(
            timestamp=timestamp + timedelta(10),
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

        count, = cursor.fetchone()
        # Row count should be the same as the stored batch sizes summed
        eq_(count, len(datapackage_b.rows) + len(datapackage_a.rows))

        attributestore.compact(cursor)
        conn.commit()

        cursor.execute(count_query)

        count, = cursor.fetchone()
        # Row count should be the same as the first stored batch size
        eq_(count, len(datapackage_a.rows))
