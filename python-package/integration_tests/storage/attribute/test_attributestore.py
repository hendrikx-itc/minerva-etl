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

import pytz
from nose.tools import eq_

from minerva.test import with_conn
from minerva.directory.basetypes import DataSource, EntityType
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
            "SELECT attribute_directory.to_table_name(attributestore) "
            "FROM attribute_directory.attributestore "
            "WHERE id = %s")

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
            "SELECT attribute_directory.to_table_name(attributestore) "
            "FROM attribute_directory.attributestore "
            "WHERE id = %s")

        args = attributestore.id,

        cursor.execute(query, args)

        table_name, = cursor.fetchone()

    eq_(table_name, expected_table_name)


@with_conn(clear_database)
def test_store_batch_simple(conn):
    """Test batch wise storing using staging table."""
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CCR', 'Drops']
        timestamp = pytz.utc.localize(datetime.utcnow())
        data_rows = [(10023 + i, timestamp, ('0.9919', '17')) for i in range(100)]

        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

        datapackage = DataPackage(attribute_names, data_rows)

        attributes = datapackage.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_batch(cursor, datapackage)
        conn.commit()

        cursor.execute(
            "SELECT attribute_directory.materialize_curr_ptr(attributestore) "
            "FROM attribute_directory.attributestore "
            "WHERE id = %s", (attributestore.id,))

        query = (
            "SELECT timestamp, \"Drops\" "
            "FROM {0}").format(attributestore.table.render())

        cursor.execute(query)
        # Row count should be the same as the stored batch size
        eq_(cursor.rowcount, len(datapackage.rows))

        stored_timestamp, drops = cursor.fetchone()
        # Timestamp should be the same as the stored batch timestamp
        eq_(stored_timestamp, timestamp)
        eq_(drops, 17)


@with_conn(clear_database)
def test_store_batch_with_list_a(conn):
    """Test batch wise storing using staging table."""
    attribute_names = ['height', 'refs']
    timestamp = pytz.utc.localize(datetime.utcnow())
    data_rows = [
        (10023 + i, timestamp, ('19.5', ['r34', 'r23', 'r33']))
        for i in range(100)]
    datapackage = DataPackage(attribute_names, data_rows)
    attributes = datapackage.deduce_attributes()

    with closing(conn.cursor()) as cursor:
        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_batch(cursor, datapackage)
        conn.commit()

        cursor.execute(
            "SELECT attribute_directory.materialize_curr_ptr(attributestore) "
            "FROM attribute_directory.attributestore "
            "WHERE id = %s", (attributestore.id,))

        query = (
            "SELECT timestamp, height "
            "FROM {0}").format(attributestore.table.render())

        cursor.execute(query)

        # Row count should be the same as the stored batch size
        eq_(cursor.rowcount, len(datapackage.rows))

        stored_timestamp, height = cursor.fetchone()
        # Timestamp should be the same as the stored batch timestamp
        eq_(stored_timestamp, timestamp)
        eq_(height, 19.5)


@with_conn(clear_database)
def test_store_batch_with_list_b(conn):
    """Test batch wise storing using staging table."""
    attribute_names = ['height', 'refs']
    timestamp = pytz.utc.localize(datetime.utcnow())
    data_rows = [
        (10023, timestamp, ('19.5', ['1', '2', '3', ''])),
        (10024, timestamp, ('19.5', ['1', '1', '1', '']))
    ]

    with closing(conn.cursor()) as cursor:
        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

        datapackage = DataPackage(attribute_names, data_rows)

        attributes = datapackage.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_batch(cursor, datapackage)
        conn.commit()


@with_conn(clear_database)
def test_store_batch_with_list_c(conn):
    """Test batch wise storing using staging table."""
    attribute_names = ['height', 'refs']
    timestamp = pytz.utc.localize(datetime.utcnow())
    data_rows = [
        (10023, timestamp, ('19.5', ['', '', '', ''])),
        (10024, timestamp, ('19.3', ['', '', '', '']))
    ]

    datapackage = DataPackage(attribute_names, data_rows)
    attributes = datapackage.deduce_attributes()

    with closing(conn.cursor()) as cursor:
        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_batch(cursor, datapackage)
        conn.commit()


@with_conn(clear_database)
def test_store_txn_with_empty(conn):
    """Test transactional storing with empty value."""
    with closing(conn.cursor()) as cursor:
        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")
        timestamp = pytz.utc.localize(datetime.utcnow())
        datapackage = DataPackage(
            attribute_names=['freeText'],
            rows=[
                (10023, timestamp, ('',))
            ]
        )

        attributes = datapackage.deduce_attributes()
        eq_(attributes[0].datatype, 'smallint')
        attributestore = AttributeStore.from_attributes(
            cursor, datasource, entitytype, attributes)
        conn.commit()

        attributestore.store_txn(datapackage).run(conn)


@with_conn(clear_database)
def test_store_batch_update(conn):
    """Test batch wise storing with updates using staging table."""
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CCR', 'Drops']
        timestamp = pytz.utc.localize(datetime.utcnow())

        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

        datapackage = DataPackage(
            attribute_names,
            [(10023 + i, timestamp, ('0.9919', '17')) for i in range(100)]
        )

        update_datapackage = DataPackage(
            attribute_names,
            [(10023 + i, timestamp, ('0.9918', '18')) for i in range(100)]
        )

        attributes = datapackage.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_batch(cursor, datapackage)
        conn.commit()
        modified_query = (
            'SELECT modified FROM {0} '
            'WHERE entity_id = 10023').format(
            attributestore.history_table.render())

        cursor.execute(modified_query)
        modified_a, = cursor.fetchone()

        attributestore.store_batch(cursor, update_datapackage)
        conn.commit()

        cursor.execute(modified_query)
        modified_b, = cursor.fetchone()

        assert modified_b > modified_a

        cursor.execute(
            "SELECT attribute_directory.materialize_curr_ptr(attributestore) "
            "FROM attribute_directory.attributestore "
            "WHERE id = %s", (attributestore.id,))

        query = (
            'SELECT timestamp, "Drops" '
            'FROM {0}').format(attributestore.table.render())

        cursor.execute(query)
        # Row count should be the same as the stored batch size
        eq_(cursor.rowcount, len(datapackage.rows))

        stored_timestamp, drops = cursor.fetchone()

        # Timestamp should be the same as the stored batch timestamp
        eq_(stored_timestamp, timestamp)
        eq_(drops, 18)


@with_conn(clear_database)
def test_store_empty_rows(conn):
    """Test storing of empty datapackage."""
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CCR', 'Drops']
        data_rows = []

        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

        datapackage = DataPackage(attribute_names, data_rows)

        attributes = datapackage.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_txn(datapackage).run(conn)
        conn.commit()


@with_conn(clear_database)
def test_store_empty_attributes(conn):
    """Test storing of empty datapackage."""
    with closing(conn.cursor()) as cursor:
        attribute_names = []
        timestamp = pytz.utc.localize(datetime.utcnow())
        rows = [(10023 + i, timestamp, tuple()) for i in range(100)]

        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")

        datapackage = DataPackage(attribute_names, rows)

        attributes = datapackage.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_txn(datapackage).run(conn)
        conn.commit()


@with_conn(clear_database)
def test_compact(conn):
    """Test compacting of redundant data."""
    def make_rows(timestamp):
        return [
            (10023 + i, timestamp, ('0.9919', '17'))
            for i in range(100)
        ]

    with closing(conn.cursor()) as cursor:
        attribute_names = ['CCR', 'Drops']

        datasource = DataSource.from_name(cursor, "integration-test")
        entitytype = EntityType.from_name(cursor, "UtranCell")
        timestamp = pytz.utc.localize(datetime.utcnow())

        datapackage_a = DataPackage(
            attribute_names=attribute_names,
            rows=make_rows(timestamp)
        )

        datapackage_b = DataPackage(
            attribute_names=attribute_names,
            rows=make_rows(timestamp + timedelta(10))
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
            "FROM {0}").format(attributestore.history_table.render())

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