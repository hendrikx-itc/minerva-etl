# -*- coding: utf-8 -*-
"""Test functionality of AttributeStore class."""
from datetime import datetime, timedelta
from contextlib import closing

import pytz

from minerva.test import with_conn, eq_, clear_database
from minerva.directory import DataSource, EntityType
from minerva.storage.attribute.attribute import AttributeDescriptor
from minerva.storage.attribute.attributestore import AttributeStore, \
    AttributeStoreDescriptor
from minerva.storage.attribute.datapackage import DataPackage
from minerva.storage import datatype


@with_conn(clear_database)
def test_create(conn):
    """Test creation of a new attribute store."""
    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        attribute_descriptors = [
            AttributeDescriptor(
                "cntr1", datatype.registry['integer'], "description of cntr2"
            ),
            AttributeDescriptor(
                "cntr2", datatype.registry['integer'], "description of cntr1"
            )
        ]

        attribute_names = [a.name for a in attribute_descriptors]

        attribute_store = AttributeStore.create(AttributeStoreDescriptor(
            data_source, entity_type, attribute_descriptors
        ))(cursor)

        eq_(len(attribute_descriptors), len(attribute_store.attributes))

        expected_table_name = "integration-test_UtranCell"

        eq_(attribute_store.table_name(), expected_table_name)

        conn.commit()

        query = (
            "SELECT attribute_directory.to_table_name(attribute_store) "
            "FROM attribute_directory.attribute_store "
            "WHERE id = %s"
        )

        args = attribute_store.id,

        cursor.execute(query, args)

        table_name, = cursor.fetchone()

        cursor.execute(
            "SELECT public.table_exists('attribute_base', %s)",
            (expected_table_name,)
        )

        table_exists, = cursor.fetchone()

        assert table_exists, 'table {} should exist'.format(
                expected_table_name)

        cursor.execute(
            "SELECT public.column_names('attribute_base', %s)",
            (expected_table_name,)
        )

        column_names = [column_name for column_name, in cursor.fetchall()]

        eq_(column_names, ["entity_id", "timestamp"] + attribute_names)

    eq_(table_name, expected_table_name)


@with_conn(clear_database)
def test_from_attributes(conn):
    """Test creation of a new attribute store from attributes."""
    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        attribute_descriptors = [
            AttributeDescriptor(
                "cntr1", datatype.registry['integer'],
                "description for this attribute"
            ),
            AttributeDescriptor(
                "cntr2", datatype.registry['integer'],
                "description for this attribute"
            )
        ]

        attribute_store = AttributeStore.from_attributes(
            data_source, entity_type, attribute_descriptors
        )(cursor)

        expected_table_name = "integration-test_UtranCell"

        eq_(attribute_store.table_name(), expected_table_name)

        conn.commit()

        query = (
            "SELECT attribute_directory.to_table_name(attribute_store) "
            "FROM attribute_directory.attribute_store "
            "WHERE id = %s"
        )

        args = attribute_store.id,

        cursor.execute(query, args)

        table_name, = cursor.fetchone()

    eq_(table_name, expected_table_name)


@with_conn(clear_database)
def test_store_batch_simple(conn):
    """Test batch wise storing using staging table."""
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CCR', 'Drops']
        timestamp = pytz.utc.localize(datetime.utcnow())
        data_rows = [
            (10023 + i, timestamp, ('0.9919', '17'))
            for i in range(100)
        ]

        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        data_package = DataPackage(attribute_names, data_rows)

        attribute_descriptors = data_package.deduce_attributes()
        attribute_store = AttributeStore.create(AttributeStoreDescriptor(
            data_source, entity_type, attribute_descriptors
        ))(cursor)

        attribute_store.store(data_package)(conn)
        conn.commit()

        cursor.execute(
            "SELECT attribute_directory.materialize_curr_ptr(attribute_store) "
            "FROM attribute_directory.attribute_store "
            "WHERE id = %s",
            (attribute_store.id,)
        )

        query = (
            'SELECT timestamp, "Drops" '
            'FROM {0}'
        ).format(attribute_store.table.render())

        cursor.execute(query)
        # Row count should be the same as the stored batch size
        eq_(cursor.rowcount, len(data_package.rows))

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
        for i in range(100)
    ]
    data_package = DataPackage(attribute_names, data_rows)
    attribute_descriptors = data_package.deduce_attributes()

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        attribute_store = AttributeStore.create(AttributeStoreDescriptor(
            data_source, entity_type, attribute_descriptors
        ))(cursor)

        attribute_store.store(data_package)(conn)
        conn.commit()

        cursor.execute(
            "SELECT attribute_directory.materialize_curr_ptr(attribute_store) "
            "FROM attribute_directory.attribute_store "
            "WHERE id = %s",
            (attribute_store.id,)
        )

        query = (
            "SELECT timestamp, height "
            "FROM {0}"
        ).format(attribute_store.table.render())

        cursor.execute(query)

        # Row count should be the same as the stored batch size
        eq_(cursor.rowcount, len(data_package.rows))

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
        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        data_package = DataPackage(attribute_names, data_rows)

        attribute_descriptors = data_package.deduce_attributes()

        attribute_store = AttributeStore.create(AttributeStoreDescriptor(
            data_source, entity_type, attribute_descriptors
        ))(cursor)

        attribute_store.store(data_package)(conn)
        conn.commit()


@with_conn(clear_database)
def test_store_batch_with_list_c(conn):
    """Test batch wise storing using staging table."""
    attribute_descriptors = [
        AttributeDescriptor('height', datatype.registry[
            'double precision'], ''),
        AttributeDescriptor('refs', datatype.registry['text'], '')
    ]
    attribute_names = [a.name for a in attribute_descriptors]
    timestamp = pytz.utc.localize(datetime.utcnow())
    data_rows = [
        (10023, timestamp, ('19.5', ['', '', '', ''])),
        (10024, timestamp, ('19.3', ['', '', '', '']))
    ]

    data_package = DataPackage(attribute_names, data_rows)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        attribute_store = AttributeStore.create(AttributeStoreDescriptor(
            data_source, entity_type, attribute_descriptors
        ))(cursor)

        conn.commit()

        attribute_store.store(data_package)(conn)
        conn.commit()


@with_conn(clear_database)
def test_store_txn_with_empty(conn):
    """Test transactional storing with empty value."""
    attribute_descriptors = [
        AttributeDescriptor('freeText', datatype.registry['text'], '')
    ]

    attribute_names = [a.name for a in attribute_descriptors]

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)
        timestamp = pytz.utc.localize(datetime.utcnow())
        data_package = DataPackage(
            attribute_names=attribute_names,
            rows=[
                (10023, timestamp, ('',))
            ]
        )

        attribute_store = AttributeStore.from_attributes(
            data_source, entity_type, attribute_descriptors
        )(cursor)
        conn.commit()

        attribute_store.store(data_package)(conn)


@with_conn(clear_database)
def test_store_batch_update(conn):
    """Test batch wise storing with updates using staging table."""
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CCR', 'Drops']
        timestamp = pytz.utc.localize(datetime.utcnow())

        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        data_package = DataPackage(
            attribute_names,
            [(10023 + i, timestamp, ('0.9919', '17')) for i in range(100)]
        )

        update_data_package = DataPackage(
            attribute_names,
            [(10023 + i, timestamp, ('0.9918', '18')) for i in range(100)]
        )

        attributes = data_package.deduce_attributes()

        attribute_store = AttributeStore.create(AttributeStoreDescriptor(
            data_source, entity_type, attributes
        ))(cursor)

        attribute_store.store(data_package)(conn)
        conn.commit()
        modified_query = (
            'SELECT modified FROM {0} '
            'WHERE entity_id = 10023'
        ).format(attribute_store.history_table.render())

        cursor.execute(modified_query)
        modified_a, = cursor.fetchone()

        attribute_store.store(update_data_package)(conn)
        conn.commit()

        cursor.execute(modified_query)
        modified_b, = cursor.fetchone()

        assert modified_b > modified_a

        cursor.execute(
            "SELECT attribute_directory.materialize_curr_ptr(attribute_store) "
            "FROM attribute_directory.attribute_store "
            "WHERE id = %s",
            (attribute_store.id,)
        )

        query = (
            'SELECT timestamp, "Drops" '
            'FROM {0}'
        ).format(attribute_store.table.render())

        cursor.execute(query)
        # Row count should be the same as the stored batch size
        eq_(cursor.rowcount, len(data_package.rows))

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

        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        data_package = DataPackage(attribute_names, data_rows)

        attribute_descriptors = data_package.deduce_attributes()

        attribute_store = AttributeStore.create(AttributeStoreDescriptor(
            data_source, entity_type, attribute_descriptors
        ))(cursor)

        conn.commit()

        attribute_store.store(data_package)(conn)
        conn.commit()


@with_conn(clear_database)
def test_store_empty_attributes(conn):
    """Test storing of empty datapackage."""
    with closing(conn.cursor()) as cursor:
        attribute_names = []
        timestamp = pytz.utc.localize(datetime.utcnow())
        rows = [(10023 + i, timestamp, tuple()) for i in range(100)]

        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        data_package = DataPackage(attribute_names, rows)

        attribute_descriptors = data_package.deduce_attributes()

        attribute_store = AttributeStore.create(AttributeStoreDescriptor(
            data_source, entity_type, attribute_descriptors
        ))(cursor)

        conn.commit()

        attribute_store.store(data_package)(conn)


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

        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)
        timestamp = pytz.utc.localize(datetime.utcnow())

        data_package_a = DataPackage(
            attribute_names=attribute_names,
            rows=make_rows(timestamp)
        )

        data_package_b = DataPackage(
            attribute_names=attribute_names,
            rows=make_rows(timestamp + timedelta(10))
        )

        attributes = data_package_a.deduce_attributes()
        attribute_store = AttributeStore.create(AttributeStoreDescriptor(
            data_source, entity_type, attributes
        ))(cursor)

        attribute_store.store(data_package_a)(conn)
        conn.commit()

        attribute_store.store(data_package_b)(conn)
        conn.commit()

        count_query = (
            "SELECT count(*) "
            "FROM {0}"
        ).format(attribute_store.history_table.render())

        cursor.execute(count_query)

        count, = cursor.fetchone()
        # Row count should be the same as the stored batch sizes summed
        eq_(count, len(data_package_b.rows) + len(data_package_a.rows))

        attribute_store.compact(cursor)
        conn.commit()

        cursor.execute(count_query)

        count, = cursor.fetchone()
        # Row count should be the same as the first stored batch size
        eq_(count, len(data_package_a.rows))
