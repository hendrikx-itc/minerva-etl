# -*- coding: utf-8 -*-
"""Test functionality of AttributeStore class."""
from datetime import datetime, timedelta
from contextlib import closing

import pytz

from psycopg2 import sql

from minerva.test import clear_database
from minerva.directory import DataSource, EntityType
from minerva.storage.attribute.attribute import AttributeDescriptor
from minerva.storage.attribute.attributestore import (
    AttributeStore,
    AttributeStoreDescriptor,
)
from minerva.storage.attribute.datapackage import DataPackage
from minerva.storage import datatype


def test_create(start_db_container):
    """Test creation of a new attribute store."""
    conn = clear_database(start_db_container)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        attribute_descriptors = [
            AttributeDescriptor(
                "cntr1", datatype.registry["integer"], "description of cntr2"
            ),
            AttributeDescriptor(
                "cntr2", datatype.registry["integer"], "description of cntr1"
            ),
        ]

        attribute_names = [a.name for a in attribute_descriptors]

        attribute_store = AttributeStore.create(
            AttributeStoreDescriptor(data_source, entity_type, attribute_descriptors)
        )(cursor)

        assert len(attribute_descriptors) == len(attribute_store.attributes)

        expected_table_name = "integration-test_UtranCell"

        assert attribute_store.table_name() == expected_table_name

        conn.commit()

        query = (
            "SELECT attribute_directory.to_table_name(attribute_store) "
            "FROM attribute_directory.attribute_store "
            "WHERE id = %s"
        )

        args = (attribute_store.id,)

        cursor.execute(query, args)

        (table_name,) = cursor.fetchone()

        cursor.execute(
            "SELECT public.table_exists('attribute_base', %s)", (expected_table_name,)
        )

        (table_exists,) = cursor.fetchone()

        assert table_exists, "table {} should exist".format(expected_table_name)

        cursor.execute(
            "SELECT public.column_names('attribute_base', %s)", (expected_table_name,)
        )

        column_names = [column_name for column_name, in cursor.fetchall()]

        assert column_names == ["entity_id", "timestamp", "end"] + attribute_names

    assert table_name == expected_table_name


def test_from_attributes(start_db_container):
    """Test creation of a new attribute store from attributes."""
    conn = clear_database(start_db_container)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        attribute_descriptors = [
            AttributeDescriptor(
                "cntr1", datatype.registry["integer"], "description for this attribute"
            ),
            AttributeDescriptor(
                "cntr2", datatype.registry["integer"], "description for this attribute"
            ),
        ]

        attribute_store = AttributeStore.from_attributes(
            data_source, entity_type, attribute_descriptors
        )(cursor)

        expected_table_name = "integration-test_UtranCell"

        assert attribute_store.table_name() == expected_table_name

        conn.commit()

        query = (
            "SELECT attribute_directory.to_table_name(attribute_store) "
            "FROM attribute_directory.attribute_store "
            "WHERE id = %s"
        )

        args = (attribute_store.id,)

        cursor.execute(query, args)

        (table_name,) = cursor.fetchone()

    assert table_name == expected_table_name


def test_store_batch_simple(start_db_container):
    """Test batch wise storing using staging table."""
    conn = clear_database(start_db_container)

    with closing(conn.cursor()) as cursor:
        attribute_names = ["CCR", "Drops"]
        timestamp = pytz.utc.localize(datetime.utcnow())
        data_rows = [(10023 + i, timestamp, ("0.9919", "17")) for i in range(100)]

        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        data_package = DataPackage(attribute_names, data_rows)

        attribute_descriptors = data_package.deduce_attributes()
        attribute_store = AttributeStore.create(
            AttributeStoreDescriptor(data_source, entity_type, attribute_descriptors)
        )(cursor)

        attribute_store.store(data_package)(start_db_container)
        conn.commit()

        cursor.execute(
            "SELECT attribute_directory.materialize_curr_ptr(attribute_store) "
            "FROM attribute_directory.attribute_store "
            "WHERE id = %s",
            (attribute_store.id,),
        )

        query = sql.SQL('SELECT timestamp, "Drops" FROM {0}').format(
            attribute_store.table.identifier()
        )

        cursor.execute(query)
        # Row count should be the same as the stored batch size
        assert cursor.rowcount == len(data_package.rows)

        stored_timestamp, drops = cursor.fetchone()
        # Timestamp should be the same as the stored batch timestamp
        assert stored_timestamp == timestamp
        assert drops == 17


def test_store_batch_with_list_a(start_db_container):
    """Test batch wise storing using staging table."""
    conn = clear_database(start_db_container)

    attribute_names = ["height", "refs"]
    timestamp = pytz.utc.localize(datetime.utcnow())
    data_rows = [
        (10023 + i, timestamp, ("19.5", ["r34", "r23", "r33"])) for i in range(100)
    ]
    data_package = DataPackage(attribute_names, data_rows)
    attribute_descriptors = data_package.deduce_attributes()

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        attribute_store = AttributeStore.create(
            AttributeStoreDescriptor(data_source, entity_type, attribute_descriptors)
        )(cursor)

        attribute_store.store(data_package)(start_db_container)
        conn.commit()

        cursor.execute(
            "SELECT attribute_directory.materialize_curr_ptr(attribute_store) "
            "FROM attribute_directory.attribute_store "
            "WHERE id = %s",
            (attribute_store.id,),
        )

        query = sql.SQL("SELECT timestamp, height FROM {}").format(
            sql.Identifier(
                attribute_store.table.schema.name, attribute_store.table.name
            )
        )

        cursor.execute(query)

        # Row count should be the same as the stored batch size
        assert cursor.rowcount == len(data_package.rows)

        stored_timestamp, height = cursor.fetchone()
        # Timestamp should be the same as the stored batch timestamp
        assert stored_timestamp == timestamp
        assert height == 19.5


def test_store_batch_with_list_b(start_db_container):
    """Test batch wise storing using staging table."""
    conn = clear_database(start_db_container)

    attribute_names = ["height", "refs"]
    timestamp = pytz.utc.localize(datetime.utcnow())
    data_rows = [
        (10023, timestamp, ("19.5", ["1", "2", "3", ""])),
        (10024, timestamp, ("19.5", ["1", "1", "1", ""])),
    ]

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        data_package = DataPackage(attribute_names, data_rows)

        attribute_descriptors = data_package.deduce_attributes()

        attribute_store = AttributeStore.create(
            AttributeStoreDescriptor(data_source, entity_type, attribute_descriptors)
        )(cursor)

        attribute_store.store(data_package)(start_db_container)
        conn.commit()


def test_store_batch_with_list_c(start_db_container):
    """Test batch wise storing using staging table."""
    conn = clear_database(start_db_container)

    attribute_descriptors = [
        AttributeDescriptor("height", datatype.registry["double precision"], ""),
        AttributeDescriptor("refs", datatype.registry["text"], ""),
    ]
    attribute_names = [a.name for a in attribute_descriptors]
    timestamp = pytz.utc.localize(datetime.utcnow())
    data_rows = [
        (10023, timestamp, ("19.5", ["", "", "", ""])),
        (10024, timestamp, ("19.3", ["", "", "", ""])),
    ]

    data_package = DataPackage(attribute_names, data_rows)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        attribute_store = AttributeStore.create(
            AttributeStoreDescriptor(data_source, entity_type, attribute_descriptors)
        )(cursor)

        conn.commit()

        attribute_store.store(data_package)(start_db_container)
        conn.commit()


def test_store_txn_with_empty(start_db_container):
    """Test transactional storing with empty value."""
    conn = clear_database(start_db_container)

    attribute_descriptors = [
        AttributeDescriptor("freeText", datatype.registry["text"], "")
    ]

    attribute_names = [a.name for a in attribute_descriptors]

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)
        timestamp = pytz.utc.localize(datetime.utcnow())
        data_package = DataPackage(
            attribute_names=attribute_names, rows=[(10023, timestamp, ("",))]
        )

        attribute_store = AttributeStore.from_attributes(
            data_source, entity_type, attribute_descriptors
        )(cursor)
        conn.commit()

        attribute_store.store(data_package)(start_db_container)


def test_store_batch_update(start_db_container):
    """Test batch wise storing with updates using staging table."""
    conn = clear_database(start_db_container)

    with closing(conn.cursor()) as cursor:
        attribute_names = ["CCR", "Drops"]
        timestamp = pytz.utc.localize(datetime.utcnow())

        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        data_package = DataPackage(
            attribute_names,
            [(10023 + i, timestamp, ("0.9919", "17")) for i in range(100)],
        )

        update_data_package = DataPackage(
            attribute_names,
            [(10023 + i, timestamp, ("0.9918", "18")) for i in range(100)],
        )

        attributes = data_package.deduce_attributes()

        attribute_store = AttributeStore.create(
            AttributeStoreDescriptor(data_source, entity_type, attributes)
        )(cursor)

        attribute_store.store(data_package)(start_db_container)
        conn.commit()
        modified_query = sql.SQL(
            "SELECT modified FROM {} WHERE entity_id = 10023"
        ).format(attribute_store.history_table.identifier())

        cursor.execute(modified_query)
        (modified_a,) = cursor.fetchone()

        attribute_store.store(update_data_package)(start_db_container)
        conn.commit()

        cursor.execute(modified_query)
        (modified_b,) = cursor.fetchone()

        assert modified_b > modified_a

        cursor.execute(
            "SELECT attribute_directory.materialize_curr_ptr(attribute_store) "
            "FROM attribute_directory.attribute_store "
            "WHERE id = %s",
            (attribute_store.id,),
        )

        query = sql.SQL('SELECT timestamp, "Drops" FROM {0}').format(
            attribute_store.table.identifier()
        )

        cursor.execute(query)
        # Row count should be the same as the stored batch size
        assert cursor.rowcount == len(data_package.rows)

        stored_timestamp, drops = cursor.fetchone()

        # Timestamp should be the same as the stored batch timestamp
        assert stored_timestamp == timestamp
        assert drops == 18


def test_store_empty_rows(start_db_container):
    """Test storing of empty datapackage."""
    conn = clear_database(start_db_container)

    with closing(conn.cursor()) as cursor:
        attribute_names = ["CCR", "Drops"]
        data_rows = []

        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        data_package = DataPackage(attribute_names, data_rows)

        attribute_descriptors = data_package.deduce_attributes()

        attribute_store = AttributeStore.create(
            AttributeStoreDescriptor(data_source, entity_type, attribute_descriptors)
        )(cursor)

        conn.commit()

        attribute_store.store(data_package)(start_db_container)
        conn.commit()


def test_store_empty_attributes(start_db_container):
    """Test storing of empty datapackage."""
    conn = clear_database(start_db_container)

    with closing(conn.cursor()) as cursor:
        attribute_names = []
        timestamp = pytz.utc.localize(datetime.utcnow())
        rows = [(10023 + i, timestamp, tuple()) for i in range(100)]

        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        data_package = DataPackage(attribute_names, rows)

        attribute_descriptors = data_package.deduce_attributes()

        attribute_store = AttributeStore.create(
            AttributeStoreDescriptor(data_source, entity_type, attribute_descriptors)
        )(cursor)

        conn.commit()

        attribute_store.store(data_package)(start_db_container)


def test_compact(start_db_container):
    """Test compacting of redundant data."""
    conn = clear_database(start_db_container)

    def make_rows(timestamp):
        return [(10023 + i, timestamp, ("0.9919", "17")) for i in range(100)]

    with closing(conn.cursor()) as cursor:
        attribute_names = ["CCR", "Drops"]

        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)
        timestamp = pytz.utc.localize(datetime.utcnow())

        data_package_a = DataPackage(
            attribute_names=attribute_names, rows=make_rows(timestamp)
        )

        data_package_b = DataPackage(
            attribute_names=attribute_names, rows=make_rows(timestamp + timedelta(10))
        )

        attributes = data_package_a.deduce_attributes()
        attribute_store = AttributeStore.create(
            AttributeStoreDescriptor(data_source, entity_type, attributes)
        )(cursor)

        attribute_store.store(data_package_a)(start_db_container)
        conn.commit()

        attribute_store.store(data_package_b)(start_db_container)
        conn.commit()

        count_query = sql.SQL("SELECT count(*) FROM {0}").format(
            attribute_store.history_table.identifier()
        )

        cursor.execute(count_query)

        (count,) = cursor.fetchone()

        # Row count should be the same as the stored batch sizes summed
        assert count == len(data_package_b.rows) + len(data_package_a.rows)

        attribute_store.compact(cursor)
        conn.commit()

        cursor.execute(count_query)

        (count,) = cursor.fetchone()

        # Row count should be the same as the first stored batch size
        assert count == len(data_package_a.rows)
