# -*- coding: utf-8 -*-
import time
from contextlib import closing
from datetime import datetime

import pytz

from minerva.directory import DataSource, EntityType

from minerva.test import with_conn, clear_database, assert_not_equal, eq_, ok_
from minerva.storage import datatype
from minerva.storage.attribute.attribute import AttributeDescriptor
from minerva.storage.attribute.attributestore import AttributeStore, Query, \
    AttributeStoreDescriptor
from minerva.storage.attribute.datapackage import DataPackage


@with_conn(clear_database)
def test_simple(conn):
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CellID', 'CCR', 'Drops']

        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)

        timestamp = pytz.utc.localize(datetime.utcnow())
        data_rows = [(10023, timestamp, ('10023', '0.9919', '17'))]

        data_package = DataPackage(attribute_names, data_rows)

        attribute_descriptors = data_package.deduce_attributes()

        attribute_store = AttributeStore.create(AttributeStoreDescriptor(
            data_source, entity_type, attribute_descriptors
        ))(cursor)

        attribute_store.store(data_package)(conn)

        query = (
            "SELECT attribute_directory.materialize_curr_ptr(attribute_store) "
            "FROM attribute_directory.attribute_store "
            "WHERE id = %s"
        )

        cursor.execute(query, (attribute_store.id,))

        query = (
            "SELECT timestamp "
            "FROM {0} "
            "LIMIT 1"
        ).format(attribute_store.table.render())

        cursor.execute(query)
        timestamp, = cursor.fetchone()

        eq_(timestamp.toordinal(), timestamp.toordinal())


@with_conn(clear_database)
def test_array(conn):
    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)
        timestamp = pytz.utc.localize(datetime.utcnow())

        data_package = DataPackage(
            attribute_names=['channel', 'pwr'],
            rows=[
                (10023, timestamp, ('7', '0,0,0,2,5,12,87,34,5,0,0')),
                (10024, timestamp, ('9', '0,0,0,1,11,15,95,41,9,0,0'))
            ]
        )

        attribute_store = AttributeStore.create(AttributeStoreDescriptor(
            data_source, entity_type, data_package.deduce_attributes()
        ))(cursor)

        conn.commit()

        attribute_store.store(data_package)(conn)


@with_conn(clear_database)
def test_update_modified_column(conn):
    attribute_names = ['CCR', 'Drops']

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)
        timestamp = pytz.utc.localize(datetime.utcnow())

        rows = [
            (10023, timestamp, ('0.9919', '17')),
            (10047, timestamp, ('0.9963', '18'))
        ]

        data_package_a = DataPackage(
            attribute_names=attribute_names,
            rows=rows
        )

        data_package_b = DataPackage(
            attribute_names=attribute_names,
            rows=rows[:1]
        )

        attributes = data_package_a.deduce_attributes()
        attribute_store = AttributeStore.create(AttributeStoreDescriptor(
            data_source, entity_type, attributes
        ))(cursor)
        attribute_store.create(cursor)
        conn.commit()

    query = Query((
        "SELECT modified, hash "
        "FROM {0} "
        "WHERE entity_id = 10023"
    ).format(attribute_store.history_table.render()))

    attribute_store.store(data_package_a)(conn)

    with closing(conn.cursor()) as cursor:
        modified_a, hash_a = query.execute(cursor).fetchone()

    # Commit between store commands, because otherwise the 'modified' timestamp
    # is the same
    conn.commit()

    attribute_store.store(data_package_b)(conn)

    with closing(conn.cursor()) as cursor:
        modified_b, hash_b = query.execute(cursor).fetchone()

    # modified should be updated when same data is delivered again
    ok_(modified_a < modified_b)

    # hash should remain the same when then same data is delivered again
    eq_(hash_a, hash_b)


@with_conn(clear_database)
def test_update(conn):
    with closing(conn.cursor()) as cursor:
        attribute_descriptors = [
            AttributeDescriptor('CellID', datatype.registry['text'], ''),
            AttributeDescriptor('CCR', datatype.registry[
                'double precision'], ''),
            AttributeDescriptor('Drops', datatype.registry['smallint'], '')
        ]

        attribute_names = [a.name for a in attribute_descriptors]

        data_source = DataSource.from_name("integration-test")(cursor)
        entity_type = EntityType.from_name("UtranCell")(cursor)
        time1 = pytz.utc.localize(datetime.utcnow())

        data_rows = [
            (33001, time1, ('10023', 0.9919, 17)),
            (33002, time1, ('10047', 0.9963, 18))
        ]
        update_data_rows = [
            (33001, time1, ('10023', 0.5555, 17))
        ]

        attribute_store = AttributeStore.create(AttributeStoreDescriptor(
            data_source, entity_type, attribute_descriptors
        ))(cursor)

        attribute_store.store(DataPackage(attribute_names, data_rows))(conn)

        # Commit between store commands, because otherwise the 'modified'
        # timestamp is the same
        conn.commit()

        time.sleep(1)

        attribute_store.store(DataPackage(attribute_names, update_data_rows))(
            conn)

        conn.commit()

        query = (
            'SELECT modified, "CCR" '
            'FROM {0}'
        ).format(attribute_store.history_table.render())

        cursor.execute(query)
        test_list = [(modified, ccr) for modified, ccr in cursor.fetchall()]
        assert_not_equal(test_list[0][0], test_list[1][0])
        assert_not_equal(test_list[0][1], test_list[1][1])
