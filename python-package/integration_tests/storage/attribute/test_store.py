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
from contextlib import closing
from datetime import datetime

from nose.tools import eq_, ok_, assert_not_equal

import pytz

from minerva.directory.helpers_v4 import name_to_datasource, name_to_entitytype
from minerva.db.postgresql import get_column_names
from minerva.test import with_conn

from .minerva_db import clear_database

from minerva.storage.attribute.attributestore import AttributeStore, Query
from minerva.storage.attribute.datapackage import DataPackage


@with_conn(clear_database)
def test_simple(conn):
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CellID', 'CCR', 'Drops']

        datasource = name_to_datasource(cursor, "integration-test")
        entitytype = name_to_entitytype(cursor, "UtranCell")

        timestamp = pytz.utc.localize(datetime.utcnow())
        data_rows = [(10023, timestamp, ('10023', '0.9919', '17'))]

        datapackage = DataPackage(attribute_names, data_rows)

        attributes = datapackage.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_txn(datapackage).run(conn)

        query = (
            "SELECT attribute_directory.materialize_curr_ptr(attributestore) "
            "FROM attribute_directory.attributestore "
            "WHERE id = %s")

        cursor.execute(query, (attributestore.id,))

        query = (
            "SELECT timestamp "
            "FROM {0} "
            "LIMIT 1").format(attributestore.table.render())

        cursor.execute(query)
        timestamp, = cursor.fetchone()

        eq_(timestamp.toordinal(), timestamp.toordinal())


@with_conn(clear_database)
def test_array(conn):
    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, "integration-test")
        entitytype = name_to_entitytype(cursor, "UtranCell")
        timestamp = pytz.utc.localize(datetime.utcnow())

        datapackage = DataPackage(
            attribute_names=['channel', 'pwr'],
            rows=[
                (10023, timestamp, ('7', '0,0,0,2,5,12,87,34,5,0,0')),
                (10024, timestamp, ('9', '0,0,0,1,11,15,95,41,9,0,0'))
            ]
        )

        attributes = datapackage.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        conn.commit()

        attributestore.store_txn(datapackage).run(conn)


@with_conn(clear_database)
def test_update_modified_column(conn):
    attribute_names = ['CCR', 'Drops']

    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, "integration-test")
        entitytype = name_to_entitytype(cursor, "UtranCell")
        timestamp = datasource.tzinfo.localize(datetime.now())

        rows = [
            (10023, timestamp, ('0.9919', '17')),
            (10047, timestamp, ('0.9963', '18'))
        ]

        datapackage_a = DataPackage(
            attribute_names=attribute_names,
            rows=rows)

        datapackage_b = DataPackage(
            attribute_names=attribute_names,
            rows=rows[:1])

        attributes = datapackage_a.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)
        conn.commit()

    query = Query((
        "SELECT modified, hash "
        "FROM {0} "
        "WHERE entity_id = 10023").format(attributestore.history_table.render()))

    attributestore.store_txn(datapackage_a).run(conn)

    with closing(conn.cursor()) as cursor:
        modified_a, hash_a = query.execute(cursor).fetchone()

    attributestore.store_txn(datapackage_b).run(conn)

    with closing(conn.cursor()) as cursor:
        modified_b, hash_b = query.execute(cursor).fetchone()

    # modified should be updated when same data is delivered again
    ok_(modified_a < modified_b)

    # hash should remain the same when then same data is delivered again
    eq_(hash_a, hash_b)


@with_conn(clear_database)
def test_update(conn):
    with closing(conn.cursor()) as cursor:
        attribute_names = ['CellID', 'CCR', 'Drops']

        datasource = name_to_datasource(cursor, "integration-test")
        entitytype = name_to_entitytype(cursor, "UtranCell")
        time1 = datasource.tzinfo.localize(datetime.now())

        data_rows = [
            (10023, time1, ('10023', '0.9919', '17')),
            (10047, time1, ('10047', '0.9963', '18'))
        ]
        update_data_rows = [
            (10023, time1, ('10023', '0.5555', '17'))
        ]

        datapackage = DataPackage(attribute_names, data_rows)
        attributes = datapackage.deduce_attributes()

        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_txn(datapackage).run(conn)

        time.sleep(1)

        datapackage = DataPackage(attribute_names, update_data_rows)
        attributestore.store_txn(datapackage).run(conn)

        conn.commit()

        query = (
            'SELECT modified, "CCR" '
            'FROM {0}').format(attributestore.history_table.render())

        cursor.execute(query)
        test_list = [(modified, ccr) for modified, ccr in cursor.fetchall()]
        assert_not_equal(test_list[0][0], test_list[1][0])
        assert_not_equal(test_list[0][1], test_list[1][1])


@with_conn(clear_database)
def test_extra_column(conn):
    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, "storagetest")
        entitytype = name_to_entitytype(cursor, "UtranCell")
        timestamp = pytz.utc.localize(datetime.utcnow())

        datapackage_a = DataPackage(
            attribute_names=['test0', 'test1'],
            rows=[
                (10023, timestamp, ('10023', '0.9919'))
            ]
        )

        datapackage_b = DataPackage(
            attribute_names=['test0', 'test1', "test2"],
            rows=[
                (10023, timestamp, ('10023', '0.9919', '17'))
            ]
        )

        attributes = datapackage_a.deduce_attributes()
        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        conn.commit()

        attributestore.store_txn(datapackage_a).run(conn)
        attributestore.store_txn(datapackage_b).run(conn)

        conn.commit()
        column_names = get_column_names(conn, 'attribute_history',
                                        attributestore.table_name())
        eq_(len(column_names), 7)


@with_conn(clear_database)
def test_changing_datatype(conn):
    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, "storagetest")
        entitytype = name_to_entitytype(cursor, "UtranCell")
        timestamp = pytz.utc.localize(datetime.utcnow())
        attribute_names = ['site_nr', 'height']

        datapackage_a = DataPackage(
            attribute_names=attribute_names,
            rows=[
                (10023, timestamp, ('10023', '15'))
            ]
        )

        datapackage_b = DataPackage(
            attribute_names=attribute_names,
            rows=[
                (10023, timestamp, ('10023', '25.6'))
            ]
        )

        attributes = datapackage_a.deduce_attributes()

        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        conn.commit()

        attributestore.store_txn(datapackage_a).run(conn)
        attributestore.store_txn(datapackage_b).run(conn)

        conn.commit()
        column_names = get_column_names(conn, "attribute_history",
                                        attributestore.table_name())
        eq_(len(column_names), 6)
