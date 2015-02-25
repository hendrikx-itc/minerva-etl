# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing
import datetime

import pytz

from minerva.directory import EntityType, DataSource
from minerva.test import with_conn, clear_database, eq_
from minerva.storage import datatype
from minerva.storage.trend.trendstore import TimestampEquals
from minerva.storage.trend.tabletrendstore import TableTrendStore, \
    TableTrendStoreDescriptor
from minerva.storage.trend.trend import TrendDescriptor
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.datapackage import DefaultPackage
from minerva.db.util import get_column_names, table_exists


@with_conn(clear_database)
def test_create_trend_store(conn):
    granularity = create_granularity("900")
    partition_size = 3600

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create("test_type", '')(cursor)

        trend_store = TableTrendStore.create(TableTrendStoreDescriptor(
            data_source, entity_type, granularity,
            [], partition_size
        ))(cursor)

    assert isinstance(trend_store, TableTrendStore)

    assert trend_store.id is not None

    with closing(conn.cursor()) as cursor:
        assert table_exists(cursor, 'trend', 'test-source_test_type_qtr')


@with_conn(clear_database)
def test_create_trend_store_with_trends(conn):
    granularity = create_granularity("900")
    partition_size = 3600

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create("test_type", '')(cursor)

        trend_store = TableTrendStore.create(TableTrendStoreDescriptor(
            data_source, entity_type, granularity,
            [
                TrendDescriptor('x', datatype.DataTypeInteger, ''),
                TrendDescriptor('y', datatype.DataTypeDoublePrecision, '')
            ], partition_size
        ))(cursor)

    assert isinstance(trend_store, TableTrendStore)

    assert trend_store.id is not None

    column_names = get_column_names(
        conn, 'trend', trend_store.base_table_name()
    )

    assert 'x' in column_names
    assert 'y' in column_names


@with_conn(clear_database)
def test_create_trend_store_with_children(conn):
    granularity = create_granularity("900")
    partition_size = 3600

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create("test_type", '')(cursor)

        trend_store = TableTrendStore.create(TableTrendStoreDescriptor(
            data_source, entity_type, granularity,
            [], partition_size
        ))(cursor)

        assert trend_store.id is not None

        timestamp = pytz.utc.localize(
            datetime.datetime(2013, 5, 6, 14, 45)
        )

        partition = trend_store.partition(timestamp)

        partition.create(cursor)

        assert table_exists(
            cursor, 'trend_partition', 'test-source_test_type_qtr_379958'
        )


@with_conn(clear_database)
def test_get(conn):
    granularity = create_granularity("900")
    partition_size = 3600

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create("test_type", '')(cursor)

        TableTrendStore.create(TableTrendStoreDescriptor(
            data_source, entity_type, granularity,
            [
                TrendDescriptor('x', datatype.DataTypeInteger, ''),
                TrendDescriptor('y', datatype.DataTypeDoublePrecision, '')
            ], partition_size
        ))(cursor)

        trend_store = TableTrendStore.get(
            data_source, entity_type, granularity
        )(cursor)

        eq_(trend_store.data_source.id, data_source.id)
        eq_(trend_store.partition_size, partition_size)
        assert trend_store.id is not None, "trend_store.id is None"

        eq_(len(trend_store.trends), 2)


@with_conn(clear_database)
def test_get_by_id(conn):
    granularity = create_granularity("900")
    partition_size = 3600

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create("test_type", '')(cursor)

        t = TableTrendStore.create(TableTrendStoreDescriptor(
            data_source, entity_type, granularity, [],
            partition_size
        ))(cursor)

        trend_store = TableTrendStore.get_by_id(t.id)(cursor)

        eq_(trend_store.data_source.id, data_source.id)
        eq_(trend_store.partition_size, partition_size)
        assert trend_store.id is not None, "trend_store.id is None"


@with_conn(clear_database)
def test_check_column_types(conn):
    granularity = create_granularity("900")
    partition_size = 3600

    trend_descriptors = [
        TrendDescriptor("counter1", datatype.DataTypeInteger, ''),
        TrendDescriptor("counter2", datatype.DataTypeInteger, '')
    ]

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create("test_type", '')(cursor)

        trend_store = TableTrendStore.create(TableTrendStoreDescriptor(
            data_source, entity_type, granularity,
            [], partition_size
        ))(cursor)

        trend_store.check_trends_exist(trend_descriptors)(cursor)
        trend_store.ensure_data_types(trend_descriptors)(cursor)


@with_conn(clear_database)
def test_store_raw_qtr(conn):
    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create("test_type", '')(cursor)

        trend_store = TableTrendStore.create(TableTrendStoreDescriptor(
            data_source,
            entity_type,
            create_granularity("900"),
            [
                TrendDescriptor('counter1', datatype.DataTypeInteger, ''),
                TrendDescriptor('counter2', datatype.DataTypeText, '')
            ],
            3600
        ))(cursor)

    conn.commit()

    granularity = create_granularity('900')
    timestamp = pytz.utc.localize(datetime.datetime.utcnow())
    trend_names = ['counter1', 'counter2']
    rows = [
        ('Network=G1,Node=001', ('42', 'foo'))
    ]

    package = DefaultPackage(granularity, timestamp, trend_names, rows)

    trend_store.store(package).run(conn)


@with_conn(clear_database)
def test_store_raw_day(conn):
    granularity = create_granularity("1 day")

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create("test_type", '')(cursor)

        trend_store = TableTrendStore.create(TableTrendStoreDescriptor(
            data_source,
            entity_type,
            granularity,
            [
                TrendDescriptor('counter1', datatype.DataTypeInteger, ''),
                TrendDescriptor('counter2', datatype.DataTypeText, '')
            ],
            3600
        ))(cursor)

    conn.commit()

    timestamp = pytz.utc.localize(datetime.datetime.utcnow())
    trend_names = ['counter1', 'counter2']
    rows = [
        ('Network=G1,Node=001', ('42', 'foo'))
    ]

    package = DefaultPackage(granularity, timestamp, trend_names, rows)

    trend_store.store(package).run(conn)


@with_conn(clear_database)
def test_retrieve(conn):
    granularity = create_granularity("900 second")

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create("test_type", '')(cursor)

        trend_store = TableTrendStore.create(TableTrendStoreDescriptor(
            data_source,
            entity_type,
            granularity,
            [
                TrendDescriptor('counter1', datatype.DataTypeInteger, ''),
                TrendDescriptor('counter2', datatype.DataTypeText, '')
            ],
            3600
        ))(cursor)

    conn.commit()

    timestamp = pytz.utc.localize(datetime.datetime(2015, 2, 24, 20, 0))
    trend_names = ['counter1', 'counter2']
    rows = [
        ('Network=G1,Node=001', ('42', 'foo'))
    ]

    package = DefaultPackage(granularity, timestamp, trend_names, rows)

    trend_store.store(package).run(conn)

    with closing(conn.cursor()) as cursor:
        trend_store.retrieve(['counter1']).execute(cursor)

        rows = cursor.fetchall()

    eq_(len(rows), 1)

    with closing(conn.cursor()) as cursor:
        trend_store.retrieve(['counter1']).timestamp(
            TimestampEquals(timestamp)
        ).execute(cursor)

        rows = cursor.fetchall()

    eq_(len(rows), 1)

    with closing(conn.cursor()) as cursor:
        trend_store.retrieve(['counter1']).timestamp(
            TimestampEquals(
                pytz.utc.localize(datetime.datetime(2015, 1, 10, 13, 0))
            )
        ).execute(cursor)

        rows = cursor.fetchall()

    eq_(len(rows), 0)
