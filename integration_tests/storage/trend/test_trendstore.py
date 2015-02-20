# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing
import datetime

import pytz

from minerva.directory import EntityType, DataSource
from minerva.test import connect, clear_database, eq_
from minerva.storage import datatype
from minerva.storage.trend.trendstore import TrendStore, TrendStoreDescriptor
from minerva.storage.trend.trend import TrendDescriptor
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.datapackage import DefaultPackage
from minerva.db.util import get_column_names, table_exists


class TestStore():
    def __init__(self):
        self.conn = None
        self.data_source = None
        self.entity_type = None

    def setup(self):
        self.conn = connect()

        clear_database(self.conn)

        self.conn.commit()

        with closing(self.conn.cursor()) as cursor:
            self.data_source = DataSource.from_name("test-source")(cursor)
            self.entity_type = EntityType.from_name("test_type")(cursor)

        self.conn.commit()

    def teardown(self):
        self.conn.rollback()
        self.conn.close()

    def test_create_trend_store(self):
        granularity = create_granularity("900")
        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            trend_store = TrendStore.create(TrendStoreDescriptor(
                self.data_source, self.entity_type, granularity,
                [], partition_size
            ))(cursor)

        assert isinstance(trend_store, TrendStore)

        assert trend_store.id is not None

        with closing(self.conn.cursor()) as cursor:
            assert table_exists(cursor, 'trend', 'test-source_test_type_qtr')

    def test_create_trend_store_with_trends(self):
        granularity = create_granularity("900")
        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            trend_store = TrendStore.create(TrendStoreDescriptor(
                self.data_source, self.entity_type, granularity,
                [
                    TrendDescriptor('x', datatype.DataTypeInteger, ''),
                    TrendDescriptor('y', datatype.DataTypeDoublePrecision, '')
                ], partition_size
            ))(cursor)

        assert isinstance(trend_store, TrendStore)

        assert trend_store.id is not None

        column_names = get_column_names(
            self.conn, 'trend', trend_store.base_table_name()
        )

        assert 'x' in column_names
        assert 'y' in column_names

    def test_create_trend_store_with_children(self):
        granularity = create_granularity("900")
        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            trend_store = TrendStore.create(TrendStoreDescriptor(
                self.data_source, self.entity_type, granularity,
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

    def test_get(self):
        granularity = create_granularity("900")
        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            TrendStore.create(TrendStoreDescriptor(
                self.data_source, self.entity_type, granularity,
                [
                    TrendDescriptor('x', datatype.DataTypeInteger, ''),
                    TrendDescriptor('y', datatype.DataTypeDoublePrecision, '')
                ], partition_size
            ))(cursor)

            trend_store = TrendStore.get(
                self.data_source, self.entity_type, granularity
            )(cursor)

            eq_(trend_store.data_source.id, self.data_source.id)
            eq_(trend_store.partition_size, partition_size)
            assert trend_store.id is not None, "trend_store.id is None"

            eq_(len(trend_store.trends), 2)

    def test_get_by_id(self):
        granularity = create_granularity("900")
        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            t = TrendStore.create(TrendStoreDescriptor(
                self.data_source, self.entity_type, granularity, [],
                partition_size
            ))(cursor)

            trend_store = TrendStore.get_by_id(t.id)(cursor)

            eq_(trend_store.data_source.id, self.data_source.id)
            eq_(trend_store.partition_size, partition_size)
            assert trend_store.id is not None, "trendstore.id is None"

    def test_check_column_types(self):
        granularity = create_granularity("900")
        partition_size = 3600

        trend_descriptors = [
            TrendDescriptor("counter1", datatype.DataTypeInteger, ''),
            TrendDescriptor("counter2", datatype.DataTypeInteger, '')
        ]

        trend_store_descriptor = TrendStoreDescriptor(
            self.data_source, self.entity_type, granularity,
            [], partition_size
        )

        with closing(self.conn.cursor()) as cursor:
            trend_store = TrendStore.create(trend_store_descriptor)(cursor)

            trend_store.check_trends_exist(trend_descriptors)(cursor)
            trend_store.ensure_data_types(trend_descriptors)(cursor)

    def test_store_raw_qtr(self):
        trend_store_descriptor = TrendStoreDescriptor(
            self.data_source,
            self.entity_type,
            create_granularity("900"),
            [
                TrendDescriptor('counter1', datatype.DataTypeInteger, ''),
                TrendDescriptor('counter2', datatype.DataTypeText, '')
            ],
            3600
        )

        with closing(self.conn.cursor()) as cursor:
            trend_store = TrendStore.create(trend_store_descriptor)(cursor)

        self.conn.commit()

        granularity = create_granularity('900')
        timestamp = pytz.utc.localize(datetime.datetime.utcnow())
        trend_names = ['counter1', 'counter2']
        rows = [
            ('Network=G1,Node=001', ('42', 'foo'))
        ]

        package = DefaultPackage(granularity, timestamp, trend_names, rows)

        trend_store.store(package).run(self.conn)

    def test_store_raw_day(self):
        granularity = create_granularity("1 day")

        trend_store_descriptor = TrendStoreDescriptor(
            self.data_source,
            self.entity_type,
            granularity,
            [
                TrendDescriptor('counter1', datatype.DataTypeInteger, ''),
                TrendDescriptor('counter2', datatype.DataTypeText, '')
            ],
            3600
        )

        with closing(self.conn.cursor()) as cursor:
            trend_store = TrendStore.create(trend_store_descriptor)(cursor)

        self.conn.commit()

        timestamp = pytz.utc.localize(datetime.datetime.utcnow())
        trend_names = ['counter1', 'counter2']
        rows = [
            ('Network=G1,Node=001', ('42', 'foo'))
        ]

        package = DefaultPackage(granularity, timestamp, trend_names, rows)

        trend_store.store(package).run(self.conn)
