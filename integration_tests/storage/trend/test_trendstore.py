# -*- coding: utf-8 -*-
from contextlib import closing
import datetime
import unittest

import pytz

from minerva.directory import EntityType, DataSource
from minerva.test import connect, clear_database
from minerva.storage import datatype
from minerva.storage.trend.trendstore import TimestampEquals
from minerva.storage.trend.tabletrendstore import TableTrendStore
from minerva.storage.trend.trend import TrendDescriptor
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.datapackage import DefaultPackage
from minerva.db.util import get_column_names, table_exists


class TestStoreTrend(unittest.TestCase):
    def setUp(self):
        self.conn = clear_database(connect())

    def tearDown(self):
        self.conn.close()

    def test_create_trend_store(self):
        granularity = create_granularity("900")
        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            trend_store = TableTrendStore.create(TableTrendStore.Descriptor(
                'test-trend-store', data_source, entity_type, granularity,
                [], partition_size
            ))(cursor)

        assert isinstance(trend_store, TableTrendStore)

        assert trend_store.id is not None

        with closing(self.conn.cursor()) as cursor:
            assert table_exists(cursor, 'trend', 'test-trend-store')

    def test_create_trend_store_with_trends(self):
        granularity = create_granularity("900")
        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            trend_store = TableTrendStore.create(TableTrendStore.Descriptor(
                'test-trend-store', data_source, entity_type, granularity,
                [
                    TrendDescriptor('x', datatype.registry['integer'], ''),
                    TrendDescriptor('y', datatype.registry['double precision'], '')
                ], partition_size
            ))(cursor)

        assert isinstance(trend_store, TableTrendStore)

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
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            trend_store = TableTrendStore.create(TableTrendStore.Descriptor(
                'test-trend-store', data_source, entity_type, granularity,
                [], partition_size
            ))(cursor)

            assert trend_store.id is not None

            timestamp = pytz.utc.localize(
                datetime.datetime(2013, 5, 6, 14, 45)
            )

            partition = trend_store.partition(timestamp)

            partition.create(cursor)

            assert table_exists(
                cursor, 'trend_partition', 'test-trend-store_379958'
            )

    def test_get(self):
        granularity = create_granularity("900")
        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            TableTrendStore.create(TableTrendStore.Descriptor(
                'test-trend-store', data_source, entity_type, granularity,
                [
                    TrendDescriptor('x', datatype.registry['integer'], ''),
                    TrendDescriptor('y', datatype.registry['double precision'], '')
                ], partition_size
            ))(cursor)

            trend_store = TableTrendStore.get(
                data_source, entity_type, granularity
            )(cursor)

            self.assertEqual(trend_store.data_source.id, data_source.id)
            self.assertEqual(trend_store.partition_size, partition_size)
            assert trend_store.id is not None, "trend_store.id is None"

            self.assertEqual(len(trend_store.trends), 2)

    def test_get_by_id(self):
        granularity = create_granularity("900")
        partition_size = 3600
        timestamp = pytz.utc.localize(datetime.datetime(2015, 1, 10, 12, 0))

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            trend_store = TableTrendStore.create(TableTrendStore.Descriptor(
                'test-trend-store', data_source, entity_type, granularity,
                [
                    TrendDescriptor('counter1', datatype.registry['integer'], ''),
                    TrendDescriptor('counter2', datatype.registry['text'], '')
                ],
                3600
            ))(cursor)

            trend_store.partition(timestamp).create(cursor)

        self.conn.commit()

        trend_names = ['counter1', 'counter2']
        rows = [
            ('Network=G1,Node=001', ('42', 'foo'))
        ]

        package = DefaultPackage(granularity, timestamp, trend_names, rows)

        trend_store.store(package)(self.conn)

        with closing(self.conn.cursor()) as cursor:
            trend_store.retrieve(['counter1']).execute(cursor)

            rows = cursor.fetchall()

        self.assertEqual(len(rows), 1)

        with closing(self.conn.cursor()) as cursor:
            trend_store.retrieve(['counter1']).timestamp(
                TimestampEquals(timestamp)
            ).execute(cursor)

            rows = cursor.fetchall()

        self.assertEqual(len(rows), 1)

        with closing(self.conn.cursor()) as cursor:
            trend_store.retrieve(['counter1']).timestamp(
                TimestampEquals(
                    pytz.utc.localize(datetime.datetime(2015, 1, 10, 13, 0))
                )
            ).execute(cursor)

            rows = cursor.fetchall()

        self.assertEqual(len(rows), 0)
