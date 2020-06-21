# -*- coding: utf-8 -*-
from contextlib import closing
import datetime
import unittest

import pytz

from minerva.directory import EntityType, DataSource
from minerva.test import connect, clear_database
from minerva.storage import datatype
from minerva.storage.trend.trendstore import TrendStore, \
    TrendStorePart
from minerva.storage.trend.trend import Trend
from minerva.storage.trend.granularity import create_granularity
from minerva.db.util import get_column_names


class TestStoreTrend(unittest.TestCase):
    def setUp(self):
        self.conn = clear_database(connect())

    def tearDown(self):
        self.conn.close()

    def test_create_trend_store(self):
        granularity = create_granularity("900s")
        partition_size = datetime.timedelta(seconds=3600)

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity, [], partition_size
            ))(cursor)

        assert isinstance(trend_store, TrendStore)

        assert trend_store.id is not None

    def test_create_trend_store_with_trends(self):
        granularity = create_granularity("900s")
        partition_size = datetime.timedelta(seconds=3600)

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [
                    TrendStorePart.Descriptor(
                        'test-trend-store-part',
                        [
                            Trend.Descriptor(
                                'x', datatype.registry['integer'], ''
                            ),
                            Trend.Descriptor(
                                'y', datatype.registry['double precision'], ''
                            )
                        ]
                    )
                ], partition_size
            ))(cursor)

        self.assertIsInstance(trend_store, TrendStore)

        self.assertIsNotNone(trend_store.id)

        column_names = get_column_names(
            self.conn, 'trend', trend_store.parts[0].base_table_name()
        )

        self.assertIn('x', column_names)
        self.assertIn('y', column_names)

    def test_create_trend_store_with_children(self):
        granularity = create_granularity("900s")
        partition_size = datetime.timedelta(seconds=3600)

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity, [
                    TrendStorePart.Descriptor(
                        'test-trend-store_part1', [])
                ], partition_size
            ))(cursor)

            self.assertIsNotNone(trend_store.id)

            timestamp = pytz.utc.localize(
                datetime.datetime(2013, 5, 6, 14, 45)
            )

            part = trend_store.part_by_name['test-trend-store_part1']

    def test_get(self):
        granularity = create_granularity("900s")
        partition_size = datetime.timedelta(seconds=3600)

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [
                    TrendStorePart.Descriptor(
                        'test-trend-store',
                        [
                            Trend.Descriptor(
                                'x', datatype.registry['integer'], ''
                            ),
                            Trend.Descriptor(
                                'y', datatype.registry['double precision'], ''
                            )
                        ]
                    )
                ], partition_size
            ))(cursor)

            trend_store = TrendStore.get(
                data_source, entity_type, granularity
            )(cursor)

            self.assertEqual(trend_store.data_source.id, data_source.id)
            self.assertEqual(trend_store.partition_size, partition_size)
            assert trend_store.id is not None, "trend_store.id is None"

            self.assertEqual(len(trend_store.parts), 1)
