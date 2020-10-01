# -*- coding: utf-8 -*-
from contextlib import closing
import datetime

import pytz

from minerva.directory import EntityType, DataSource
from minerva.test import clear_database
from minerva.storage import datatype
from minerva.storage.trend.trendstore import TrendStore, \
    TrendStorePart
from minerva.storage.trend.trend import Trend
from minerva.storage.trend.granularity import create_granularity
from minerva.db.util import get_column_names


def test_create_trend_store(start_db_container):
    conn = clear_database(start_db_container)

    granularity = create_granularity("900s")
    partition_size = datetime.timedelta(seconds=3600)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create("test_type", '')(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity, [], partition_size
        ))(cursor)

    assert isinstance(trend_store, TrendStore)

    assert trend_store.id is not None


def test_create_trend_store_with_trends(start_db_container):
    conn = clear_database(start_db_container)

    granularity = create_granularity("900s")
    partition_size = datetime.timedelta(seconds=3600)

    with closing(conn.cursor()) as cursor:
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

    assert isinstance(trend_store, TrendStore)

    assert trend_store.id is not None

    column_names = get_column_names(
        conn, 'trend', trend_store.parts[0].base_table_name()
    )

    assert 'x' in column_names
    assert 'y' in column_names


def test_create_trend_store_with_children(start_db_container):
    conn = clear_database(start_db_container)

    granularity = create_granularity("900s")
    partition_size = datetime.timedelta(seconds=3600)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create("test_type", '')(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity, [
                TrendStorePart.Descriptor(
                    'test-trend-store_part1', [])
            ], partition_size
        ))(cursor)

        assert trend_store.id is not None

        timestamp = pytz.utc.localize(
            datetime.datetime(2013, 5, 6, 14, 45)
        )

        part = trend_store.part_by_name['test-trend-store_part1']


def test_get(start_db_container):
    conn = clear_database(start_db_container)

    granularity = create_granularity("900s")
    partition_size = datetime.timedelta(seconds=3600)

    with closing(conn.cursor()) as cursor:
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

        assert trend_store.data_source.id == data_source.id
        assert trend_store.partition_size == partition_size
        assert trend_store.id is not None, "trend_store.id is None"

        assert len(trend_store.parts) == 1
