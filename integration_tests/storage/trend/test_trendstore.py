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

from nose.tools import eq_

from minerva.directory.helpers_v4 import name_to_entitytype, name_to_datasource
from minerva.test import connect
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.granularity import create_granularity

from minerva_db import clear_database


class TestStore(object):
    def __init__(self):
        self.conn = None
        self.data_source = None
        self.entity_type = None

    def setup(self):
        self.conn = connect()

        clear_database(self.conn)

        with closing(self.conn.cursor()) as cursor:
            self.data_source = name_to_datasource(cursor, "test-source")
            self.entity_type = name_to_entitytype(cursor, "test_type")

        self.conn.commit()

    def teardown(self):
        self.conn.close()

    def test_create_trend_store(self):
        granularity = create_granularity("900")
        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            trend_store = TrendStore(
                self.data_source, self.entity_type, granularity, partition_size,
                "table", []
            ).create(cursor)

        assert isinstance(trend_store, TrendStore)

        assert trend_store.id is not None

    def test_create_trend_store_with_children(self):
        granularity = create_granularity("900")
        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            trend_store = TrendStore(
                self.data_source, self.entity_type, granularity, partition_size,
                "table", []
            ).create(cursor)

            assert trend_store.id is not None

            timestamp = self.data_source.tzinfo.localize(
                datetime.datetime(2013, 5, 6, 14, 45)
            )

            partition = trend_store.partition(timestamp)

            partition.create(cursor)

    def test_get(self):
        granularity = create_granularity("900")
        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            TrendStore(
                self.data_source, self.entity_type, granularity,
                partition_size, "table", []
            ).create(cursor)

            trend_store = TrendStore.get(
                cursor, self.data_source, self.entity_type, granularity
            )

            eq_(trend_store.datasource.id, self.data_source.id)
            eq_(trend_store.partition_size, partition_size)
            assert trend_store.id is not None, "trendstore.id is None"
            eq_(trend_store.version, 4)

    def test_get_by_id(self):
        granularity = create_granularity("900")
        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            t = TrendStore(
                self.data_source, self.entity_type, granularity, partition_size,
                "table", []
            ).create(cursor)

            trend_store = TrendStore.get_by_id(cursor, t.id)

            eq_(trend_store.datasource.id, self.data_source.id)
            eq_(trend_store.partition_size, partition_size)
            assert trend_store.id is not None, "trendstore.id is None"
            eq_(trend_store.version, 4)

    def test_check_column_types(self):
        granularity = create_granularity("900")
        partition_size = 3600

        trend_store = TrendStore(
            self.data_source, self.entity_type, granularity, partition_size,
            "table", []
        )

        with closing(self.conn.cursor()) as cursor:
            trend_store.create(cursor)

            column_names = ["counter1", "counter2"]
            initial_data_types = ["smallint", "smallint"]
            data_types = ["integer", "text"]

            check_columns_exist = trend_store.check_columns_exist(
                column_names, initial_data_types
            )

            check_columns_exist(cursor)

            check_column_types = trend_store.check_column_types(
                column_names, data_types
            )

            check_column_types(cursor)
