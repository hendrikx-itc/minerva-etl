# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from datetime import datetime
import pytz

from nose.tools import eq_, ok_

from minerva.directory import DataSource, EntityType
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.granularity import create_granularity

TIMEZONE = "Europe/Amsterdam"


def test_trendstore():
    data_source = DataSource(
        id=10, name="test-src",
        description="this is just a test data source"
    )
    entity_type = EntityType(
        id=20, name="test_type",
        description="this is just a test entity type"
    )
    granularity = create_granularity("900")
    partition_size = 3600

    trend_store = TrendStore(
        42, data_source, entity_type, granularity, partition_size,
        "table", []
    )

    timestamp = pytz.utc.localize(datetime(2013, 5, 6, 13, 30))

    partition = trend_store.partition(timestamp)

    eq_(partition.name(), "test-src_test_type_qtr_379955")

    expected_start_local = data_source.tzinfo.localize(
        datetime(2013, 5, 6, 13, 0)
    )
    expected_start_utc = expected_start_local.astimezone(pytz.utc)

    eq_(partition.start(), expected_start_utc)

    expected_end_local = data_source.tzinfo.localize(
        datetime(2013, 5, 6, 14, 0)
    )
    expected_end_utc = expected_end_local.astimezone(pytz.utc)

    eq_(partition.end(), expected_end_utc)

    eq_(trend_store.base_table_name(), "test-src_test_type_qtr")

    trend_store = TrendStore(
        42, data_source, entity_type, granularity, partition_size,
        "view", []
    )

    p = trend_store.partition(timestamp)

    eq_(p.table().name, "test-src_test_type_qtr")

    trend_store = TrendStore(
        42, data_source, entity_type, granularity, partition_size,
        None, []
    )

    p = trend_store.partition(timestamp)

    eq_(p.table().name, "test-src_test_type_qtr_379955")
