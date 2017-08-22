# -*- coding: utf-8 -*-

from datetime import datetime
import pytz

from nose.tools import eq_, ok_

from minerva.directory.basetypes import DataSource, EntityType
from minerva.storage.trend.types_v4 import TrendStore
from minerva.storage.trend.granularity import create_granularity
__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2013 - 2017 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""

TIMEZONE = "Europe/Amsterdam"


def test_trendstore():
    datasource = DataSource(
        id=10, name="test-src",
            description="this is just a test datasource", timezone=TIMEZONE)
    entitytype = EntityType(
        id=20, name="test_type",
            description="this is just a test entitytype")
    granularity = create_granularity("900")
    partition_size = 3600

    trendstore = TrendStore(
        datasource, entitytype, granularity, partition_size, "table")

    timestamp = datasource.tzinfo.localize(datetime(2013, 5, 6, 13, 30))

    partition = trendstore.partition(timestamp)

    eq_(partition.name, "test-src_test_type_qtr_379955")

    expected_start_local = datasource.tzinfo.localize(datetime(
        2013, 5, 6, 13, 0))
    expected_start_utc = expected_start_local.astimezone(pytz.utc)

    eq_(partition.start, expected_start_utc)

    expected_end_local = datasource.tzinfo.localize(datetime(
        2013, 5, 6, 14, 0))
    expected_end_utc = expected_end_local.astimezone(pytz.utc)

    eq_(partition.end, expected_end_utc)

    table_basename = trendstore.make_table_basename()

    eq_(table_basename, "test-src_test_type_qtr")

    trendstore = TrendStore(
        datasource, entitytype, granularity, partition_size, "view")

    p = trendstore.partition(timestamp)

    eq_(p.table().name, "test-src_test_type_qtr")

    trendstore = TrendStore(
        datasource, entitytype, granularity, partition_size, None)

    p = trendstore.partition(timestamp)

    eq_(p.table().name, "test-src_test_type_qtr_379955")
