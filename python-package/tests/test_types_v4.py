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

from minerva.directory.basetypes import DataSource, EntityType

from minerva_storage_trend.types_v4 import TrendStore, to_unix_timestamp, \
        from_unix_timestamp, Partitioning
from minerva_storage_trend.granularity import create_granularity

TIMEZONE = "Europe/Amsterdam"


def test_trendstore():
    datasource = DataSource(id=10, name="test-src",
            description="this is just a test datasource", timezone=TIMEZONE)
    entitytype = EntityType(id=20, name="test_type",
            description="this is just a test entitytype")
    granularity = create_granularity("900")
    partition_size = 3600

    trendstore = TrendStore(datasource, entitytype, granularity, partition_size,
            "table")

    timestamp = datasource.tzinfo.localize(datetime(2013, 5, 6, 13, 30))

    partition = trendstore.partition(timestamp)

    eq_(partition.name, "test-src_test_type_qtr_379955")

    expected_start_local = datasource.tzinfo.localize(datetime(2013, 5, 6, 13, 0))
    expected_start_utc = expected_start_local.astimezone(pytz.utc)

    eq_(partition.start, expected_start_utc)

    expected_end_local = datasource.tzinfo.localize(datetime(2013, 5, 6, 14, 0))
    expected_end_utc = expected_end_local.astimezone(pytz.utc)

    eq_(partition.end, expected_end_utc)

    table_basename = trendstore.make_table_basename()

    eq_(table_basename, "test-src_test_type_qtr")

    trendstore = TrendStore(datasource, entitytype, granularity, partition_size,
            "view")

    p = trendstore.partition(timestamp)

    eq_(p.table().name, "test-src_test_type_qtr")

    trendstore = TrendStore(datasource, entitytype, granularity, partition_size,
            None)

    p = trendstore.partition(timestamp)

    eq_(p.table().name, "test-src_test_type_qtr_379955")


def test_to_unix_timestamp():
    tzinfo = pytz.timezone(TIMEZONE)

    naive_timestamp = datetime(2013, 5, 16, 9, 30)
    timestamp = tzinfo.localize(naive_timestamp)

    unix_timestamp = to_unix_timestamp(timestamp)

    expected_unix_timestamp = 1368689400

    delta = unix_timestamp - expected_unix_timestamp

    eq_(delta, 0)

    eq_(unix_timestamp, expected_unix_timestamp)

    timestamp = pytz.utc.localize(datetime(1970, 1, 5, 8, 0))
    unix_timestamp = to_unix_timestamp(timestamp)

    eq_(unix_timestamp, 4 * 86400 + 8 * 3600)


def test_from_unix_timestamp():
    timestamp = from_unix_timestamp(0)
    expected_timestamp = pytz.utc.localize(datetime(1970, 1, 1, 0, 0, 0))
    eq_(timestamp, expected_timestamp)

    timestamp = from_unix_timestamp(1365022800)
    expected_timestamp = pytz.utc.localize(datetime(2013, 4, 3, 21, 0, 0))
    eq_(timestamp, expected_timestamp)


def test_index_to_interval():
    partition_size = 3600

    partitioning = Partitioning(partition_size)

    # 0 = '1970-01-01T00:00:00+00:00'
    # (0, 0) = divmod(0, 3600)
    start, end = partitioning.index_to_interval(0)

    expected_start = pytz.utc.localize(datetime(1970, 1, 1, 0, 0))
    expected_end = pytz.utc.localize(datetime(1970, 1, 1, 1, 0))

    eq_(start, expected_start)
    eq_(end, expected_end)

    # 1365022800 = '2013-04-03T21:00:00+00:00'
    # (379173, 0) = divmod(1365022800, 3600)
    start, end = partitioning.index_to_interval(379173)

    expected_start = pytz.utc.localize(datetime(2013, 4, 3, 21, 0))
    expected_end = pytz.utc.localize(datetime(2013, 4, 3, 22, 0))

    eq_(start, expected_start)
    eq_(end, expected_end)

    partition_size = 4 * 86400
    partitioning = Partitioning(partition_size)

    start, end = partitioning.index_to_interval(0)
    expected_start = pytz.utc.localize(datetime(1970, 1, 1, 0, 0))
    expected_end = pytz.utc.localize(datetime(1970, 1, 5, 0, 0))

    eq_(start, expected_start)
    eq_(end, expected_end)

    start, end = partitioning.index_to_interval(3963)
    expected_start = pytz.utc.localize(datetime(2013, 5, 27, 0, 0))
    expected_end = pytz.utc.localize(datetime(2013, 5, 31, 0, 0))

    eq_(start, expected_start)
    eq_(end, expected_end)

    granularity = create_granularity("86400")

    # Test if all timestamps in between match
    for t in granularity.range(expected_start, expected_end):
        print(t)
        ok_(expected_start <= t)
        ok_(t <= expected_end)


def test_partition_index():
    partition_size = 345600
    partitioning = Partitioning(partition_size)

    timestamp = pytz.utc.localize(datetime(1970, 1, 5, 0, 0))
    index = partitioning.index(timestamp)
    eq_(index, 0)

    timestamp = pytz.utc.localize(datetime(1970, 1, 5, 3, 0))
    index = partitioning.index(timestamp)
    eq_(index, 1)
