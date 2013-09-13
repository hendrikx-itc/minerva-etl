from datetime import datetime

import pytz
from nose.tools import eq_, ok_

from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.partitioning import to_unix_timestamp, \
    from_unix_timestamp, Partitioning

TIMEZONE = "Europe/Amsterdam"


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


def test_partition_index():
    partition_size = 345600
    partitioning = Partitioning(partition_size)

    timestamp = pytz.utc.localize(datetime(1970, 1, 5, 0, 0))
    index = partitioning.index(timestamp)
    eq_(index, 0)

    timestamp = pytz.utc.localize(datetime(1970, 1, 5, 3, 0))
    index = partitioning.index(timestamp)
    eq_(index, 1)


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

