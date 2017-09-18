from datetime import datetime
import unittest

import pytz

from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.partitioning import Partitioning

TIMEZONE = "Europe/Amsterdam"


class TestPartitioning(unittest.TestCase):
    def test_partition_index(self):
        partition_size = 345600
        partitioning = Partitioning(partition_size)

        timestamp = pytz.utc.localize(datetime(1970, 1, 5, 0, 0))
        index = partitioning.index(timestamp)
        self.assertEqual(index, 0)

        timestamp = pytz.utc.localize(datetime(1970, 1, 5, 3, 0))
        index = partitioning.index(timestamp)
        self.assertEqual(index, 1)

    def test_index_to_interval(self):
        partition_size = 3600

        partitioning = Partitioning(partition_size)

        # 0 = '1970-01-01T00:00:00+00:00'
        # (0, 0) = divmod(0, 3600)
        start, end = partitioning.index_to_interval(0)

        expected_start = pytz.utc.localize(datetime(1970, 1, 1, 0, 0))
        expected_end = pytz.utc.localize(datetime(1970, 1, 1, 1, 0))

        self.assertEqual(start, expected_start)
        self.assertEqual(end, expected_end)

        # 1365022800 = '2013-04-03T21:00:00+00:00'
        # (379173, 0) = divmod(1365022800, 3600)
        start, end = partitioning.index_to_interval(379173)

        expected_start = pytz.utc.localize(datetime(2013, 4, 3, 21, 0))
        expected_end = pytz.utc.localize(datetime(2013, 4, 3, 22, 0))

        self.assertEqual(start, expected_start)
        self.assertEqual(end, expected_end)

        partition_size = 4 * 86400
        partitioning = Partitioning(partition_size)

        start, end = partitioning.index_to_interval(0)
        expected_start = pytz.utc.localize(datetime(1970, 1, 1, 0, 0))
        expected_end = pytz.utc.localize(datetime(1970, 1, 5, 0, 0))

        self.assertEqual(start, expected_start)
        self.assertEqual(end, expected_end)

        start, end = partitioning.index_to_interval(3963)
        expected_start = pytz.utc.localize(datetime(2013, 5, 27, 0, 0))
        expected_end = pytz.utc.localize(datetime(2013, 5, 31, 0, 0))

        self.assertEqual(start, expected_start)
        self.assertEqual(end, expected_end)

        granularity = create_granularity("86400")

        # Test if all timestamps in between match
        for t in granularity.range(expected_start, expected_end):
            self.assertTrue(expected_start <= t)
            self.assertTrue(t <= expected_end)
