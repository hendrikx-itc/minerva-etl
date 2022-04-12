# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import unittest

import pytz
from dateutil.relativedelta import relativedelta

from minerva.storage.trend.granularity import Granularity, create_granularity


class TestGranularity(unittest.TestCase):
    def test_granularity(self):
        """Test Granularity base class."""
        g = Granularity(relativedelta())

        self.assertEqual(str(g), "00:00:00")

        g = Granularity(relativedelta(seconds=300))

        timestamp = pytz.utc.localize(datetime(2013, 3, 6, 13, 0))

        g.inc(timestamp)

    def test_granularity_seconds(self):
        """Test GranularitySeconds for generic sized granularities."""
        g = Granularity(relativedelta(seconds=900))

        timestamp = pytz.utc.localize(datetime(2013, 3, 6, 13, 0))

        v = g.inc(timestamp)

        self.assertEqual(v, pytz.utc.localize(datetime(2013, 3, 6, 13, 15)))

        self.assertEqual(str(g), "00:15:00")

        self.assertEqual(str(Granularity(relativedelta(seconds=3600))), "01:00:00")

        self.assertEqual(str(Granularity(relativedelta(seconds=43200))), "12:00:00")

        self.assertEqual(str(Granularity(relativedelta(seconds=86400))), "1 day")

    def test_granularity_days(self):
        g = Granularity(relativedelta(days=7))

        self.assertEqual(str(g), "7 days")

        g = Granularity(relativedelta(days=1))

        self.assertEqual(str(g), "1 day")

    def test_granularity_month(self):
        tzinfo = pytz.timezone("Europe/Amsterdam")
        g = Granularity(relativedelta(months=1))

        timestamp = tzinfo.localize(datetime(2013, 1, 1))

        v = g.inc(timestamp)

        self.assertEqual(v, tzinfo.localize(datetime(2013, 2, 1)))

        v = g.decr(timestamp)

        self.assertEqual(v, tzinfo.localize(datetime(2012, 12, 1)))

        start = tzinfo.localize(datetime(2012, 12, 1))
        end = tzinfo.localize(datetime(2013, 3, 1))

        timestamps = list(g.range(start, end))

        self.assertEqual(
            timestamps,
            [
                tzinfo.localize(datetime(2013, 1, 1)),
                tzinfo.localize(datetime(2013, 2, 1)),
                tzinfo.localize(datetime(2013, 3, 1)),
            ],
        )

    def test_granularity_month_dst(self):
        tzinfo = pytz.timezone("Europe/Amsterdam")

        granularity = Granularity(relativedelta(months=1))
        timestamp = tzinfo.localize(datetime(2013, 11, 1))

        before_dst_switch = granularity.decr(granularity.decr(timestamp))

        self.assertEqual(before_dst_switch, tzinfo.localize(datetime(2013, 9, 1)))

    def test_create_granularity_int(self):
        granularity = create_granularity(900)

        self.assertEqual(type(granularity), Granularity)

    def test_create_granularity_days(self):
        granularity = create_granularity("7 days")

        self.assertEqual(type(granularity), Granularity)

        granularity = create_granularity("1 day")

        self.assertEqual(type(granularity), Granularity)
        self.assertEqual(str(granularity), "1 day")

        granularity = create_granularity("1 day, 0:00:00")

        self.assertEqual(type(granularity), Granularity)
        self.assertEqual(str(granularity), "1 day")

        granularity = create_granularity(timedelta(days=1))

        self.assertEqual(type(granularity), Granularity)
        self.assertEqual(str(granularity), "1 day")

    def test_create_granularity_weeks(self):
        granularity = create_granularity("2 weeks")

        self.assertEqual(type(granularity), Granularity)

    def test_create_granularity_months(self):
        granularity = create_granularity("3 months")

        self.assertEqual(type(granularity), Granularity)
        self.assertEqual(str(granularity), "3 months")
