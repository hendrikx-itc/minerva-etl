# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import unittest

import pytz
from minerva.storage.trend.helpers import (
    get_next_timestamp,
    get_previous_timestamp,
    get_most_recent_timestamp,
)


class TestTimestamps(unittest.TestCase):
    def test_next_timestamp(self):
        """
        Test next timestamp retrieval
        """
        tz = pytz.timezone("Europe/Amsterdam")

        granularity = 604800
        ts = tz.localize(datetime(2012, 10, 8, 0, 0, 0))
        next_timestamp = tz.localize(datetime(2012, 10, 15, 0, 0, 0))
        self.assertEqual(get_next_timestamp(ts, granularity), next_timestamp)

        # One hour extra due to DST switch
        granularity = 86400
        ts = tz.localize(datetime(2012, 10, 27, 0, 0, 0))
        next_timestamp = tz.localize(datetime(2012, 10, 28, 0, 0, 0))
        self.assertEqual(get_next_timestamp(ts, granularity), next_timestamp)

    def test_previous_timestamp(self):
        """
        Test previous timestamp retrieval
        """
        tz = pytz.timezone("Europe/Amsterdam")

        granularity = 3600
        ts = tz.localize(datetime(2013, 4, 2, 10, 0, 0))
        previous_timestamp = tz.localize(datetime(2013, 4, 2, 9, 0, 0))
        self.assertEqual(get_previous_timestamp(ts, granularity), previous_timestamp)

        granularity = 86400
        ts = get_most_recent_timestamp(
            tz.localize(datetime(2013, 4, 2, 10, 13, 0)), granularity
        )
        previous_timestamp = tz.localize(datetime(2013, 4, 1, 0, 0, 0))
        self.assertEqual(get_previous_timestamp(ts, granularity), previous_timestamp)

    def test_most_recent_timestamp(self):
        """
        Test most recent timestamp
        """
        tz = pytz.timezone("Europe/Amsterdam")

        ts = tz.localize(datetime(2012, 10, 8, 2, 42, 0))
        most_recent_timestamp = tz.localize(datetime(2012, 10, 8, 2, 0, 0))
        granularity = 3600

        self.assertEqual(
            get_most_recent_timestamp(ts, granularity), most_recent_timestamp
        )

        ts = tz.localize(datetime(2012, 10, 8, 2, 42, 0))
        most_recent_timestamp = tz.localize(datetime(2012, 10, 8, 2, 0, 0))
        granularity = 3600

        self.assertEqual(
            get_most_recent_timestamp(ts, granularity), most_recent_timestamp
        )

        ts = tz.localize(datetime(2012, 10, 29, 0, 0, 0))
        most_recent_timestamp = tz.localize(datetime(2012, 10, 29, 0, 0, 0))
        granularity = 604800

        self.assertEqual(
            get_most_recent_timestamp(ts, granularity), most_recent_timestamp
        )

        ts = tz.localize(datetime(2012, 10, 28, 23, 59, 59))
        most_recent_timestamp = tz.localize(datetime(2012, 10, 22, 0, 0, 0))
        granularity = 604800

        self.assertEqual(
            get_most_recent_timestamp(ts, granularity), most_recent_timestamp
        )

        ts = tz.localize(datetime(2012, 10, 8, 0, 0, 0)) - timedelta(0, 1)
        most_recent_timestamp = tz.localize(datetime(2012, 10, 1, 0, 0, 0))
        granularity = 604800

        self.assertEqual(
            get_most_recent_timestamp(ts, granularity), most_recent_timestamp
        )

        ts = tz.localize(datetime(2012, 10, 9, 2, 30, 0))
        most_recent_timestamp = tz.localize(datetime(2012, 10, 9, 0, 0, 0))
        granularity = 86400

        self.assertEqual(
            get_most_recent_timestamp(ts, granularity), most_recent_timestamp
        )

        ts = tz.localize(datetime(2012, 10, 29, 0, 0, 0))
        most_recent_timestamp = tz.localize(datetime(2012, 10, 29, 0, 0, 0))

        for granularity in [900, 3600, 86400, 604800]:
            self.assertEqual(
                get_most_recent_timestamp(ts, granularity), most_recent_timestamp
            )

        ts = pytz.utc.localize(datetime(2012, 10, 9, 0, 14, 0))
        loc_ts = ts.astimezone(tz)
        timestamp = tz.localize(datetime(2012, 10, 9, 2, 0, 0))
        granularity = 86400

        self.assertFalse(timestamp <= get_most_recent_timestamp(loc_ts, granularity))

        ts = pytz.utc.localize(datetime(2012, 10, 9, 9, 14, 0))
        loc_ts = ts.astimezone(tz)
        timestamp = tz.localize(datetime(2012, 10, 9, 11, 0, 0))
        granularity = 3600

        self.assertTrue(timestamp <= get_most_recent_timestamp(loc_ts, granularity))

        # DST switch on oct 28th
        ts = tz.localize(datetime(2012, 10, 28, 17, 42, 0))
        most_recent_timestamp = tz.localize(datetime(2012, 10, 28, 0, 0, 0))
        granularity = 86400

        self.assertEqual(
            get_most_recent_timestamp(ts, granularity), most_recent_timestamp
        )

        ts_utc = pytz.utc.localize(datetime(2013, 2, 25, 23, 0, 0))
        most_recent_timestamp = tz.localize(datetime(2013, 2, 26, 0, 0, 0))
        granularity = 86400

        self.assertEqual(
            get_most_recent_timestamp(ts_utc, granularity, minerva_tz=tz),
            most_recent_timestamp,
        )

        # DST switch on oct 28th
        ts_utc = pytz.utc.localize(datetime(2012, 10, 28, 16, 42, 0))
        most_recent_timestamp = tz.localize(datetime(2012, 10, 28, 0, 0, 0))
        granularity = 86400

        self.assertEqual(
            get_most_recent_timestamp(ts_utc, granularity, minerva_tz=tz),
            most_recent_timestamp,
        )
