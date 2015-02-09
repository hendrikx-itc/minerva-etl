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
from nose.tools import assert_raises, assert_equal

from minerva.storage.trend.granularity import Granularity, \
    GranularitySeconds, GranularityMonths, GranularityDays, create_granularity


def test_granularity():
    """Test Granularity base class."""
    g = Granularity()

    timestamp = datetime.now()

    assert_raises(NotImplementedError, g.inc, timestamp)


def test_granularity_seconds():
    """Test GranularitySeconds for generic sized granularities."""
    g = GranularitySeconds(900)

    timestamp = datetime(2013, 3, 6, 13, 0)

    v = g.inc(timestamp)

    assert_equal(v, datetime(2013, 3, 6, 13, 15))

    assert_equal(str(g), '0:15:00')

    assert_equal(str(GranularitySeconds(3600)), '1:00:00')

    assert_equal(str(GranularitySeconds(43200)), '12:00:00')

    assert_equal(str(GranularitySeconds(86400)), '1 day, 0:00:00')


def test_granularity_month():
    tzinfo = pytz.timezone('Europe/Amsterdam')
    g = GranularityMonths(1)

    timestamp = tzinfo.localize(datetime(2013, 1, 1))

    v = g.inc(timestamp)

    assert_equal(v, tzinfo.localize(datetime(2013, 2, 1)))

    v = g.decr(timestamp)

    assert_equal(v, tzinfo.localize(datetime(2012, 12, 1)))

    start = tzinfo.localize(datetime(2012, 12, 1))
    end = tzinfo.localize(datetime(2013, 3, 1))

    timestamps = list(g.range(start, end))

    assert_equal(
        timestamps,
        [
            tzinfo.localize(datetime(2013, 1, 1)),
            tzinfo.localize(datetime(2013, 2, 1)),
            tzinfo.localize(datetime(2013, 3, 1))
        ]
    )


def test_granularity_month_dst():
    tzinfo = pytz.timezone('Europe/Amsterdam')

    granularity = GranularityMonths(1)
    timestamp = tzinfo.localize(datetime(2013, 11, 1))

    before_dst_switch = granularity.decr(granularity.decr(timestamp))

    assert_equal(before_dst_switch, tzinfo.localize(datetime(2013, 9, 1)))


def test_create_granularity_int():
    granularity = create_granularity(900)

    assert_equal(type(granularity), GranularitySeconds)


def test_create_granularity_days():
    granularity = create_granularity('7 days')

    assert_equal(type(granularity), GranularityDays)


def test_create_granularity_weeks():
    granularity = create_granularity('2 weeks')

    assert_equal(type(granularity), GranularityDays)


def test_create_granularity_months():
    granularity = create_granularity('3 months')

    assert_equal(type(granularity), GranularityMonths)