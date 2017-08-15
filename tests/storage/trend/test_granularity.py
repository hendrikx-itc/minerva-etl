# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pytz
from nose.tools import assert_raises, assert_equal
from dateutil.relativedelta import relativedelta

from minerva.storage.trend.granularity import Granularity, create_granularity


def test_granularity():
    """Test Granularity base class."""
    g = Granularity(relativedelta())

    assert_equal(str(g), '00:00:00')

    g = Granularity(relativedelta(seconds=300))

    timestamp = pytz.utc.localize(datetime(2013, 3, 6, 13, 0))

    g.inc(timestamp)


def test_granularity_seconds():
    """Test GranularitySeconds for generic sized granularities."""
    g = Granularity(relativedelta(seconds=900))

    timestamp = pytz.utc.localize(datetime(2013, 3, 6, 13, 0))

    v = g.inc(timestamp)

    assert_equal(v, pytz.utc.localize(datetime(2013, 3, 6, 13, 15)))

    assert_equal(str(g), '00:15:00')

    assert_equal(str(Granularity(relativedelta(seconds=3600))), '01:00:00')

    assert_equal(str(Granularity(relativedelta(seconds=43200))), '12:00:00')

    assert_equal(str(Granularity(relativedelta(seconds=86400))), '1 day')


def test_granularity_days():
    g = Granularity(relativedelta(days=7))

    assert_equal(str(g), '7 days')

    g = Granularity(relativedelta(days=1))

    assert_equal(str(g), '1 day')


def test_granularity_month():
    tzinfo = pytz.timezone('Europe/Amsterdam')
    g = Granularity(relativedelta(months=1))

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

    granularity = Granularity(relativedelta(months=1))
    timestamp = tzinfo.localize(datetime(2013, 11, 1))

    before_dst_switch = granularity.decr(granularity.decr(timestamp))

    assert_equal(before_dst_switch, tzinfo.localize(datetime(2013, 9, 1)))


def test_create_granularity_int():
    granularity = create_granularity(900)

    assert_equal(type(granularity), Granularity)


def test_create_granularity_days():
    granularity = create_granularity('7 days')

    assert_equal(type(granularity), Granularity)

    granularity = create_granularity('1 day')

    assert_equal(type(granularity), Granularity)
    assert_equal(str(granularity), '1 day')

    granularity = create_granularity('1 day, 0:00:00')

    assert_equal(type(granularity), Granularity)
    assert_equal(str(granularity), '1 day')

    granularity = create_granularity(timedelta(days=1))

    assert_equal(type(granularity), Granularity)
    assert_equal(str(granularity), '1 day')


def test_create_granularity_weeks():
    granularity = create_granularity('2 weeks')

    assert_equal(type(granularity), Granularity)


def test_create_granularity_months():
    granularity = create_granularity('3 months')

    assert_equal(type(granularity), Granularity)
    assert_equal(str(granularity), '3 months')
