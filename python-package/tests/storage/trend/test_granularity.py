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

from nose.tools import assert_raises, assert_equal

from minerva.storage.trend.granularity import Granularity, \
        GranularitySeconds, GranularityMonth


def test_granularity():
    """
    Test Granularity base class.
    """
    g = Granularity('900')

    timestamp = datetime.now()

    assert_raises(NotImplementedError, g.inc, timestamp)


def test_granularity_seconds():
    """
    Test GranularitySeconds for generic sized granularities.
    """
    g = GranularitySeconds('900')

    timestamp = datetime(2013, 3, 6, 13, 0)

    v = g.inc(timestamp)

    assert_equal(v, datetime(2013, 3, 6, 13, 15))


def test_granularity_month():
    g = GranularityMonth()

    timestamp = datetime(2013, 1, 1)

    v = g.inc(timestamp)

    assert_equal(v, datetime(2013, 2, 1))

    v = g.decr(timestamp)

    assert_equal(v, datetime(2012, 12, 1))

    start = datetime(2012, 12, 1)
    end = datetime(2013, 3, 1)

    timestamps = list(g.range(start, end))

    assert_equal(timestamps, [
        datetime(2013, 1, 1),
        datetime(2013, 2, 1),
        datetime(2013, 3, 1)])
