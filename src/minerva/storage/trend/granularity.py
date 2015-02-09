# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import re
import datetime
import logging

from dateutil.relativedelta import relativedelta


def ensure_granularity(obj):
    if isinstance(obj, Granularity):
        return obj
    else:
        return create_granularity(str(obj))


def create_granularity(gr):
    if isinstance(gr, datetime.timedelta):
        timedelta_to_granularity(gr)
    elif isinstance(gr, str):
        str_to_granularity(gr)
    elif isinstance(gr, int):
        int_to_granularity(gr)
    else:
        raise Exception('unsupported type to convert to granularity: {}'.format(type(gr)))


def int_to_granularity(seconds):
    return GranularitySeconds(seconds)


def timedelta_to_granularity(delta):
    return GranularitySeconds(delta.seconds)


def str_to_granularity(granularity_str):
    m = re.match('([0-9]{2}):([0-9]{2}):([0-9]{2})', granularity_str)

    if m:
        hours, minutes, seconds = m.groups()

        return GranularitySeconds(hours * 60 * 60 + minutes * 60 + seconds)

    m = re.match('([0-9]+)', granularity_str)

    if m:
        seconds, = m.groups()

        return GranularitySeconds(int(seconds))

    elif granularity_str == "month":
        return GranularityMonth()
    else:
        raise Exception("Unsupported granularity: {}".format(granularity_str))


def fn_range(incr, start, end):
    """
    :param incr: a function that increments with 1 step.
    :param start: start value.
    :param end: end value.
    """
    current = start

    while current < end:
        yield current

        current = incr(current)


class Granularity(object):
    def __str__(self):
        return NotImplementedError()

    def inc(self, x):
        raise NotImplementedError()

    def decr(self, x):
        raise NotImplementedError()

    def truncate(self, x):
        raise NotImplementedError()

    def range(self, start, end):
        return fn_range(self.inc, self.inc(start), self.inc(end))


class GranularitySeconds(Granularity):
    def __init__(self, seconds):
        self.delta = datetime.timedelta(seconds=seconds)

    def __str__(self):
        return str(self.delta)

    def inc(self, x):
        return x + self.delta

    def decr(self, x):
        return x - self.delta

    def truncate(self, ts, minerva_tz=None):
        """
        Return most recent timestamp based on granularity

        :param ts: non-naive timestamp
        :param minerva_tz: timezone of Minerva, might differ from tz of ts:
        """
        tz = ts.tzinfo

        if minerva_tz:
            _ts = ts.astimezone(minerva_tz)
        else:
            _ts = ts

        if self.seconds < (60 * 60):
            gran_minutes = self.seconds / 60
            most_recent = datetime.datetime.combine(
                _ts, datetime.time(_ts.hour, gran_minutes *
                divmod(_ts.minute, gran_minutes)[0]))
        elif self.seconds == (60 * 60):
            most_recent = datetime.datetime.combine(_ts, datetime.time(_ts.hour))
        elif self.seconds == (24 * 60 * 60):
            most_recent = datetime.datetime.combine(_ts, datetime.time(0))
        elif self.seconds == (7 * 24 * 60 * 60):
            _ts = _ts
            while _ts.isoweekday() != 1:
                _ts -= datetime.timedelta(1)
            most_recent = datetime.datetime.combine(_ts, datetime.time(0))
        else:
            logging.warning("Unsupported granularity {0}".format(self.seconds))
            return None

        try:
            if minerva_tz:
                _most_recent = minerva_tz.localize(most_recent)
                return _most_recent.astimezone(tz)
            else:
                return tz.localize(most_recent)
        except AttributeError:
            if minerva_tz:
                _most_recent = datetime.datetime.combine(
                    most_recent,
                    datetime.time(
                        most_recent.hour, most_recent.minute,
                        most_recent.second, tzinfo=minerva_tz
                    )
                )
                return _most_recent.astimezone(tz)
            else:
                return datetime.datetime.combine(
                    most_recent,
                    datetime.time(
                        most_recent.hour, most_recent.minute,
                        most_recent.second, tzinfo=tz
                    )
                )


class GranularityMonth(Granularity):
    def __init__(self, num):
        self.delta = relativedelta(months=num)

    def __str__(self):
        return self.delta

    def inc(self, x):
        new_timestamp = x + self.delta

        return x.tzinfo.localize(
            datetime.datetime(
                new_timestamp.year, new_timestamp.month, new_timestamp.day
            )
        )

    def decr(self, x):
        new_timestamp = x - self.delta

        return x.tzinfo.localize(
            datetime.datetime(
                new_timestamp.year, new_timestamp.month, new_timestamp.day
            )
        )

    def truncate(self, timestamp):
        return timestamp.tzinfo.localize(
            datetime.datetime(timestamp.year, timestamp.month, 1)
        )


def integer_from(str_val):
    try:
        return int(str_val)
    except ValueError:
        return None
