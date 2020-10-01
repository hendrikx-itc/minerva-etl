# -*- coding: utf-8 -*-
import re
import datetime
from typing import Union, Callable, Dict, Any
from functools import total_ordering

from dateutil.relativedelta import relativedelta

DUMMY_EPOCH = datetime.datetime(year=1900, month=1, day=1)

@total_ordering
class Granularity:
    delta: relativedelta

    def __init__(self, delta: relativedelta):
        self.delta = delta

    def __str__(self) -> str:
        parts = []

        if self.delta.months:
            months = months_str(self.delta.months)

            parts.append(months)

        if self.delta.days:
            days = days_str(self.delta.days)

            parts.append(days)

        if not parts or (
                self.delta.hours or self.delta.minutes or self.delta.seconds):
            parts.append(time_to_str(
                self.delta.hours, self.delta.minutes, self.delta.seconds))

        return " ".join(parts)

    def __lt__(self, other):
        return self.to_timedelta() < other.to_timedelta()

    def __eq__(self, other):
        return self.to_timedelta() == other.to_timedelta()

    def to_timedelta(self):
        return (DUMMY_EPOCH + self.delta) - DUMMY_EPOCH

    def inc(self, x: datetime.datetime) -> datetime.datetime:
        return x.tzinfo.localize(
            datetime.datetime(
                *(x + self.delta).timetuple()[:6]
            )
        )

    def decr(self, x: datetime.datetime) -> datetime.datetime:
        return x.tzinfo.localize(
            datetime.datetime(
                *(x - self.delta).timetuple()[:6]
            )
        )

    def truncate(self, x: datetime.datetime) -> datetime.datetime:
        years, months, days, hours, minutes, seconds = x.timetuple()[:6]

        if self.delta == DELTA_1D:
            return x.tzinfo.localize(
                datetime.datetime(years, months, days, 0, 0, 0)
            )
        elif self.delta == DELTA_1H:
            return x.tzinfo.localize(
                datetime.datetime(years, months, days, hours, 0, 0)
            )
        elif self.delta == DELTA_30M:
            truncated_minutes = minutes - (minutes % 30)

            return x.tzinfo.localize(
                datetime.datetime(years, months, days, hours, truncated_minutes, 0)
            )
        elif self.delta == DELTA_15M:
            truncated_minutes = minutes - (minutes % 15)

            return x.tzinfo.localize(
                datetime.datetime(years, months, days, hours, truncated_minutes, 0)
            )

        raise NotImplementedError()

    def range(self, start, end):
        return fn_range(self.inc, self.inc(start), self.inc(end))


def ensure_granularity(obj) -> Granularity:
    if isinstance(obj, Granularity):
        return obj
    else:
        return create_granularity(str(obj))


def int_to_granularity(seconds) -> Granularity:
    return Granularity(relativedelta(seconds=seconds))


def timedelta_to_granularity(delta: datetime.timedelta) -> Granularity:
    return Granularity(relativedelta(days=delta.days, seconds=delta.seconds))


def str_to_granularity(granularity_str: str) -> Granularity:
    m = re.match('([0-9]{2}):([0-9]{2}):([0-9]{2})', granularity_str)

    if m:
        hours_str, minutes_str, seconds_str = m.groups()

        hours = int(hours_str)
        minutes = int(minutes_str)
        seconds = int(seconds_str)

        return Granularity(
            relativedelta(hours=hours, minutes=minutes, seconds=seconds)
        )

    m = re.match('^([0-9]+)[ ]*(s|second|seconds)$', granularity_str)

    if m:
        seconds_str, _ = m.groups()

        return Granularity(relativedelta(seconds=int(seconds_str)))

    m = re.match('^([0-9]+)[ ]*(m|min|minute|minutes)$', granularity_str)

    if m:
        minutes_str, _ = m.groups()

        return Granularity(relativedelta(minutes=int(minutes_str)))

    m = re.match('^([0-9]+)[ ]*(h|hour|hours)$', granularity_str)

    if m:
        hours_str, _ = m.groups()
        hours = int(hours_str)

        return Granularity(relativedelta(minutes=(60 * hours)))

    m = re.match('([0-9]+)[ ]*(d|day|days)', granularity_str)

    if m:
        days_str, _ = m.groups()

        return Granularity(relativedelta(days=int(days_str)))

    m = re.match('([0-9]+)[ ]*(w|week|weeks)', granularity_str)

    if m:
        weeks_str, _ = m.groups()

        return Granularity(relativedelta(days=int(weeks_str) * 7))

    m = re.match('([0-9]+)[ ]*(month|months)', granularity_str)

    if m:
        months_str, _ = m.groups()

        return Granularity(relativedelta(months=int(months_str)))

    raise Exception("Unsupported granularity: {}".format(granularity_str))


granularity_casts: Dict[Any, Callable[[Any], Granularity]] = {
    datetime.timedelta: timedelta_to_granularity,
    str: str_to_granularity,
    int: int_to_granularity
}


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


DELTA_1D = relativedelta(days=1)
DELTA_1H = relativedelta(hours=1)
DELTA_30M = relativedelta(minutes=30)
DELTA_15M = relativedelta(minutes=15)


def months_str(num: int) -> str:
    if num == 1:
        return '1 month'
    else:
        return '{} months'.format(num)


def days_str(num: int) -> str:
    if num == 1:
        return '1 day'
    else:
        return '{} days'.format(num)


def time_to_str(hours: int, minutes: int, seconds: int) -> str:
    return "{:02}:{:02}:{:02}".format(hours, minutes, seconds)


def create_granularity(gr: Union[str, datetime.timedelta, int]) -> Granularity:
    try:
        return granularity_casts[type(gr)](gr)
    except IndexError:
        raise Exception(
            'unsupported type to convert to granularity: {}'.format(type(gr))
        )
