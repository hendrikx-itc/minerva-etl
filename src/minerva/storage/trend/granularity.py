# -*- coding: utf-8 -*-
import re
import datetime
from typing import Union

from dateutil.relativedelta import relativedelta


class Granularity:
    delta: relativedelta

    def __init__(self, delta: relativedelta):
        self.delta = delta

    def __str__(self) -> str:
        parts = []

        months = months_str(self.delta.months)

        if months:
            parts.append(months)

        days = days_str(self.delta.days)

        if days:
            parts.append(days)

        if not parts or (
                self.delta.hours or self.delta.minutes or self.delta.seconds):
            parts.append(time_to_str(
                self.delta.hours, self.delta.minutes, self.delta.seconds))

        return " ".join(parts)

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
        hours, minutes, seconds = m.groups()

        return Granularity(
            relativedelta(hours=hours, minutes=minutes, seconds=seconds)
        )

    m = re.match('^([0-9]+)[ ]*(s|second|seconds)$', granularity_str)

    if m:
        seconds, _ = m.groups()

        return Granularity(relativedelta(seconds=int(seconds)))

    m = re.match('^([0-9]+)[ ]*(m|min|minute|minutes)$', granularity_str)

    if m:
        minutes, _ = m.groups()

        return Granularity(relativedelta(minutes=int(minutes)))

    m = re.match('([0-9]+)[ ]*(d|day|days)', granularity_str)

    if m:
        days, _ = m.groups()

        return Granularity(relativedelta(days=int(days)))

    m = re.match('([0-9]+)[ ]*(w|week|weeks)', granularity_str)

    if m:
        weeks, _ = m.groups()

        return Granularity(relativedelta(days=int(weeks) * 7))

    m = re.match('([0-9]+)[ ]*(month|months)', granularity_str)

    if m:
        months, _ = m.groups()

        return Granularity(relativedelta(months=int(months)))

    raise Exception("Unsupported granularity: {}".format(granularity_str))


granularity_casts = {
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
DELTA_15M = relativedelta(minutes=15)


def months_str(num: int) -> str:
    if num == 1:
        return '1 month'
    elif num > 1:
        return '{} months'.format(num)


def days_str(num: int) -> str:
    if num == 1:
        return '1 day'
    elif num > 1:
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
