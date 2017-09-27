# -*- coding: utf-8 -*-
import re
import datetime

from dateutil.relativedelta import relativedelta


def ensure_granularity(obj):
    if isinstance(obj, Granularity):
        return obj
    else:
        return create_granularity(str(obj))


def create_granularity(gr):
    try:
        return granularity_casts[type(gr)](gr)
    except IndexError:
        raise Exception(
            'unsupported type to convert to granularity: {}'.format(type(gr))
        )


def int_to_granularity(seconds):
    return Granularity(relativedelta(seconds=seconds))


def timedelta_to_granularity(delta):
    return Granularity(relativedelta(days=delta.days, seconds=delta.seconds))


def str_to_granularity(granularity_str):
    m = re.match('([0-9]{2}):([0-9]{2}):([0-9]{2})', granularity_str)

    if m:
        hours, minutes, seconds = m.groups()

        return Granularity(
            relativedelta(hours=hours, minutes=minutes, seconds=seconds)
        )

    m = re.match('^([0-9]+)( second[s]?)?$', granularity_str)

    if m:
        seconds, _ = m.groups()

        return Granularity(relativedelta(seconds=int(seconds)))

    m = re.match('([0-9]+) day[s]?', granularity_str)

    if m:
        days, = m.groups()

        return Granularity(relativedelta(days=int(days)))

    m = re.match('([0-9]+) week[s]?', granularity_str)

    if m:
        weeks, = m.groups()

        return Granularity(relativedelta(days=int(weeks) * 7))

    m = re.match('([0-9]+) month[s]?', granularity_str)

    if m:
        months, = m.groups()

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


class Granularity:
    def __init__(self, delta):
        self.delta = delta

    def __str__(self):
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

    def inc(self, x):
        return x.tzinfo.localize(
            datetime.datetime(
                *(x + self.delta).timetuple()[:6]
            )
        )

    def decr(self, x):
        return x.tzinfo.localize(
            datetime.datetime(
                *(x - self.delta).timetuple()[:6]
            )
        )

    def truncate(self, x):
        raise NotImplementedError()

    def range(self, start, end):
        return fn_range(self.inc, self.inc(start), self.inc(end))


def months_str(num):
    if num == 1:
        return '1 month'
    elif num > 1:
        return '{} months'.format(num)


def days_str(num):
    if num == 1:
        return '1 day'
    elif num > 1:
        return '{} days'.format(num)


def time_to_str(hours, minutes, seconds):
    return "{:02}:{:02}:{:02}".format(hours, minutes, seconds)
