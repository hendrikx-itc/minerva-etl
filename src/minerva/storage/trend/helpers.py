# -*- coding: utf-8 -*-
"""
Helper functions for the trend schema.
"""
from datetime import datetime, timedelta, time
from contextlib import closing
import logging
from itertools import chain

import pytz

from minerva.storage.trend.trend import Trend
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.granularity import create_granularity


class NoSuchTrendError(Exception):
    """
    Exception raised when no matching Trend is found.
    """
    pass


def get_trend_by_id(conn, trend_id):
    """
    Return trend with specified id.
    """
    query = (
        "SELECT t.id, t.name, t.description, ts.datasource_id, ts.entitytype_id, "
            "ts.granularity "
        "FROM trend.trend t "
        "JOIN trend.trendstore_trend_link ttl on ttl.trend_id = t.id "
        "JOIN trend.trendstore ts on ts.id = ttl.trendstore_id "
        "WHERE t.id = %s")

    args = (trend_id, )

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)

        if cursor.rowcount == 1:
            return Trend(*cursor.fetchone())
        else:
            raise NoSuchTrendError("No trend with id {0}".format(trend_id))


def get_previous_timestamp(timestamp, granularity):
    """
    Returns previous timestamp based on timedelta.
    This function deals with DST issues (e.g. in case of granularity of a day).
    :param timestamp: non-naive timestamp
    :param granularity: granularity in seconds to subtract from timestamp
    """
    previous = (timestamp.astimezone(pytz.utc) -
        timedelta(0, granularity)).astimezone(timestamp.tzinfo)
    utc_offset_delta = timestamp.utcoffset() - previous.utcoffset()
    return previous + utc_offset_delta


def get_next_timestamp(timestamp, granularity):
    """
    Returns next timestamp based on timedelta.
    This function deals with DST issues (e.g. in case of granularity of a day).
    :param timestamp: non-naive timestamp
    :param granularity: granularity in seconds to subtract from timestamp
    """
    next = (timestamp.astimezone(pytz.utc) +
        timedelta(0, granularity)).astimezone(timestamp.tzinfo)
    utc_offset_delta = timestamp.utcoffset() - next.utcoffset()
    return next + utc_offset_delta


def get_most_recent_timestamp(ts, granularity, minerva_tz=None):
    """
    Return most recent timestamp based on granularity
    :param ts: non-naive timetamp
    :param granularity: granularity in seconds
    :param minerva_tz: timezone of Minerva, might differ from tz of ts:
    """
    tz = ts.tzinfo

    if minerva_tz:
        _ts = ts.astimezone(minerva_tz)
    else:
        _ts = ts

    if granularity == (15 * 60):
        most_recent = datetime.combine(
            _ts, time(_ts.hour, 15 * divmod(_ts.minute, 15)[0]))
    elif granularity == (30 * 60):
        most_recent = datetime.combine(
            _ts, time(_ts.hour, 30 * divmod(_ts.minute, 30)[0]))
    elif granularity == (60 * 60):
        most_recent = datetime.combine(_ts, time(_ts.hour))
    elif granularity == (24 * 60 * 60):
        most_recent = datetime.combine(_ts, time(0))
    elif granularity == (7 * 24 * 60 * 60):
        _ts = _ts
        while _ts.isoweekday() != 1:
            _ts -= timedelta(1)
        most_recent = datetime.combine(_ts, time(0))
    else:
        logging.warning("Unsupported granularity {0}".format(granularity))
        return None

    try:
        if minerva_tz:
            _most_recent = minerva_tz.localize(most_recent)
            return _most_recent.astimezone(tz)
        else:
            return tz.localize(most_recent)
    except AttributeError:
        if minerva_tz:
            _most_recent = datetime.combine(most_recent, time(most_recent.hour,
                most_recent.minute, most_recent.second, tzinfo=minerva_tz))
            return _most_recent.astimezone(tz)
        else:
            return datetime.combine(most_recent, time(most_recent.hour,
                most_recent.minute, most_recent.second, tzinfo=tz))


def get_table_names(trendstores, start, end):
    """
    Returns table names containing trend data corresponding to specification.
    """
    return list(set(chain(*[trendstore.table_names(start, end)
            for trendstore in trendstores])))


def get_table_names_v4(cursor, datasources, granularity, entitytype, start,
        end):
    """
    A get_table_names like function that supports both v3 and v4 trendstores.
    """
    if isinstance(granularity, int):
        granularity = create_granularity(granularity)

    trendstores = [TrendStore.get(cursor, datasource, entitytype,
        granularity) for datasource in datasources]

    return get_table_names(trendstores, start, end)


def get_relation_name(conn, source_entitytype_name, target_entitytype_name):
    relation_name = None

    relationtype_name = "{}->{}".format(source_entitytype_name, target_entitytype_name)
    query = "SELECT name FROM relation.type WHERE lower(name) = lower(%s)"

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (relationtype_name,))
        relation_name, = cursor.fetchone()

    return relation_name
