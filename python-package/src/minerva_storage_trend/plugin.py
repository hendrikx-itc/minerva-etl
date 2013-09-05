from __future__ import division
from contextlib import closing
from functools import partial
from datetime import datetime

import psycopg2
import pytz

from minerva.directory.helpers import get_entity, get_entitytype_by_id

from minerva.directory.helpers_v4 import get_entitytype

from minerva_storage_trend import schema
from minerva_storage_trend.granularity import ensure_granularity
from minerva_storage_trend.storage import refine_data_rows, retrieve, \
        retrieve_orderedby_time, retrieve_aggregated, retrieve_related, \
        aggregate
from minerva_storage_trend.tables import PARTITION_SIZES
from minerva_storage_trend.helpers import get_trend_by_id, \
        get_previous_timestamp, get_most_recent_timestamp, get_next_timestamp, \
        get_table_names_v4
from minerva_storage_trend.trendstore import TrendStore, store_raw
from minerva_storage_trend.datapackage import DataPackage
from minerva_storage_trend.rawdatapackage import RawDataPackage


class TrendPlugin(object):
    def __init__(self, conn):
        self.conn = conn

    def api_version(self):
        return 3

    def get_trend_by_id(self, trend_id):
        return get_trend_by_id(self.conn, trend_id)

    def store(self, datasource, entitytype, gp, ts, trend_names, data_rows,
        sub_query=None):

        if len(data_rows) == 0:
            return

        granularity = ensure_granularity(gp)

        with closing(self.conn.cursor()) as cursor:
            trendstore = get_or_create_trendstore(cursor, datasource, entitytype,
                    granularity)

        self.conn.commit()

        datapackage = DataPackage(granularity, ts, trend_names, data_rows)

        transaction = trendstore.store(datapackage)
        transaction.run(self.conn)

    def retrieve(self, datasources, gp, entitytype, trend_names, entities,
        start, end, subquery_filter=None, relation_table_name=None, limit=None):

        with closing(self.conn.cursor()) as cursor:
            if isinstance(entitytype, str):
                entitytype = get_entitytype(cursor, entitytype)

            table_names = get_table_names_v4(cursor, datasources, gp, entitytype,
                    start, end)

        return retrieve(self.conn, schema.name, table_names, trend_names, entities,
            start, end, subquery_filter, relation_table_name, limit,
            entitytype=entitytype)

    def retrieve_orderedby_time(self, datasources, gp, entitytype, trend_names,
            entities, start, end, limit=None):

        with closing(self.conn.cursor()) as cursor:
            if isinstance(entitytype, str):
                entitytype = get_entitytype(cursor, entitytype)

            table_names = get_table_names_v4(cursor, datasources, gp, entitytype,
                    start, end)

        return retrieve_orderedby_time(self.conn, schema.name, table_names,
            trend_names, entities, start, end, limit)

    def retrieve_aggregated(self, datasource, granularity, entitytype,
            column_identifiers, interval, group_by, subquery_filter=None,
            relation_table_name=None):

        return retrieve_aggregated(self.conn, datasource, granularity, entitytype,
            column_identifiers, interval, group_by, subquery_filter,
            relation_table_name)

    def retrieve_related(self, datasources, gp, source_entitytype,
        target_entitytype, trend_names, start, end, subquery_filter=None,
            limit=None):

        with closing(self.conn.cursor()) as cursor:
            if isinstance(target_entitytype, str):
                target_entitytype = get_entitytype(cursor, target_entitytype)

            table_names = get_table_names_v4(cursor, datasources, gp,
                    target_entitytype, start, end)

        if source_entitytype.name == target_entitytype.name:
            relation_table_name = "self"
        else:
            relation_table_name = "{}->{}".format(
                source_entitytype.name, target_entitytype.name)

        return retrieve_related(self.conn, schema.name, relation_table_name,
            table_names, trend_names, start, end, subquery_filter, limit)

    def count(self, datasource, gp, entitytype_name, interval, filter=None):
        """
        Returns row count for specified datasource, gp, entity type and interval
        """
        (start, end) = interval

        with closing(self.conn.cursor()) as cursor:
            entitytype = get_entitytype(cursor, entitytype_name)

            table_names = get_table_names_v4(cursor, [datasource], gp, entitytype,
                    start, end)

        query = (
            "SELECT COUNT(*) FROM \"{0}\".\"{1}\" "
            "WHERE timestamp > %s AND timestamp <= %s ")

        if filter is not None:
            if len(filter) == 0:
                return 0
            else:
                query += "AND entity_id IN ({0}) ".format(
                    ",".join(str(id) for id in filter))

        args = (start, end)

        count = 0

        with closing(self.conn.cursor()) as cursor:
            for table_name in table_names:
                try:
                    cursor.execute(query.format(schema.name, table_name), args)
                    c, = cursor.fetchone()
                    count += c
                except (psycopg2.ProgrammingError, psycopg2.InternalError):
                    continue

        return count

    def last_modified(self, interval, datasource, granularity, entitytype_name,
            subquery_filter=None):
        """
        Return last modified timestamp for specified datasource, granularity,
        entity type and interval
        :param interval: tuple (start, end) with non-naive timestamps,
            specifying interval to check
        :param datasource: datasource object
        :param granularity: granularity in seconds
        :param entitytype_name: name of entity type
        :param subquery_filter: subquery for additional filtering
            by JOINing on field 'id'
        """
        (start, end) = interval

        with closing(self.conn.cursor()) as cursor:
            entitytype = get_entitytype(cursor, entitytype_name)
            table_names = get_table_names_v4(cursor, [datasource], granularity, entitytype,
                    start, end)

        if subquery_filter:
            query = ("SELECT MAX(t.modified) FROM \"{0}\".\"{1}\" AS t "
                "JOIN ({0}) AS filter ON filter.id = t.entity_id "
                "WHERE t.timestamp > %s AND t.timestamp <= %s ")
        else:
            query = ("SELECT MAX(t.modified) FROM \"{0}\".\"{1}\" AS t "
                "WHERE t.timestamp > %s AND t.timestamp <= %s ")

        modifieds = []
        with closing(self.conn.cursor()) as cursor:
            for table_name in table_names:
                try:
                    cursor.execute(query.format(schema.name, table_name), interval)
                    modified, = cursor.fetchone()
                    modifieds.append(modified)
                except (psycopg2.ProgrammingError, psycopg2.InternalError):
                    continue

        if modifieds:
            return max(modifieds)
        else:
            return None

    def timestamp_exists(self, datasource, gp, entitytype_name, timestamp):
        """
        Returns True when timestamp occurs for specified data source.
        False otherwise.
        """
        with closing(self.conn.cursor()) as cursor:
            entitytype = get_entitytype(cursor, entitytype_name)

            table_name = get_table_names_v4(cursor, [datasource], gp, entitytype,
                timestamp, timestamp)[0]

        query = (
            "SELECT 1 FROM \"{0}\".\"{1}\" WHERE timestamp = %s "
            "LIMIT 1".format(schema.name, table_name))

        with closing(self.conn.cursor()) as cursor:
            try:
                cursor.execute(query, (timestamp,))
                return bool(cursor.rowcount)
            except (psycopg2.ProgrammingError, psycopg2.InternalError):
                return False

    def is_complete(self, interval, datasource, gp, entitytype_name,
        filter=None, ratio=1):
        """
        Returns False when trend data is considered incomplete for a
        specific interval.

        Trend data is considered to be complete if:

            Two row counts are done:	row count for interval : (start, end) and
            a row count for the same interval a week earlier.

            The row counts are both non zero and their ratio is more than
            specified ratio

            If ref row count is zero, the check is done for the interval one
            day earlier (instead of a week earlier)
        """
        def _ratio(n, d):
            try:
                return n / d
            except (ZeroDivisionError, TypeError):
                return None

        complete = False
        row_count = partial(self.count, datasource, gp, entitytype_name,
            filter=filter)

        count = row_count(interval)
        ref_count = row_count([get_previous_timestamp(ts, 7 * 86400)
                for ts in interval])

        complete = _ratio(count, ref_count) >= ratio

        # Plan B: Try to compare with day earlier
        if ref_count == 0:
            ref_count = row_count([get_previous_timestamp(ts, 1 * 86400)
                    for ts in interval])
            complete = _ratio(count, ref_count) >= ratio

        return complete

    def store_raw(self, datasource, gp, timestamp, trend_names, raw_data_rows):
        if len(raw_data_rows) > 0:
            granularity = ensure_granularity(gp)
            raw_datapackage = RawDataPackage(granularity, timestamp, trend_names, raw_data_rows)
            transaction = store_raw(datasource, raw_datapackage)

            transaction.run(self.conn)

    def refine_data_rows(self, raw_data_rows):
        return refine_data_rows(self.conn, raw_data_rows)

    def aggregate(self, source, target, trend_names, timestamp):
        """
        :param source: tuple (datasource, gp, entitytype_name) specifying source
        :param target: tuple (datasource, gp, entitytype_name) specifying target
        :param trend_names: trends to aggregate
        :param timestamp: non-naive timestamp specifying end of interval to aggregate
        """
        aggregate(self.conn, schema.name, source, target, trend_names, timestamp)


def get_or_create_trendstore(cursor, datasource, entitytype, granularity):
    trendstore = TrendStore.get(cursor, datasource, entitytype, granularity)

    if trendstore is None:
        partition_size = PARTITION_SIZES.get(granularity.name)

        if partition_size is None:
            raise Exception("unsupported granularity size '{}'".format(
                    granularity.name))

        return TrendStore(datasource, entitytype, granularity,
                partition_size, "table").create(cursor)
    else:
        return trendstore


TrendPlugin.get_previous_timestamp = staticmethod(get_previous_timestamp)
TrendPlugin.get_next_timestamp = staticmethod(get_next_timestamp)
TrendPlugin.get_most_recent_timestamp = staticmethod(get_most_recent_timestamp)
