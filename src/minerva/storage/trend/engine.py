# -*- coding: utf-8 -*-
from __future__ import division
"""
Plugin API v4
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing
from functools import partial

import psycopg2

from minerva.util import first
from minerva.db.query import Table
from minerva.storage.trend import schema
from minerva.storage.trend.tables import PARTITION_SIZES
from minerva.storage.trend.trendstore import TrendStore


class TrendEngine():
    def __init__(self, conn):
        self.conn = conn

    def get_trend_by_id(self, trend_id):
        return get_trend_by_id(self.conn, trend_id)

    def get_trendstore(self, datasource, entitytype, granularity):
        with closing(self.conn.cursor()) as cursor:
            return TrendStore.get(cursor, datasource, entitytype, granularity)

    def create_trendstore(self, datasource, entitytype, granularity):
        partition_size = PARTITION_SIZES[str(granularity)]
        trendstore = TrendStore(
            datasource, entitytype, granularity, partition_size, 'table'
        )

        with closing(self.conn.cursor()) as cursor:
            return trendstore.create(cursor)

    def store_txn(self, trendstore, datapackage):
        return trendstore.store(datapackage)

    def store(self, trendstore, datapackage):
        transaction = self.store_txn(trendstore, datapackage)
        transaction.run(self.conn)

    def retrieve(
            self, trendstores, trend_names, entities, start, end,
            subquery_filter=None, relation_table_name=None, limit=None):

        if isinstance(trendstores, TrendStore):
            trendstores = [trendstores]

        entitytype = trendstores[0].entitytype

        tables = map(
            partial(Table, "trend"),
            get_table_names_v4(trendstores, start, end)
        )

        with closing(self.conn.cursor()) as cursor:
            return retrieve(
                cursor, tables, trend_names, entities, start, end,
                subquery_filter, relation_table_name, limit,
                entitytype=entitytype
            )

    def retrieve_orderedby_time(
            self, datasources, gp, entitytype, trend_names, entities, start,
            end, limit=None):

        table_names = get_table_names_v4(
            datasources, gp, entitytype, start, end
        )

        return retrieve_orderedby_time(
            self.conn, schema.name, table_names, trend_names, entities, start,
            end, limit
        )

    def retrieve_aggregated(
            self, trendstore, column_identifiers, interval, group_by,
            subquery_filter=None, relation_table_name=None):

        with closing(self.conn.cursor()) as cursor:
            return retrieve_aggregated(
                cursor, trendstore, column_identifiers, interval, group_by,
                subquery_filter, relation_table_name
            )

    def retrieve_related(
            self, datasources, granularity, source_entitytype,
            target_entitytype, trend_names, start, end, subquery_filter=None,
            limit=None):

        table_names = get_table_names_v4(
            datasources, granularity, target_entitytype, start, end
        )

        if source_entitytype.name == target_entitytype.name:
            relation_table_name = "self"
        else:
            relation_table_name = "{}->{}".format(
                source_entitytype.name, target_entitytype.name
            )

        return retrieve_related(
            self.conn, schema.name, relation_table_name, table_names,
            trend_names, start, end, subquery_filter, limit
        )

    def count(self, trendstore, interval, filter=None):
        """
        Returns row count for specified trendstore and interval
        """
        (start, end) = interval

        tables = trendstore.tables(start, end)
        table_names = [table.name for table in tables]

        query_template = (
            "SELECT COUNT(*) FROM {} "
            "WHERE timestamp > %s AND timestamp <= %s"
        )

        if filter is not None:
            if len(filter) == 0:
                return 0
            else:
                query_template += " AND entity_id IN ({0}) ".format(
                    ",".join(str(id) for id in filter)
                )

        args = (start, end)

        with closing(self.conn.cursor()) as cursor:
            def count_for_table(table):
                query = query_template.format(table.render())
                try:
                    cursor.execute(query, args)
                except (psycopg2.ProgrammingError, psycopg2.InternalError):
                    return 0
                else:
                    return first(cursor.fetchone())

            return sum(map(count_for_table, table_names))

    def last_modified(self, interval, trendstore, subquery_filter=None):
        """
        Return last modified timestamp for specified trendstore and interval
        :param interval: tuple (start, end) with non-naive timestamps,
            specifying interval to check
        :param trendstore: trendstore object
        :param subquery_filter: subquery for additional filtering
            by JOINing on field 'id'
        """
        (start, end) = interval

        tables = trendstore.tables(start, end)

        if subquery_filter:
            query = (
                "SELECT MAX(t.modified) FROM {0} AS t "
                "JOIN ({0}) AS filter ON filter.id = t.entity_id "
                "WHERE t.timestamp > %s AND t.timestamp <= %s"
            )
        else:
            query = (
                "SELECT MAX(t.modified) FROM {0} AS t "
                "WHERE t.timestamp > %s AND t.timestamp <= %s"
            )

        modifieds = []

        with closing(self.conn.cursor()) as cursor:
            for table in tables:
                try:
                    cursor.execute(query.format(table.render()), interval)
                    modified, = cursor.fetchone()
                    modifieds.append(modified)
                except (psycopg2.ProgrammingError, psycopg2.InternalError):
                    continue

        if modifieds:
            return max(modifieds)
        else:
            return None

    def timestamp_exists(self, trendstore, timestamp):
        """
        Returns True when timestamp occurs for specified data source.
        False otherwise.
        """
        table = trendstore.partition(timestamp).table()

        query = (
            "SELECT 1 FROM {} WHERE timestamp = %s "
            "LIMIT 1".format(table.render())
        )

        args = (timestamp,)

        with closing(self.conn.cursor()) as cursor:
            try:
                cursor.execute(query, args)
                return bool(cursor.rowcount)
            except (psycopg2.ProgrammingError, psycopg2.InternalError):
                return False

    def is_complete(self, interval, trendstore, filter=None, ratio=1):
        """
        Returns False when trend data is considered incomplete for a
        specific interval.

        Trend data is considered to be complete if:

            Two row counts are done: row count for interval : (start, end) and
            a row count for the same interval a week earlier.

            The row counts are both non zero and their ratio is more than
            specified ratio
        """
        complete = False
        row_count = partial(self.count, trendstore, filter=filter)

        count = row_count(interval)
        ref_count = row_count([
            get_previous_timestamp(ts, 7 * 86400)
            for ts in interval
        ])

        try:
            if count / ref_count >= ratio:
                complete = True
        except (ZeroDivisionError, TypeError):
            pass

        return complete
