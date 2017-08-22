# -*- coding: utf-8 -*-
from minerva.util import head
from minerva.db.query import Column, Eq
from minerva.directory.helpers_v4 import get_datasource_by_id, \
    get_entitytype_by_id, get_datasource, get_entitytype
from minerva.storage.trend import schema
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.partition import Partition

__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2012-2017 Hendrikx-ITC B.V.
Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


def partition_getter(datasource_name, entitytype_name, granularity, timestamp):
    def f(cursor):
        return get_partition(
            cursor, datasource_name, entitytype_name,
            granularity, timestamp)

    return f


def partition_getter_by_name(name):
    def f(cursor):
        return get_partition_by_name(cursor, name)

    return f


def get_partition_by_name(cursor, name):
    column_names = [
        "table_name", "datasource_id", "entitytype_id", "granularity",
        "data_start", "data_end"]
    columns = map(Column, column_names)

    query = schema.partition.select(columns, where_=Eq(Column("table_name")))

    args = name,

    query.execute(cursor, args)

    if cursor.rowcount > 0:
        name, datasource_id, entitytype_id, granularity_str, data_start, \
                data_end = cursor.fetchone()

        granularity = create_granularity(str(granularity_str))
        datasource = get_datasource_by_id(cursor, datasource_id)
        entitytype = get_entitytype_by_id(cursor, entitytype_id)

        trendstore = TrendStore(datasource, entitytype, granularity)

        return Partition(name, trendstore, data_start, data_end)
    else:
        return None


def get_partition(
        cursor, datasource_name, entitytype_name, granularity, timestamp):
    datasource = get_datasource(cursor, datasource_name)
    entitytype = get_entitytype(cursor, entitytype_name)
    granularity = create_granularity(granularity)

    trendstore = TrendStore(datasource, entitytype, granularity)

    return trendstore.partition(timestamp)


def link_trend(cursor, table_name, trend_id):
    query = (
        "INSERT INTO trend.trend_partition_link"
        "(trend_id, partition_table_name) "
        "VALUES (%s, %s)")

    args = trend_id, table_name

    cursor.execute(query, args)


def partition_has_trend(cursor, table_name, trend_name):
    query = (
        "SELECT 1 FROM trend.partition p "
        "JOIN trend.trend_partition_link tpl "
        "ON tpl.partition_table_name = p.table_name "
        "JOIN trend.trend t "
        "ON t.id = tpl.trend_id "
        "WHERE p.table_name = %s AND t.name = %s")

    args = table_name, trend_name

    cursor.execute(query, args)

    return cursor.rowcount > 0


def find_trend(cursor, entitytype, datasource, granularity, name):
    query = (
        "SELECT t.id, t.name "
        "FROM trend.trend t "
        "JOIN trend.trend_partition_link tpl ON tpl.trend_id = t.id "
        "JOIN trend.partition p ON p.table_name = tpl.partition_table_name "
        "WHERE "
        "p.entitytype_id = %s AND "
        "p.datasource_id = %s AND "
        "p.granularity = %s AND "
        "t.name = %s")

    args = entitytype.id, datasource.id, granularity.name, name

    cursor.execute(query, args)

    if cursor.rowcount > 0:
        return cursor.fetchone()


class View(object):
    def __init__(self, id, description, sql, trendstore_id):
        self.id = id
        self.description = description
        self.sql = sql
        self.trendstore_id = trendstore_id
        self.sources = []

    def __str__(self):
        return "{0.description}".format(self)

    @staticmethod
    def load_all(cursor):
        query = "SELECT id, description, sql, trendstore_id FROM trend.view"

        cursor.execute(query)

        rows = cursor.fetchall()

        views = [View(*row) for row in rows]

        def get_sources(cursor, view_id):
            query = (
                "SELECT trendstore_id "
                "FROM trend.view_trendstore_link "
                "WHERE view_id = %s")

            args = (view_id,)

            cursor.execute(query, args)

            return map(head, cursor.fetchall())

        for view in views:
            view.sources = get_sources(cursor, view.id)

        return views
