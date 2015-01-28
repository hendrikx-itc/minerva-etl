# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import psycopg2

from minerva.db.error import translate_postgresql_exception, DuplicateTable
from minerva.db.query import Table, Column, Eq, And, Call
from minerva.storage.trend import schema


class Partition(object):
    """
    A partition of a trend store.
    """
    def __init__(self, index, name, trendstore, start, end):
        self.index = index
        self.name = name
        self.trendstore = trendstore
        self.start = start
        self.end = end

        self.last_modified = self._last_modified()
        self.max_modified = self._max_modified()

    def __str__(self):
        return self.name

    def table(self):
        return Table("trend", self.name)

    def timestamps(self):
        current = self.start

        while current < self.end:
            current = self.trendstore.granularity.inc(current)

            yield current

    def _last_modified(self):
        end_col = Column("end")
        table_name_col = Column("table_name")
        timestamp_col = Column("timestamp")

        return schema.modified.select(
            [end_col]
        ).where_(
            And(
                Eq(table_name_col, self.name),
                Eq(timestamp_col)
            )
        )

    def _max_modified(self):
        timestamp_col = Column("timestamp")
        table = self.table()

        return table.select(
            Call("max", Column("modified"))
        ).where_(Eq(timestamp_col))

    def create(self, cursor):
        query = (
            "SELECT trend.create_partition(trendstore, %s) "
            "FROM trend.trendstore "
            "WHERE id = %s"
        )
        args = self.index, self.trendstore.id

        try:
            cursor.execute(query, args)
        except psycopg2.IntegrityError:
            raise DuplicateTable()
        except psycopg2.ProgrammingError as exc:
            raise translate_postgresql_exception(exc)

    def check_columns_exist(self, column_names, data_types):
        return self.trendstore.check_columns_exist(column_names, data_types)

    def check_column_types(self, column_names, data_types):
        return self.trendstore.check_column_types(column_names, data_types)

    def clear_timestamp(self, timestamp):
        def f(cursor):
            query = (
                "DELETE FROM {} "
                "WHERE timestamp = %s"
            ).format(self.table.render())
            args = timestamp,

            cursor.execute(query, args)

        return f
