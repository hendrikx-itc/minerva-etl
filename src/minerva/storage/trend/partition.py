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
from minerva.db.query import Table


class Partition():
    """
    A partition of a trend store.
    """
    def __init__(self, index, trend_store):
        self.index = index
        self.trend_store = trend_store

    def name(self):
        return "{}_{}".format(self.trend_store.base_table_name(), self.index)

    def start(self):
        return self.trend_store.partitioning.timestamp(self.index)

    def end(self):
        return self.trend_store.partitioning.timestamp(self.index + 1)

    def __str__(self):
        return self.name()

    def table(self):
        return Table("trend_partition", self.name())

    def timestamps(self):
        current = self.start

        while current < self.end:
            current = self.trend_store.granularity.inc(current)

            yield current

    def create(self, cursor):
        query = (
            "SELECT trend_directory.create_partition(trend_store, %s) "
            "FROM trend_directory.trend_store "
            "WHERE id = %s"
        )
        args = self.index, self.trend_store.id

        try:
            try:
                cursor.execute(query, args)
            except Exception as exc:
                print(exc)
        except psycopg2.IntegrityError:
            raise DuplicateTable()
        except psycopg2.ProgrammingError as exc:
            raise translate_postgresql_exception(exc)
