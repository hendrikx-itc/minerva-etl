# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class View():
    def __init__(self, trend_store, sql):
        self.id = None
        self.trend_store = trend_store
        self.sql = sql

    def define(self, cursor):
        query = (
            "SELECT (trend_directory.define_view(trend_store)).id "
            "FROM trend_directory.trend_store "
            "WHERE id = %s"
        )

        args = self.trend_store.id,

        cursor.execute(query, args)

        view_id, = cursor.fetchone()

        self.id = view_id

        return self

    def create(self, cursor):
        query = (
            "SELECT trend_directory.create_view(view, %s) "
            "FROM trend_directory.view "
            "WHERE id = %s"
        )

        args = self.sql, self.id

        cursor.execute(query, args)

        return self
