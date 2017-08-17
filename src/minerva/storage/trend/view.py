# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class View(object):
    def __init__(self, trendstore, sql):
        self.id = None
        self.trendstore = trendstore
        self.sql = sql

    def define(self, cursor):
        query = (
            "SELECT (trend.define_view(trendstore, %s)).id "
            "FROM trend.trendstore "
            "WHERE id = %s")

        args = self.sql, self.trendstore.id

        cursor.execute(query, args)

        view_id, = cursor.fetchone()

        self.id = view_id

        return self

    def create(self, cursor):
        query = (
            "SELECT trend.create_view(view) "
            "FROM trend.view "
            "WHERE id = %s")

        args = self.id,

        cursor.execute(query, args)

        return self
