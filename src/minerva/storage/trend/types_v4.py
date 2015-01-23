# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.util import head


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
