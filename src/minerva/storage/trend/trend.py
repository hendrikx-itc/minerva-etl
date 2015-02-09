# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class TrendDescriptor(object):
    def __init__(self, name, data_type, description):
        self.name = name
        self.data_type = data_type
        self.description = description


class Trend(object):
    def __init__(self, id, name, data_type, trendstore_id, description):
        self.id = id
        self.name = name
        self.data_type = data_type
        self.trendstore_id = trendstore_id
        self.description = description

    @staticmethod
    def create(trendstore_id, descriptor):
        def f(cursor):
            query = (
                "INSERT INTO trend_directory.trend ("
                "name, data_type, trendstore_id, description"
                ") "
                "VALUES (%s, %s, %s, %s) "
                "RETURNING *"
            )

            args = (
                descriptor.name, descriptor.data_type, trendstore_id,
                descriptor.description
            )

            cursor.execute(query, args)

            return Trend(*cursor.fetchone())

        return f