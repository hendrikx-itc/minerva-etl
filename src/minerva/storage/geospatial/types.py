# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class Point():
    def __init__(self, x, y, srid=None):
        self.x = x
        self.y = y
        self.srid = srid

    def as_sql(self):
        sql = make_point(self.x, self.y)

        if self.srid:
            return set_srid(sql, self.srid)
        else:
            return sql


class Site():
    def __init__(self, entity_id, position):
        self.entity_id = entity_id
        self.position = position


class Cell():
    def __init__(self, entity_id, azimuth, type):
        self.entity_id = entity_id
        self.azimuth = azimuth
        self.type = type


def make_point(x, y):
    return "ST_MakePoint({}, {})".format(x, y)


def set_srid(point, srid):
    return "ST_SetSRID({}, {})".format(point, srid)


def transform_srid(point, target_srid):
    return "ST_Transform({}, {})".format(point, target_srid)
