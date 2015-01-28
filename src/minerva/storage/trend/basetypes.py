# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2011 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from granularity import ensure_granularity


class Trend(object):
    def __init__(
            self, id, name, description, datasource_id, entitytype_id,
            granularity):
        self.id = id
        self.name = name
        self.description = description
        self.datasource_id = datasource_id
        self.entitytype_id = entitytype_id
        self.granularity = ensure_granularity(granularity)

    def __repr__(self):
        return "<Trend({0}/{1}/{2}/{3})>".format(
            self.name, self.datasource_id, self.entitytype_id, self.granularity
        )

    def __str__(self):
        return self.name


class TrendTag(object):
    def __init__(self, id, name):
        self.id = id
        self.name = name

    def __repr__(self):
        return "<TrendTag({0})>".format(self.name)

    def __str__(self):
        return self.name
