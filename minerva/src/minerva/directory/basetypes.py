# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import datetime

import pytz


class EntityType(object):
    def __init__(self, id, name, description):
        self.id = id
        self.name = name
        self.description = description

    def __repr__(self):
        return "<EntityType({0})>".format(self.name)

    def __str__(self):
        return self.name


class Entity(object):
    """
    All data within the Minerva platform is linked to entities. Entities are
    very minimal objects with only very generic properties such as name,
    parent, type and a few more.
    """
    def __init__(self, id, name, entitytype_id, dn, parent_id):
        self.id = id
        self.first_appearance = pytz.utc.localize(datetime.datetime.utcnow())
        self.name = name
        self.entitytype_id = entitytype_id
        self.dn = dn
        self.parent_id = parent_id

    def __repr__(self):
        return "<Entity('{0:s}')>".format(self.name)

    def __str__(self):
        return self.name


class DataSource(object):
    """
    A DataSource describes where a certain set of data comes from.
    """
    def __init__(self, id, name, description="", timezone="UTC"):
        self.id = id
        self.name = name
        self.description = description
        self.timezone = timezone

    def __str__(self):
        return self.name

    def get_tzinfo(self):
        return pytz.timezone(self.timezone)

    def set_tzinfo(self, tzinfo):
        self.timezone = tzinfo.zone

    tzinfo = property(get_tzinfo, set_tzinfo)


class TagGroup(object):
    def __init__(self, id, name, complementary):
        self.id = id
        self.name = name
        self.complementary = complementary


class Tag(object):
    def __init__(self, id, name, group_id, description=""):
        self.id = id
        self.name = name
        self.description = description
        self.group_id = group_id
