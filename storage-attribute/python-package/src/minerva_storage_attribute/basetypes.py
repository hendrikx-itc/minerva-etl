# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.directory.distinguishedname import entitytype_name_from_dn


class RawDataPackage(object):
    def __init__(self, timestamp, attribute_names, rows):
        self.timestamp = timestamp
        self.attribute_names = attribute_names
        self.rows = rows

    def get_entitytype_name(self):
        if self.rows:
            first_dn = self.rows[0][0]

            return entitytype_name_from_dn(first_dn)

    def get_key(self):
        return self.timestamp, self.get_entitytype_name()

    def as_tuple(self):
        """
        Return the legacy tuple (timestamp, attribute_names, rows)
        """
        return self.timestamp, self.attribute_names, self.rows

    def is_empty(self):
        return len(self.rows) == 0


class AttributeTag(object):
    def __init__(self, id, name):
        self.id = id
        self.name = name

    def __repr__(self):
        return "<AttributeTag({0})>".format(self.name)

    def __str__(self):
        return self.name
