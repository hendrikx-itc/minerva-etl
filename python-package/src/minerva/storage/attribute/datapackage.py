# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from functools import partial
from operator import itemgetter

from minerva.util import compose, expand_args
from minerva.storage import datatype
from minerva.storage.attribute.attribute import Attribute


class DataPackage(object):
    """
    A DataPackage represents a batch of attribute records for the same
    EntityType and timestamp. The EntityType is implicitly determined by the
    entities in the data package, and they must all be of the same EntityType.

    A graphical depiction of a DataPackage instance might be::

    +-------------------------------------------------+
    | '2013-08-30 15:00:00+02:00'                     | <- timestamp
    +-------------------------------------------------+
    |         | "height" | "tilt" | "power" | "state" | <- attribute_names
    +---------+----------+--------+---------+---------+
    | 1234001 |    15.6  |    10  |     90  | "on"    | <- rows
    | 1234002 |    20.0  |     0  |     85  | "on"    |
    | 1234003 |    22.5  |     3  |     90  | "on"    |
    +---------+----------+--------+---------+---------+
    """
    def __init__(self, timestamp, attribute_names, rows):
        self.timestamp = timestamp
        self.attribute_names = attribute_names
        self.rows = rows

    def __str__(self):
        return str((self.timestamp, self.attribute_names, self.rows))

    def is_empty(self):
        return len(self.rows) == 0

    def deduce_data_types(self):
        """
        Return a list of the minimal required datatypes to store the values in
        this datapackage, in the same order as the values and thus matching the
        order of attribute_names.
        """
        return reduce(datatype.max_datatypes, map(row_to_types, self.rows))

    def deduce_attributes(self):
        data_types = self.deduce_data_types()

        return map(expand_args(Attribute),
                   zip(self.attribute_names, data_types))


snd = itemgetter(1)

types_from_values = partial(map, datatype.deduce_from_value)

row_to_types = compose(types_from_values, snd)
