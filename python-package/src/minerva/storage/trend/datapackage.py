# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.storage.generic import extract_data_types


class DataPackage(object):
    """
    A DataPackage represents a batch of trend records for the same EntityType
    granularity and timestamp. The EntityType is implicitly determined by the
    entities in the data package, and they must all be of the same EntityType.

    A graphical depiction of a DataPackage instance might be::

        +-------------------------------------------------+
        | '2013-08-30 15:00:00+02:00'                     | <- timestamp
        |-------------------------------------------------+
        | GranularitySeconds(900)                         | <- granularity
        +-------------------------------------------------+
        |         | "cntrA"  | "ctrB" | "cntrC" | "ctrD"  | <- trend_names
        +---------+----------+--------+---------+---------+
        | 1234001 |    15.6  |    10  |     90  | "on"    | <- rows
        | 1234002 |    20.0  |     0  |     85  | "on"    |
        | 1234003 |    22.5  |     3  |     90  | "on"    |
        +---------+----------+--------+---------+---------+
    """
    def __init__(self, granularity, timestamp, trend_names, rows):
        self.granularity = granularity
        self.timestamp = timestamp
        self.trend_names = trend_names
        self.rows = rows

    def deduce_data_types(self):
        """
        Return a list of the minimal required datatypes to store the values in
        this datapackage, in the same order as the values and thus matching the
        order of attribute_names.
        """
        return extract_data_types(self.rows)

    def is_empty(self):
        """Return True if the package has no data rows."""
        return len(self.rows) == 0

    def as_tuple(self):
        """
        Return the legacy tuple (granularity, timestamp, trend_names, rows)
        """
        return self.granularity, self.timestamp, self.trend_names, self.rows

    def transform_rows(self, transformer):
        return self.__class__(
            self.granularity,
            self.timestamp,
            self.trend_names,
            map(transformer, self.rows)
        )