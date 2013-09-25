# -*- coding: utf-8 -*-
"""Provides the RawDataPackage class."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from datetime import datetime

import pytz

from minerva.directory.distinguishedname import entitytype_name_from_dn
from minerva.directory.helpers_v4 import dns_to_entity_ids
from minerva.storage.attribute.datapackage import DataPackage


class RawDataPackage(DataPackage):
    """A DataPackage subclass with refining functionality."""
    def get_entitytype_name(self):
        """Return the entity type name from the first Distinguished Name."""
        if self.rows:
            first_dn = self.rows[0][0]

            return entitytype_name_from_dn(first_dn)

    def get_key(self):
        """
        Return key by which this package could be merged with other packages.
        """
        return self.timestamp, self.get_entitytype_name()

    def refine(self, cursor):
        """
        Return a DataPackage with 'refined' data of this package.

        This means that:

        * If the timestamp of this raw datapackage is a string,
        it will be parsed as a timestamp in UTC timezone.
        * All distinguished names are translated to entity Ids.
        """
        dns, value_rows = zip(*self.rows)

        entity_ids = dns_to_entity_ids(cursor, list(dns))

        if isinstance(self.timestamp, datetime):
            timestamp = self.timestamp
        elif isinstance(self.timestamp, str):
            naive_timestamp = datetime.strptime(self.timestamp,
                                                "%Y-%m-%dT%H:%M:%S")
            timestamp = pytz.utc.localize(naive_timestamp)
        else:
            raise Exception("timestamp should be datetime or string")

        rows = zip(entity_ids, value_rows)
        return DataPackage(timestamp, self.attribute_names, rows)
