
# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from datetime import datetime

from dateutil.parser import parse as parse_timestamp
import pytz

from minerva.directory.distinguishedname import entitytype_name_from_dn
from minerva.directory.helpers_v4 import dns_to_entity_ids
from minerva.storage.attribute.datapackage import DataPackage


class RawDataPackage(DataPackage):
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

    def refine(self, cursor):
        dns, value_rows = zip(*self.rows)

        entity_ids = dns_to_entity_ids(cursor, list(dns))

        refined_value_rows = map(refine_values, value_rows)

        if isinstance(self.timestamp, datetime):
            timestamp = self.timestamp
        elif isinstance(self.timestamp, str):
            naive_timestamp = datetime.strptime(self.timestamp,
                                                "%Y-%m-%dT%H:%M:%S")
            timestamp = pytz.utc.localize(naive_timestamp)
        else:
            raise Exception("timestamp should be datetime or string")

        rows = zip(entity_ids, refined_value_rows)
        return DataPackage(timestamp, self.attribute_names, rows)

    def to_dict(self):
        def row_to_list(row):
            entity_id, values = row

            result = [entity_id]
            result.extend(values)

            return result

        return {
            "timestamp": self.timestamp.isoformat(),
            "attribute_names": list(self.attribute_names),
            "rows": map(row_to_list, self.rows)
        }

    @staticmethod
    def from_dict(d):
        def list_to_row(l):
            return l[0], l[1:]

        return RawDataPackage(
            timestamp=parse_timestamp(d["timestamp"]),
            attribute_names=d["attribute_names"],
            rows=map(list_to_row, d["rows"]))


def refine_values(raw_values):
    values = []

    for value in raw_values:
        if type(value) is tuple:
            joined = ",".join(value)

            if len(joined) > 0:
                values.append(joined)
            else:
                values.append(None)
        elif len(value) == 0:
            values.append(None)
        else:
            values.append(value)

    return values
