# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import StringIO
from functools import partial
from operator import itemgetter
from itertools import chain

from dateutil.parser import parse as parse_timestamp

from minerva.util import compose, expand_args, zipapply
from minerva.storage import datatype
from minerva.storage.attribute.attribute import Attribute


DEFAULT_DATATYPE = 'smallint'
SYSTEM_COLUMNS = "entity_id", "timestamp"


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
        return reduce(datatype.max_datatypes, map(row_to_types, self.rows),
                      [DEFAULT_DATATYPE] * len(self.attribute_names))

    def deduce_attributes(self):
        data_types = self.deduce_data_types()

        return map(expand_args(Attribute),
                   zip(self.attribute_names, data_types))

    def create_copy_from_query(self, table):
        column_names = chain(SYSTEM_COLUMNS, self.attribute_names)

        quote = partial(str.format, '"{}"')

        query = "COPY {0}({1}) FROM STDIN".format(
            table.render(), ",".join(map(quote, column_names)))

        return query

    def create_copy_from_file(self, data_types):
        copy_from_file = StringIO.StringIO()

        lines = self._create_copy_from_lines(data_types)

        copy_from_file.writelines(lines)

        copy_from_file.seek(0)

        return copy_from_file

    def _create_copy_from_lines(self, data_types):
        return [create_copy_from_line(self.timestamp, data_types, r)
                for r in self.rows]

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

        return DataPackage(
            timestamp=parse_timestamp(d["timestamp"]),
            attribute_names=d["attribute_names"],
            rows=map(list_to_row, d["rows"]))


snd = itemgetter(1)

types_from_values = partial(map, datatype.deduce_from_value)

txt_values_from_row = compose(partial(map, str), snd)

#row_to_types = compose(types_from_values, txt_values_from_row)
row_to_types = compose(types_from_values, snd)


def create_copy_from_line(timestamp, data_types, row):
    entity_id, attributes = row
    print(data_types)

    value_mappers = map(value_mapper_by_type.get, data_types)

    values = chain((str(entity_id), str(timestamp)), zipapply(value_mappers, attributes))

    return "\t".join(values) + "\n"


def value_to_string(value):
    if isinstance(value, list):
        return "{" + ",".join(map(str, value)) + "}"
    elif isinstance(value, str):
        return "{" + value + "}"


value_mapper_by_type = {
    "text": str,
    "bigint[]": value_to_string,
    "integer[]": value_to_string,
    "smallint[]": value_to_string,
    "text[]": value_to_string,
    "bigint": str,
    "integer": str,
    "smallint": str,
    "boolean": str,
    "real": str,
    "double precision": str,
    "timestamp without time zone": str,
    "numeric": str
}


quote_ident = partial(str.format, '"{}"')
