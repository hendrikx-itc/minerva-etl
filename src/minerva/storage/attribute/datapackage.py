# -*- coding: utf-8 -*-
"""Provides the DataPackage class."""
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
from collections import Iterable

from minerva.util import compose, expand_args, zipapply
from minerva.storage import datatype
from minerva.storage.attribute.attribute import Attribute


DEFAULT_DATATYPE = 'smallint'
SYSTEM_COLUMNS = "entity_id", "timestamp"


class DataPackage(object):
    """
    A DataPackage represents a batch of attribute records for the same
    EntityType. The EntityType is implicitly determined by the
    entities in the data package, and they must all be of the same EntityType.

    A graphical depiction of a DataPackage instance might be::

        +-------------------------------------------------+
        |         | "height" | "tilt" | "power" | "state" | <- attribute_names
        +---------+----------+--------+---------+---------+
        | 1234001 |    15.6  |    10  |     90  | "on"    | <- rows
        | 1234002 |    20.0  |     0  |     85  | "on"    |
        | 1234003 |    22.5  |     3  |     90  | "on"    |
        +---------+----------+--------+---------+---------+
    """
    def __init__(self, attribute_names, rows):
        self.attribute_names = attribute_names
        self.rows = rows

    def __str__(self):
        return str((self.attribute_names, self.rows))

    def is_empty(self):
        """Return True if the package has no data."""
        return len(self.rows) == 0 or len(self.attribute_names) == 0

    def deduce_data_types(self):
        """
        Return a list of the minimal required datatypes to store the values in
        this datapackage, in the same order as the values and thus matching the
        order of attribute_names.
        """
        return reduce(datatype.max_data_types, map(row_to_types, self.rows),
                      [DEFAULT_DATATYPE] * len(self.attribute_names))

    def deduce_attributes(self):
        """Return list of attributes matching the data in this package."""
        data_types = self.deduce_data_types()

        return map(expand_args(Attribute),
                   zip(self.attribute_names, data_types))

    def copy_expert(self, table, data_types):
        """
        Return a function that can execute a COPY FROM query on a cursor.

        :param data_types: A list of datatypes that determine how the values
                           should be rendered.
        """
        def fn(cursor):
            cursor.copy_expert(
                self._create_copy_from_query(table),
                self._create_copy_from_file(data_types)
            )

        return fn

    def _create_copy_from_query(self, table):
        """Return SQL query that can be used in the COPY FROM command."""
        column_names = chain(SYSTEM_COLUMNS, self.attribute_names)

        quote = partial(str.format, '"{}"')

        return "COPY {0}({1}) FROM STDIN".format(
            table.render(), ",".join(map(quote, column_names)))

    def _create_copy_from_file(self, data_types):
        """
        Return StringIO instance to use with COPY FROM command.

        :param data_types: A list of datatypes that determine how the values
        should be rendered.
        """
        copy_from_file = StringIO.StringIO()

        lines = self._create_copy_from_lines(data_types)

        copy_from_file.writelines(lines)

        copy_from_file.seek(0)

        return copy_from_file

    def _create_copy_from_lines(self, data_types):
        return [create_copy_from_line(data_types, row)
                for row in self.rows]

    def to_dict(self):
        """Return dictionary representing this package."""
        def render_row(row):
            return [row[0], row[1].isoformat()] + list(row[2:])

        return {
            "attribute_names": list(self.attribute_names),
            "rows": map(render_row, self.rows)
        }

    @classmethod
    def from_dict(cls, d):
        """Return DataPackage constructed from the dictionary."""
        return cls(
            attribute_names=d["attribute_names"],
            rows=d["rows"]
        )


snd = itemgetter(1)

types_from_values = partial(map, datatype.deduce_from_string)

row_to_types = compose(types_from_values, itemgetter(2))


def create_copy_from_line(data_types, row):
    """Return line compatible with COPY FROM command."""
    entity_id, timestamp, attributes = row

    value_mappers = map(value_mapper_by_type.get, data_types)

    values = chain(
        (str(entity_id), str(timestamp)),
        zipapply(value_mappers, attributes)
    )

    return "\t".join(values) + "\n"


def value_to_string(null_value="\\N"):
    def fn(value):
        if is_null_value(value):
            return null_value
        else:
            return str(value)

    return fn


def format_text_value(null_value='\\N'):
    def fn(value):
        if is_null_value(value):
            return null_value
        else:
            return value.replace('\t', '\\\t')

    return fn


def is_null_value(value):
    return (
        value is None or
        (isinstance(value, (str, unicode)) and len(value) == 0)
    )


def array_value_to_string(value):
    """Return PostgreSQL compatible string for ARRAY-like variable."""
    if isinstance(value, str):
        return "{" + value + "}"
    elif isinstance(value, Iterable):
        return "{" + ",".join(map(value_to_string('NULL'), value)) + "}"
    else:
        raise Exception("Unexpected type '{}'".format(type(value)))


def text_array_value_to_string(value):
    """Return PostgreSQL compatible string for ARRAY-like variable."""
    if isinstance(value, str):
        return "{" + value + "}"
    elif isinstance(value, Iterable):
        return "{" + ",".join(map(format_text_value('NULL'), value)) + "}"
    else:
        raise Exception("Unexpected type '{}'".format(type(value)))


value_mapper_by_type = {
    "text": format_text_value(),
    "bigint[]": array_value_to_string,
    "integer[]": array_value_to_string,
    "smallint[]": array_value_to_string,
    "text[]": text_array_value_to_string,
    "bigint": value_to_string(),
    "integer": value_to_string(),
    "smallint": value_to_string(),
    "boolean": value_to_string(),
    "real": value_to_string(),
    "double precision": value_to_string(),
    "timestamp without time zone": value_to_string(),
    "numeric": value_to_string()
}


quote_ident = partial(str.format, '"{}"')
