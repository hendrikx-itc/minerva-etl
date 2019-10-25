# -*- coding: utf-8 -*-
"""Provides the DataPackage class."""
from io import StringIO
from itertools import chain

from minerva.db.util import quote_ident
from minerva.util import zip_apply
from minerva.storage import datatype
from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage.attribute.attribute import AttributeDescriptor

from minerva.directory.distinguishedname import entity_type_name_from_dn
from minerva.directory.helpers import aliases_to_entity_ids


SYSTEM_COLUMNS = "entity_id", "timestamp"


class DataPackage:
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
    def __init__(self, attribute_names, rows, alias_type='dn'):
        self.attribute_names = attribute_names
        self.rows = rows
        self.alias_type = alias_type

    def __str__(self):
        return str((self.attribute_names, self.rows))

    def is_empty(self):
        """Return True if the package has no data."""
        return len(self.rows) == 0 or len(self.attribute_names) == 0

    def deduce_value_descriptors(self):
        """
        Return a list of the minimal required data types to store the values in
        this data package, in the same order as the values and thus matching
        the order of attribute_names.
        """
        if self.is_empty():
            if len(self.attribute_names):
                return [
                    ValueDescriptor(name, datatype.registry['smallint'])
                    for name in self.attribute_names
                ]
            else:
                return []
        else:
            return [
                ValueDescriptor(name, data_type)
                for name, data_type in zip(
                    self.attribute_names, datatype.deduce_data_types(
                     (values for alias, timestamp, values in self.rows)
                    ))
                 ]

    def deduce_attributes(self):
        """Return list of attributes matching the data in this package."""
        return [
            AttributeDescriptor(
                value_descriptor.name, value_descriptor.data_type, ''
            )
            for value_descriptor in self.deduce_value_descriptors()
        ]

    def copy_expert(self, table, output_descriptors):
        """
        Return a function that can execute a COPY FROM query on a cursor.

        :param data_types: A list of datatypes that determine how the values
                           should be rendered.
        """
        def fn(cursor):
            cursor.copy_expert(
                self._create_copy_from_query(table),
                self._create_copy_from_file(output_descriptors)
            )

        return fn

    def _create_copy_from_query(self, table):
        """Return SQL query that can be used in the COPY FROM command."""
        column_names = chain(SYSTEM_COLUMNS, self.attribute_names)

        return "COPY {0}({1}) FROM STDIN".format(
            table.render(), ",".join(map(quote_ident, column_names))
        )

    def _create_copy_from_file(self, output_descriptors):
        """
        Return StringIO instance to use with COPY FROM command.

        :param data_types: A list of datatypes that determine how the values
        should be rendered.
        """
        copy_from_file = StringIO()

        lines = self._create_copy_from_lines(output_descriptors)

        copy_from_file.writelines(lines)

        copy_from_file.seek(0)

        return copy_from_file

    def _create_copy_from_lines(self, output_descriptors):
        value_mappers = [
            output_descriptor.serialize
            for output_descriptor in output_descriptors
        ]

        return [
            create_copy_from_line(value_mappers, row)
            for row in self.rows
        ]

    def to_dict(self):
        """Return dictionary representing this package."""
        def render_row(row):
            return [row[0], row[1].isoformat()] + list(row[2:])

        return {
            "attribute_names": list(self.attribute_names),
            "rows": list(map(render_row, self.rows))
        }

    @classmethod
    def from_dict(cls, d):
        """Return DataPackage constructed from the dictionary."""
        return cls(
            attribute_names=d["attribute_names"],
            rows=d["rows"]
        )

    def get_entity_type_name(self):
        """Return the entity type name from the first Distinguished Name."""
        if self.alias_type == 'dn':
            if self.rows:
                first_dn = self.rows[0][0]

                return entity_type_name_from_dn(first_dn)

        return self.alias_type

    def get_key(self):
        """Return key by which to merge this package with other packages."""
        return self.get_entity_type_name()

    def refine(self, cursor):
        """
        Return a DataPackage with 'refined' data of this package.

        This means that all distinguished names are translated to entity Ids.

        """
        aliases, timestamps, value_rows = zip(*self.rows)

        entity_ids = aliases_to_entity_ids(cursor, self.alias_type, list(aliases), self.get_entity_type_name())

        rows = zip(entity_ids, timestamps, value_rows)
        return DataPackage(self.attribute_names, rows)


def create_copy_from_line(value_mappers, row):
    """Return line compatible with COPY FROM command."""
    entity_id, timestamp, attributes = row

    values = chain(
        (str(entity_id), str(timestamp)),
        zip_apply(value_mappers)(attributes)
    )

    return "\t".join(values) + "\n"
