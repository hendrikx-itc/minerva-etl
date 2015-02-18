# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from io import StringIO
from itertools import chain

from minerva.db.util import quote_ident
from minerva.storage.datatype import deduce_data_types
from minerva.storage.trend import schema
from minerva.util import grouped_by, zip_apply, identity, k
from minerva.directory.entityref import EntityDnRef, EntityIdRef
from minerva.directory.distinguishedname import entity_type_name_from_dn


class DataPackageBase():
    """
    A DataPackage represents a batch of trend records for the same EntityType
    granularity and timestamp. The EntityType is implicitly determined by the
    entities in the data package, and they must all be of the same EntityType.

    A graphical depiction of a DataPackage instance might be::

        +---------------------------------------------------+
        | '2013-08-30 15:00:00+02:00'                       | <- timestamp
        |---------------------------------------------------+
        | Granularity('900')                                | <- granularity
        +---------------------------------------------------+
        |         | "cntrA"  | "cntrB" | "cntrC" | "cntrD"  | <- trend_names
        +---------+----------+---------+---------+----------+
        | 1234001 |    15.6  |    10   |     90  | "on"     | <- rows
        | 1234002 |    20.0  |     0   |     85  | "on"     |
        | 1234003 |    22.5  |     3   |     90  | "on"     |
        +---------+----------+---------+---------+----------+
    """
    def __init__(
            self, granularity, timestamp, trend_names, rows):
        self.granularity = granularity
        self.timestamp = timestamp
        self.trend_names = trend_names
        self.rows = rows

    @classmethod
    def entity_ref_type(cls):
        raise NotImplementedError()

    def is_empty(self):
        """Return True if the package has no data rows."""
        return len(self.rows) == 0

    def deduce_data_types(self):
        """
        Return a list of the minimal required data types to store the values in
        this data package, in the same order as the values and thus matching the
        order of attribute_names.
        """
        return deduce_data_types(values for entity_ref, values in self.rows)

    def get_key(self):
        return (
            self.__class__, self.timestamp,
            self.entity_type_name(), self.granularity
        )

    def refined_rows(self, value_parsers):
        def f(cursor):
            entity_refs, value_rows = zip(*self.rows)

            entity_ids = self.entity_ref_type().map_to_entity_ids(
                list(entity_refs)
            )(cursor)

            refine_values = self.refine_values(value_parsers)

            refined_value_rows = list(map(refine_values, value_rows))

            return list(zip(entity_ids, refined_value_rows))

        return f

    @staticmethod
    def merge_packages(packages):
        return [
            package_group(k, list(group))
            for k, group in grouped_by(packages, DataPackageBase.get_key)
        ]

    def copy_from(self, table, value_descriptors, modified):
        """
        Return a function that can execute a COPY FROM query on a cursor.
        """
        def fn(cursor):
            cursor.copy_expert(
                self._create_copy_from_query(table),
                self._create_copy_from_file(value_descriptors, modified)
            )

        return fn

    def _create_copy_from_query(self, table):
        """Return SQL query that can be used in the COPY FROM command."""
        column_names = chain(schema.system_columns, self.trend_names)

        return "COPY {0}({1}) FROM STDIN".format(
            table.render(),
            ",".join(map(quote_ident, column_names))
        )

    def _create_copy_from_file(self, value_descriptors, modified):
        copy_from_file = StringIO()

        copy_from_file.writelines(
            self._create_copy_from_lines(value_descriptors, modified)
        )

        copy_from_file.seek(0)

        return copy_from_file

    def _create_copy_from_lines(self, value_descriptors, modified):
        value_mappers = [
            value_descriptor.serialize_to_string
            for value_descriptor in value_descriptors
        ]

        map_values = zip_apply(value_mappers)

        return (
            u"{0:d}\t'{1!s}'\t'{2!s}'\t{3}\n".format(
                entity_id,
                self.timestamp.isoformat(),
                modified.isoformat(),
                "\t".join(map_values(values))
            )
            for entity_id, values in self.rows
        )


def package_group(key, packages):
    cls, timestamp, _entitytype_name, granularity = key

    all_field_names = set()
    dict_rows_by_entity_ref = {}

    for p in packages:
        for entity_ref, values in p.rows:
            value_dict = dict(zip(p.trend_names, values))

            dict_rows_by_entity_ref.setdefault(
                entity_ref, {}
            ).update(value_dict)

        all_field_names.update(p.trend_names)

    field_names = list(all_field_names)

    rows = []
    for entity_ref, value_dict in dict_rows_by_entity_ref.items():
        values = [value_dict.get(f, "") for f in field_names]

        row = entity_ref, values

        rows.append(row)

    return cls(granularity, timestamp, field_names, rows)


def data_package_type(name, entity_ref_type, entity_type_name, refine_values):
    """
    @type entity_ref_type: EntityRef
    @type entity_type_name: function(DataPackage) -> str
    @type refine_values: function(parsers) -> function(values) -> refined_values
    """
    return type(name, (DataPackageBase,), {
        'entity_ref_type': classmethod(lambda cls: entity_ref_type),
        'entity_type_name': entity_type_name,
        'refine_values': staticmethod(refine_values)
    })


def from_first_dn(data_package):
    dn = data_package.rows[0][0]

    return entity_type_name_from_dn(dn)


def parse_values(parsers):
    return zip_apply(parsers)


def values_pass_through(parsers):
    return identity


DefaultPackage = data_package_type(
    'DataPackageClassic', EntityDnRef, from_first_dn, parse_values
)


def refined_package_type_for_entity_type(entity_type_name):
    """
    :param entity_type_name: name of entity type
    :return: new DataPackageBase subclass
    """
    return data_package_type(
        '{}DataPackage'.format(entity_type_name),
        EntityIdRef,
        k(entity_type_name),
        values_pass_through
    )