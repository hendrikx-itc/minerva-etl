# -*- coding: utf-8 -*-
from io import StringIO
from itertools import chain, groupby
from operator import itemgetter
from typing import Callable, List, Type

from minerva.db.util import quote_ident
from minerva.storage.trend import schema
from minerva.util import grouped_by, zip_apply, identity, k
from minerva.util.tabulate import render_table
from minerva.directory.entityref import EntityDnRef, EntityIdRef, EntityRef


class DataPackageType:
    def __init__(self, entity_ref_type, get_entity_type_name, alias_type='dn'):
        self.entity_ref_type = entity_ref_type
        self.get_entity_type_name = get_entity_type_name
        self.alias_type = alias_type


class DataPackage:
    """
    A DataPackage represents a batch of trend records for the same EntityType
    granularity and timestamp. The EntityType is implicitly determined by the
    entities in the data package, and they must all be of the same EntityType.
    """
    def __init__(
            self, data_package_type, granularity, trend_descriptors, rows):
        self.data_package_type = data_package_type
        self.granularity = granularity
        self.trend_descriptors = trend_descriptors
        self.rows = rows
        self.alias_type = data_package_type.alias_type

    def render_table(self):
        column_names = ["entity", "timestamp"] + list(
            trend_descriptor.name
            for trend_descriptor in self.trend_descriptors
        )
        column_align = ">" * len(column_names)
        column_sizes = ["max"] * len(column_names)

        rows = [row[:-1] + tuple(row[-1]) for row in self.rows]
        table = render_table(column_names, column_align, column_sizes, rows)

        return '\n'.join(table)

    def entity_type_name(self):
        return self.data_package_type.get_entity_type_name(self)

    def is_empty(self):
        """Return True if the package has no data rows."""
        return len(self.rows) == 0

    def filter_trends(self, fn):
        """
        :param fn: Filter function for trend names
        :return: A new data package with just the trend data for the trends
        filtered by provided function
        """
        value_getters, filtered_trend_descriptors = zip(*[
            (itemgetter(index), trend_descriptor)
            for index, trend_descriptor in enumerate(self.trend_descriptors)
            if fn(trend_descriptor.trend_name)
        ])

        return DataPackage(
            self.data_package_type,
            self.granularity,
            filtered_trend_descriptors,
            [
                (entity_ref, timestamp, tuple(g(values) for g in value_getters))
                for entity_ref, timestamp, values in self.rows
            ]
        )

    def split(self, group_fn):
        """
        Split the trends in this package by passing the trend name through the
        provided function. The trends with the same resulting key are placed in
        a new separate package.

        :param group_fn: Function that returns the group key for a trend name
        :return: A list of data packages with trends grouped by key
        """
        for key, group in grouped_by([
            (group_fn(trend_descriptor.name), itemgetter(index), trend_descriptor)
            for index, trend_descriptor in enumerate(self.trend_descriptors)
        ], key=itemgetter(0)):
            keys, value_getters, trend_names = zip(*list(group))

            yield key, DataPackage(self.data_package_type,
                self.granularity,
                trend_names,
                [
                    (entity_ref, timestamp, tuple(g(values) for g in value_getters))
                    for entity_ref, timestamp, values in self.rows
                ]
            )

    def get_key(self):
        return (
            self.data_package_type,
            self.entity_type_name(), self.granularity
        )

    def refined_rows(self, cursor):
        """
        Map the entity reference to an entity ID in each row and return the
        newly formed rows with IDs.
        """
        entity_refs, timestamps, value_rows = zip(*self.rows)

        entity_ids = self.data_package_type.entity_ref_type.map_to_entity_ids(
            list(entity_refs)
        )(cursor)

        return list(zip(entity_ids, timestamps, value_rows))

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
        column_names = chain(
            schema.system_columns,
            [
                trend_descriptor.name
                for trend_descriptor in self.trend_descriptors
            ]
        )

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
                timestamp.isoformat(),
                modified.isoformat(),
                "\t".join(map_values(values))
            )
            for entity_id, timestamp, values in self.rows
        )


def package_group(key, packages):
    data_package_type, _entitytype_name, granularity = key

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

    return DataPackage(data_package_type, granularity, field_names, rows)


def parse_values(parsers):
    return zip_apply(parsers)
