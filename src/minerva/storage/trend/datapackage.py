# -*- coding: utf-8 -*-
from io import StringIO
from itertools import chain
from operator import itemgetter

from minerva.db.util import quote_ident
from minerva.storage.trend import schema
from minerva.util import grouped_by, zip_apply, identity, k
from minerva.util.tabulate import render_table
from minerva.directory.entityref import EntityDnRef, EntityIdRef
from minerva.directory.distinguishedname import entity_type_name_from_dn


class DataPackage:
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

    def render_table(self):
        lines = [
            str(self.timestamp),
            str(self.granularity),
            ','.join(self.trend_names),
        ]

        lines.extend([','.join(values) for entity_ref, values in self.rows])

        column_names = ["entity"] + list(self.trend_names)
        column_align = ">" * len(column_names)
        column_sizes = ["max"] * len(column_names)

        rows = [row[:-1] + tuple(row[-1]) for row in self.rows]
        table = render_table(column_names, column_align, column_sizes, rows)

        return '\n'.join(table)

    @classmethod
    def entity_ref_type(cls):
        raise NotImplementedError()

    def entity_type_name(self):
        raise NotImplementedError()

    def is_empty(self):
        """Return True if the package has no data rows."""
        return len(self.rows) == 0

    def filter_trends(self, fn):
        """
        :param fn: Filter function for trend names
        :return: A new data package with just the trend data for the trends
        filtered by provided function
        """
        value_getters, filtered_trend_names = zip(*[
            (itemgetter(index), trend_name)
            for index, trend_name in enumerate(self.trend_names)
            if fn(trend_name)
        ])

        return self.__class__(
            self.granularity,
            self.timestamp,
            filtered_trend_names,
            [
                (entity_ref, tuple(g(values) for g in value_getters))
                for entity_ref, values in self.rows
            ]
        )

    def get_key(self):
        return (
            self.__class__, self.timestamp,
            self.entity_type_name(), self.granularity
        )

    def refined_rows(self, cursor):
        """
        Map the entity reference to an entity ID in each row and return the
        newly formed rows with IDs.
        """
        entity_refs, value_rows = zip(*self.rows)

        entity_ids = self.entity_ref_type().map_to_entity_ids(
            list(entity_refs)
        )(cursor)

        return list(zip(entity_ids, value_rows))

    @staticmethod
    def merge_packages(packages):
        return [
            package_group(key, list(group))
            for key, group in grouped_by(packages, DataPackage.get_key)
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
    Define and return a new DataPackageBase sub-class.

    @type entity_ref_type: EntityRef
    @type entity_type_name: function(DataPackage) -> str
    @type refine_values: function(parsers) -> function(values)
    -> refined_values
    """
    return type(name, (DataPackage,), {
        'entity_ref_type': classmethod(lambda cls: entity_ref_type),
        'entity_type_name': entity_type_name,
        'refine_values': staticmethod(refine_values)
    })


def from_first_dn(data_package):
    dn = data_package.rows[0][0]

    return entity_type_name_from_dn(dn)


def parse_values(parsers):
    return zip_apply(parsers)


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
        k(identity)
    )
