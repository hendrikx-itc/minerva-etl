# -*- coding: utf-8 -*-
from datetime import datetime
from io import StringIO
from typing import Callable, List, Generator, Tuple, Any, Optional, Dict

from itertools import chain
from operator import itemgetter
from functools import total_ordering

from minerva.db.util import quote_ident
from minerva.directory.entityref import EntityRef
from minerva.storage.trend import schema
from minerva.storage.trend.trend import Trend
from minerva.storage.trend.granularity import Granularity
from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.util import grouped_by, zip_apply
from minerva.util.tabulate import render_table


@total_ordering
class DataPackageType:
    get_entity_type_name: Callable[['DataPackage'], str]
    entity_ref_type: EntityRef

    def __init__(self, identifier, entity_ref_type: EntityRef, get_entity_type_name: Callable[['DataPackage'], str]):
        self.identifier = identifier
        self.entity_ref_type = entity_ref_type
        self.get_entity_type_name = get_entity_type_name

    def __eq__(self, other):
        return self.identifier == other.identifier

    def __lt__(self, other):
        return self.identifier < other.identifier


DataPackageRow = Tuple[int, datetime, List[Any]]


class DataPackage:
    """
    A DataPackage represents a batch of trend records for the same EntityType
    granularity and timestamp. The EntityType is implicitly determined by the
    entities in the data package, and they must all be of the same EntityType.
    """
    data_package_type: DataPackageType
    granularity: Granularity
    trend_descriptors: List[Trend.Descriptor]

    def __init__(
            self, data_package_type: DataPackageType, granularity: Granularity,
            trend_descriptors: List[Trend.Descriptor], rows):
        self.data_package_type = data_package_type
        self.granularity = granularity
        self.trend_descriptors = trend_descriptors
        self.rows = rows

    @staticmethod
    def merge_packages(packages: List['DataPackage']) -> List['DataPackage']:
        return [
            package_group(key, list(group))
            for key, group in grouped_by(packages, DataPackage.get_key)
        ]

    def render_table(self) -> str:
        column_names = ["entity", "timestamp"] + list(
            trend_descriptor.name
            for trend_descriptor in self.trend_descriptors
        )
        column_align = [">"] * len(column_names)
        column_sizes = ["max"] * len(column_names)

        rows = [row[:-1] + tuple(row[-1]) for row in self.rows]
        table = render_table(column_names, column_align, column_sizes, rows)

        return '\n'.join(table)

    def entity_type_name(self) -> str:
        return self.data_package_type.get_entity_type_name(self)

    def is_empty(self) -> bool:
        """Return True if the package has no data rows."""
        return len(self.rows) == 0

    def filter_trends(self, fn: Callable[[str], bool]) -> 'DataPackage':
        """
        :param fn: Filter function for trend names
        :return: A new data package with just the trend data for the trends
        filtered by provided function
        """
        value_getters, filtered_trend_descriptors = zip(*[
            (itemgetter(index), trend_descriptor)
            for index, trend_descriptor in enumerate(self.trend_descriptors)
            if fn(trend_descriptor.name)
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

    def split(self, group_fn: Callable[[str], Optional[str]]) -> Generator[Tuple[str, "DataPackage"], None, None]:
        """
        Split the trends in this package by passing the trend name through the
        provided function. The trends with the same resulting key are placed in
        a new separate package.

        :param group_fn: Function that returns the group key for a trend name
        :return: A list of data packages with trends grouped by key
        """
        value_getters = (
            (group_fn(trend_descriptor.name), itemgetter(index), trend_descriptor)
            for index, trend_descriptor in enumerate(self.trend_descriptors)
        )

        grouped_value_getters = grouped_by(
            (g for g in value_getters if g[0] is not None),
            key=itemgetter(0)
        )

        for key, group in grouped_value_getters:
            keys, value_getters, trend_descriptors = zip(*list(group))

            yield (
                key,
                DataPackage(
                    self.data_package_type,
                    self.granularity,
                    trend_descriptors,
                    [
                        (entity_ref, timestamp, tuple(g(values) for g in value_getters))
                        for entity_ref, timestamp, values in self.rows
                    ]
                )
            )

    def get_key(self) -> Tuple[DataPackageType, str, Granularity]:
        return (
            self.data_package_type,
            self.entity_type_name(), self.granularity
        )

    def refined_rows(self, cursor) -> List[DataPackageRow]:
        """
        Map the entity reference to an entity ID in each row and return the
        newly formed rows with IDs.
        """
        entity_refs, timestamps, value_rows = zip(*self.rows)

        entity_ids = self.data_package_type.entity_ref_type.map_to_entity_ids(
            list(entity_refs)
        )(cursor)

        return list(zip(entity_ids, timestamps, value_rows))

    def copy_from(self, table, value_descriptors, modified) -> Callable:
        """
        Return a function that can execute a COPY FROM query on a cursor.
        """
        def fn(cursor):
            cursor.copy_expert(
                self._create_copy_from_query(table),
                self._create_copy_from_file(value_descriptors, modified)
            )

        return fn

    def timestamps(self) -> List[datetime]:
        return list(set(row[1] for row in self.rows))

    def _create_copy_from_query(self, table) -> str:
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

    def _create_copy_from_file(self, value_descriptors: List[ValueDescriptor], modified: datetime) -> StringIO:
        copy_from_file = StringIO()

        copy_from_file.writelines(
            self._create_copy_from_lines(value_descriptors, modified)
        )

        copy_from_file.seek(0)

        return copy_from_file

    def _create_copy_from_lines(
            self, value_descriptors: List[ValueDescriptor], modified: datetime
    ) -> Generator[str, None, None]:
        value_mappers = [
            value_descriptor.data_type.string_serializer()
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


def package_group(key: Tuple[DataPackageType, str, Granularity], packages: List[DataPackage]) -> DataPackage:
    data_package_type, _entity_type_name, granularity = key

    all_trend_descriptors = {}

    # Combine all values from all packages using dictionaries
    dict_rows_by_entity_ref: Dict[Tuple[EntityRef, datetime], dict] = {}

    for p in packages:
        for entity_ref, timestamp, values in p.rows:
            value_dict = dict(zip(
                (td.name for td in p.trend_descriptors), values
            ))

            dict_rows_by_entity_ref.setdefault(
                (entity_ref, timestamp), {}
            ).update(value_dict)

        all_trend_descriptors.update(
            {td.name: td for td in p.trend_descriptors}
        )

    # Use the encountered field names to construct new rows with a value for
    # each field
    trend_descriptors = list(all_trend_descriptors.values())

    rows = []

    for (entity_ref, timestamp), value_dict in dict_rows_by_entity_ref.items():
        values = [value_dict.get(td.name) for td in trend_descriptors]

        row = entity_ref, timestamp, values

        rows.append(row)

    return DataPackage(data_package_type, granularity, trend_descriptors, rows)


def parse_values(parsers):
    return zip_apply(parsers)
