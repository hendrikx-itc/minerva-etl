# -*- coding: utf-8 -*-
from datetime import datetime

import pytz

from minerva.directory import DataSource, EntityType, Entity
from minerva.storage import datatype
from minerva.storage.trend.datapackage import DataPackageType
from minerva.storage.trend.trendstorepart import TrendStorePart
from minerva.storage.trend.test import DataSet
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.trend import Trend
from minerva.storage.trend.trendstore import TrendStore
from minerva.directory.entityref import EntityIdRef, EntityRef, entity_name_ref_class
from minerva.util import k


def refined_package_type_for_entity_type(type_name: str) -> DataPackageType:
    def identifier():
        return None

    get_entity_type_name = k(type_name)

    return DataPackageType(
        identifier,
        EntityIdRef,
        get_entity_type_name
    )


def package_type_for_entity_type(type_name: str) -> DataPackageType:
    def identifier():
        return None

    get_entity_type_name = k(type_name)

    return DataPackageType(
        identifier,
        entity_name_ref_class(type_name),
        get_entity_type_name
    )


class TestSetQtr(DataSet):
    def __init__(self):
        self.data_source = None
        self.entity_type = None
        self.entities = None
        self.timestamp = pytz.utc.localize(datetime(2012, 12, 6, 14, 15))
        self.modified = pytz.utc.localize(datetime(2012, 12, 6, 14, 36, 4))
        self.trend_store = None
        self.granularity = create_granularity("900")
        self.entity_type_name = "dummy_type"
        self.dns = [
            "{}=node_{}".format(self.entity_type_name, i)
            for i in range(63020, 63025)
        ]


class TestSet1Small(TestSetQtr):
    def load(self, cursor):
        self.data_source = DataSource.from_name("testset1")(cursor)

        self.entity_type = EntityType.from_name(self.entity_type_name)(cursor)

        self.entities = [Entity.from_dn(dn)(cursor) for dn in self.dns]

        trend_descriptors, data_package = generate_data_package_a(
            self.granularity, self.timestamp, self.entities
        )

        table_trend_store_part_descr = TrendStorePart.Descriptor(
            'test-set-1-small', trend_descriptors
        )

        self.trend_store = TrendStore.get(
            self.data_source, self.entity_type, self.granularity
        )(cursor)

        if not self.trend_store:
            self.trend_store = TrendStore.create(
                TrendStore.Descriptor(
                    self.data_source, self.entity_type, self.granularity,
                    [table_trend_store_part_descr], 86400
                )
            )(cursor)

        self.trend_store.partition(
            'test-set-1-small', data_package.timestamp
        ).create(cursor)
        self.trend_store.part_by_name['test-set-1-small'].store_copy_from(data_package, self.modified)(cursor)


class TestSet1Large(TestSetQtr):
    def load(self, cursor):
        self.data_source = DataSource.from_name(cursor, "testset1")

        self.entity_type = EntityType.from_name(cursor, self.entity_type_name)

        self.entities = [Entity.from_dn(dn)(cursor) for dn in self.dns]

        data_package = generate_data_package_a(
            self.granularity, self.timestamp, self.entities
        )

        self.trend_store = TrendStore.get(
            cursor, self.data_source, self.entity_type, self.granularity
        )

        if not self.trend_store:
            self.trend_store = TrendStore.create(
                TrendStore.Descriptor(
                    self.data_source, self.entity_type, self.granularity,
                    [], partition_size=86400
                )
            )(cursor)

        self.trend_store.partition(data_package.timestamp).create(cursor)
        self.trend_store.store_copy_from(data_package, self.modified)(cursor)


class TestData:
    def __init__(self):
        self.entity_type_name = "dummy_type"
        self.dns = [
            "{}=dummy_{}".format(self.entity_type_name, i)
            for i in range(1020, 1025)
        ]

        self.timestamp_1 = pytz.utc.localize(datetime(2012, 12, 6, 14, 15))
        self.timestamp_2 = pytz.utc.localize(datetime(2012, 12, 7, 14, 15))

        self.modified = pytz.utc.localize(datetime(2012, 12, 6, 14, 15))
        self.entity_type = None
        self.entities = None
        self.data_source_a = None
        self.trend_store_a = None
        self.partition_a = None
        self.data_source_b = None
        self.trend_store_b = None
        self.partition_b = None
        self.data_source_c = None
        self.trend_store_c = None

    def load(self, cursor):
        self.entity_type = EntityType.from_name(self.entity_type_name)(cursor)

        self.entities = [Entity.from_dn(dn)(cursor) for dn in self.dns]

        granularity = create_granularity("900")

        # Data a

        trend_descriptors, data_package = generate_data_package_a(
            granularity, self.timestamp_1, self.entities
        )

        self.data_source_a = DataSource.from_name("test-source-a")(cursor)

        self.trend_store_a = TrendStore.create(
            TrendStore.Descriptor(
                'test-data-a',
                self.data_source_a, self.entity_type, granularity,
                trend_descriptors, partition_size=86400
            )
        )(cursor)

        self.trend_store_a.store_copy_from(data_package, self.modified)(cursor)

        # Data b

        trend_descriptors, data_package = generate_data_package_b(
            granularity, self.timestamp_1, self.entities
        )

        self.data_source_b = DataSource.from_name("test-source-b")(cursor)
        self.trend_store_b = TrendStore.create(
            TrendStore.Descriptor(
                'test-data-b',
                self.data_source_b, self.entity_type, granularity,
                trend_descriptors, partition_size=86400
            )
        )(cursor)

        self.trend_store_b.store_copy_from(data_package, self.modified)(cursor)

        # Data c

        self.data_source_c = DataSource.from_name("test-source-c")(cursor)
        self.trend_store_c = TrendStore.create(
            TrendStore.Descriptor(
                'test-data-c',
                self.data_source_c, self.entity_type, granularity, [],
                partition_size=86400
            )
        )(cursor)
        data_package = generate_data_package_c(
            granularity, self.timestamp_1, self.entities
        )

        self.trend_store_c.store_copy_from(
            data_package, self.modified
        )(cursor)

        # Data d

        self.data_source_d = DataSource.from_name("test-source-d")(cursor)
        self.trend_store_d = TrendStore.create(
            TrendStore.Descriptor(
                'test-data-d',
                self.data_source_d, self.entity_type, granularity, [],
                partition_size=86400
            )
        )(cursor)
        data_package_1 = generate_data_package_d(
            granularity, self.timestamp_1, self.entities
        )
        self.partition_d_1 = self.trend_store_d.store(data_package_1)(cursor)

        data_package_2 = generate_data_package_d(
            granularity, self.timestamp_2, self.entities
        )

        self.trend_store_d.store(data_package_2)(cursor)


def generate_data_package_a(granularity, timestamp, entities):
    trend_descriptors = [
        Trend.Descriptor('CellID', datatype.registry['smallint'], ''),
        Trend.Descriptor('CCR', datatype.registry['double precision'], ''),
        Trend.Descriptor('CCRatts', datatype.registry['smallint'], ''),
        Trend.Descriptor('Drops', datatype.registry['smallint'], '')
    ]

    trend_names = [t.name for t in trend_descriptors]

    data_package = refined_package_type_for_entity_type('dummy_type')(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=trend_names,
        rows=[
            (entities[0].id, (10023, 0.9919, 2105, 17)),
            (entities[1].id, (10047, 0.9963, 4906, 18)),
            (entities[2].id, (10048, 0.9935, 2448, 16))
        ]
    )

    return trend_descriptors, data_package


def generate_data_package_a_large(granularity, timestamp, entities):
    trend_descriptors = [
        Trend.Descriptor('CellID', datatype.registry['smallint'], ''),
        Trend.Descriptor('CCR', datatype.registry['double precision'], ''),
        Trend.Descriptor('CCRatts', datatype.registry['smallint'], ''),
        Trend.Descriptor('Drops', datatype.registry['smallint'], '')
    ]

    trend_names = [t.name for t in trend_descriptors]

    data_package = refined_package_type_for_entity_type('dummy_type')(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=trend_names,
        rows=[
            (entities[0].id, (10023, 0.9919, 210500, 17)),
            (entities[1].id, (10047, 0.9963, 490600, 18)),
            (entities[2].id, (10048, 0.9935, 244800, 16))
        ]
    )

    return trend_descriptors, data_package


def generate_data_package_b(granularity, timestamp, entities):
    trend_descriptors = [
        Trend.Descriptor('counter_a', datatype.registry['smallint'], ''),
        Trend.Descriptor('counter_b', datatype.registry['smallint'], '')
    ]

    trend_names = [t.name for t in trend_descriptors]

    data_package = refined_package_type_for_entity_type('dummy_type')(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=trend_names,
        rows=[
            (entities[0].id, ('443', '21')),
            (entities[1].id, ('124', '34')),
            (entities[2].id, ('783', '15')),
            (entities[3].id, ('309', '11'))
        ]
    )

    return trend_descriptors, data_package


def generate_data_package_c(granularity, timestamp, entities):
    trend_descriptors = [
        Trend.Descriptor('counter_x', datatype.registry['smallint'], ''),
        Trend.Descriptor('counter_y', datatype.registry['smallint'], '')
    ]

    trend_names = [t.name for t in trend_descriptors]

    data_package = refined_package_type_for_entity_type('dummy_type')(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=trend_names,
        rows=[
            (entities[1].id, (110, 0)),
            (entities[2].id, (124, 0)),
            (entities[3].id, (121, 2))
        ]
    )

    return trend_descriptors, data_package


def generate_data_package_d(granularity, timestamp, entities):
    trend_descriptors = [
        Trend.Descriptor('counter_x', datatype.registry['smallint'], ''),
        Trend.Descriptor('counter_y', datatype.registry['smallint'], '')
    ]

    trend_names = [t.name for t in trend_descriptors]

    data_package = refined_package_type_for_entity_type('dummy_type')(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=trend_names,
        rows=[
            (entities[1].id, (110, 3))
        ]
    )

    return trend_descriptors, data_package
