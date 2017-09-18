# -*- coding: utf-8 -*-
from datetime import datetime
from functools import partial
from contextlib import closing

import pytz

from minerva.storage import datatype
from minerva.storage.trend.storage import store_copy_from

from minerva.directory import DataSource, EntityType, Entity
from minerva.storage.trend.test import DataSet
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.datapackage import \
    refined_package_type_for_entity_type
from minerva.storage.trend.trend import TrendDescriptor
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.tabletrendstore import TableTrendStore


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
    def __init__(self):
        self.partition = None

    def load(self, cursor):
        self.data_source = DataSource.from_name("testset1")(cursor)

        self.entity_type = EntityType.from_name(self.entity_type_name)(cursor)

        self.entities = map(partial(Entity.from_dn, cursor), self.dns)

        data_package = generate_data_package_a(
            self.granularity, self.timestamp, self.entities
        )

        self.trend_store = TrendStore.get(
            cursor, self.data_source, self.entity_type, self.granularity
        )

        if not self.trend_store:
            self.trend_store = TableTrendStore.create(
                TableTrendStore.Descriptor(
                    'test_store',
                    self.data_source, self.entity_type, self.granularity, [],
                    partition_size=86400
                )
            )(cursor)

        self.partition = store_data_package(
            cursor, self.trend_store, data_package, self.modified
        )


class TestSet1Large(TestSetQtr):
    def __init__(self):
        self.partition = None

    def load(self, cursor):
        self.data_source = DataSource.from_name(cursor, "testset1")

        self.entity_type = EntityType.from_name(cursor, self.entity_type_name)

        self.entities = map(partial(Entity.from_dn, cursor), self.dns)

        data_package = generate_data_package_a(
            self.granularity, self.timestamp, self.entities
        )

        self.trend_store = TrendStore.get(
            cursor, self.data_source, self.entity_type, self.granularity
        )

        if not self.trend_store:
            self.trend_store = TableTrendStore.create(
                TableTrendStore.Descriptor(
                    'test_store',
                    self.data_source, self.entity_type, self.granularity,
                    [], partition_size=86400
                )
            )(cursor)

        self.partition = store_data_package(
            cursor, self.trend_store, data_package, self.modified
        )


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
        self.partition_c = None
        self.data_source_d = None
        self.trend_store_d = None

    def load(self, conn):
        granularity = create_granularity("900")

        with closing(conn.cursor()) as cursor:
            self.entity_type = EntityType.from_name(self.entity_type_name)(
                cursor
            )

            self.entities = [Entity.from_dn(dn)(cursor) for dn in self.dns]

        # Data a

        with closing(conn.cursor()) as cursor:
            self.data_source_a = DataSource.from_name("test-source-a")(cursor)

            self.trend_store_a = TableTrendStore.create(
                TableTrendStore.Descriptor(
                    'test_store_a',
                    self.data_source_a, self.entity_type, granularity,
                    [], partition_size=86400
                )
            )(cursor)

        data_types, data_package = generate_data_package_a(
            granularity, self.timestamp_1, self.entities
        )

        self.partition_a = store_data_package(
            conn, self.trend_store_a, data_package, self.modified,
            data_types
        )

        # Data b

        with closing(conn.cursor()) as cursor:
            self.data_source_b = DataSource.from_name("test-source-b")(cursor)

            self.trend_store_b = TableTrendStore.create(
                TableTrendStore.Descriptor(
                    'test_store_b',
                    self.data_source_b, self.entity_type, granularity, [],
                    partition_size=86400
                )
            )(cursor)

        data_types, data_package = generate_data_package_b(
            granularity, self.timestamp_1, self.entities
        )

        self.partition_b = store_data_package(
            conn, self.trend_store_b, data_package, self.modified, data_types
        )

        # Data c

        with closing(conn.cursor()) as cursor:
            self.data_source_c = DataSource.from_name("test-source-c")(cursor)
            self.trend_store_c = TableTrendStore.create(
                TableTrendStore.Descriptor(
                    'test_store_c',
                    self.data_source_c, self.entity_type, granularity, [],
                    partition_size=86400
                )
            )(cursor)

        data_types, data_package = generate_data_package_c(
            granularity, self.timestamp_1, self.entities
        )

        self.partition_c = store_data_package(
            conn, self.trend_store_c, data_package, self.modified, data_types
        )

        # Data d

        with closing(conn.cursor()) as cursor:
            self.data_source_d = DataSource.from_name("test-source-d")(cursor)
            self.trend_store_d = TableTrendStore.create(
                TableTrendStore.Descriptor(
                    'test_store_d',
                    self.data_source_d, self.entity_type, granularity, [],
                    partition_size=86400
                )
            )(cursor)

        data_types, datapackage_1 = generate_data_package_d(
            granularity, self.timestamp_1, self.entities
        )

        self.partition_d_1 = store_data_package(
            conn, self.trend_store_d, datapackage_1, self.modified, data_types
        )

        data_types, datapackage_2 = generate_data_package_d(
            granularity, self.timestamp_2, self.entities
        )

        self.partition_d_2 = store_data_package(
            conn, self.trend_store_d, datapackage_2, self.modified, data_types
        )


def store_data_package(conn, trend_store, data_package, modified, data_types):
    partition = trend_store.partition(data_package.timestamp)

    with closing(conn.cursor()) as cursor:
        partition.create(cursor)

    trend_descriptors = [
        TrendDescriptor(name, data_type, '')
        for name, data_type in zip(data_package.trend_names, data_types)
    ]

    with closing(conn.cursor()) as cursor:
        trend_store.check_trends_exist(trend_descriptors)(cursor)

    store_copy_from(
        conn, partition.table().schema.name, partition.table().name,
        data_package.trend_names, data_package.timestamp, modified,
        data_package.rows
    )

    return partition


def generate_data_package_a(granularity, timestamp, entities):
    data_type_names = ['text', 'text', 'text', 'text']
    data_types = [
        datatype.registry[data_type_name] for data_type_name in data_type_names
    ]

    return data_types, refined_package_type_for_entity_type('TestType')(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=['CellID', 'CCR', 'CCRatts', 'Drops'],
        rows=[
            (entities[0].id, ('10023', '0.9919', '2105', '17')),
            (entities[1].id, ('10047', '0.9963', '4906', '18')),
            (entities[2].id, ('10048', '0.9935', '2448', '16'))
        ]
    )


def generate_data_package_a_large(granularity, timestamp, entities):
    data_type_names = ['text', 'text', 'text', 'text']
    data_types = [
        datatype.registry[data_type_name] for data_type_name in data_type_names
    ]

    return data_types, refined_package_type_for_entity_type('TestType')(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=['CellID', 'CCR', 'CCRatts', 'Drops'],
        rows=[
            (entities[0].id, ('10023', '0.9919', '210500', '17')),
            (entities[1].id, ('10047', '0.9963', '490600', '18')),
            (entities[2].id, ('10048', '0.9935', '244800', '16'))
        ]
    )


def generate_data_package_b(granularity, timestamp, entities):
    data_type_names = ['text', 'text', 'text', 'text']
    data_types = [
        datatype.registry[data_type_name] for data_type_name in data_type_names
    ]

    return data_types, refined_package_type_for_entity_type('TestType')(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=['counter_a', 'counter_b'],
        rows=[
            (entities[0].id, ('443', '21')),
            (entities[1].id, ('124', '34')),
            (entities[2].id, ('783', '15')),
            (entities[3].id, ('309', '11'))
        ]
    )


def generate_data_package_c(granularity, timestamp, entities):
    data_type_names = ['text', 'text', 'text', 'text']
    data_types = [
        datatype.registry[data_type_name] for data_type_name in data_type_names
    ]

    return data_types, refined_package_type_for_entity_type('TestType')(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=['counter_x', 'counter_y'],
        rows=[
            (entities[1].id, ('110', '0')),
            (entities[2].id, ('124', '0')),
            (entities[3].id, ('121', '2'))
        ]
    )


def generate_data_package_d(granularity, timestamp, entities):
    data_type_names = ['text', 'text', 'text', 'text']
    data_types = [
        datatype.registry[data_type_name] for data_type_name in data_type_names
    ]

    return data_types, refined_package_type_for_entity_type('TestType')(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=['counter_x', 'counter_y'],
        rows=[
            (entities[1].id, ('110', '3'))
        ]
    )
