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
from functools import partial

import pytz

from minerva.directory import DataSource, EntityType, Entity
from minerva.storage import datatype
from minerva.storage.trend.test import DataSet
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.datapackage import refined_package_type_for_entity_type
from minerva.storage.trend.trend import TrendDescriptor
from minerva.storage.trend.trendstore import TrendStore, TrendStoreDescriptor


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

        self.trend_store = TrendStore.get(
            self.data_source, self.entity_type, self.granularity
        )(cursor)

        if not self.trend_store:
            self.trend_store = TrendStore.create(TrendStoreDescriptor(
                self.data_source, self.entity_type, self.granularity,
                trend_descriptors, partition_size=86400
            ))(cursor)

        self.trend_store.partition(data_package.timestamp).create(cursor)
        self.trend_store.store_copy_from(data_package, self.modified)(cursor)


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
            self.trend_store = TrendStore.create(TrendStoreDescriptor(
                self.data_source, self.entity_type, self.granularity,
                [], partition_size=86400)
            )(cursor)

        self.trend_store.partition(data_package.timestamp).create(cursor)
        self.trend_store.store_copy_from(data_package, self.modified)(cursor)


class TestData():
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

        self.trend_store_a = TrendStore.create(TrendStoreDescriptor(
            self.data_source_a, self.entity_type, granularity,
            trend_descriptors, partition_size=86400
        ))(cursor)

        self.trend_store_a.store_copy_from(data_package, self.modified)(cursor)

        # Data b

        trend_descriptors, data_package = generate_data_package_b(
            granularity, self.timestamp_1, self.entities
        )

        self.data_source_b = DataSource.from_name("test-source-b")(cursor)
        self.trend_store_b = TrendStore.create(TrendStoreDescriptor(
            self.data_source_b, self.entity_type, granularity,
            trend_descriptors, partition_size=86400
        ))(cursor)

        self.trend_store_b.store_copy_from(data_package, self.modified)(cursor)

        # Data c

        self.data_source_c = DataSource.from_name("test-source-c")(cursor)
        self.trend_store_c = TrendStore.create(TrendStoreDescriptor(
            self.data_source_c, self.entity_type, granularity, [],
            partition_size=86400
        ))(cursor)
        data_package = generate_data_package_c(
            granularity, self.timestamp_1, self.entities
        )

        self.trend_store_c.store_copy_from(
            data_package, self.modified
        )(cursor)

        # Data d

        self.data_source_d = DataSource.from_name("test-source-d")(cursor)
        self.trend_store_d = TrendStore.create(TrendStoreDescriptor(
            self.data_source_d, self.entity_type, granularity, [],
            partition_size=86400
        ))(cursor)
        data_package_1 = generate_data_package_d(
            granularity, self.timestamp_1, self.entities
        )
        self.partition_d_1 = store_data_package(
            cursor, self.trend_store_d, data_package_1, self.modified
        )

        data_package_2 = generate_data_package_d(
            granularity, self.timestamp_2, self.entities
        )
        self.partition_d_2 = store_data_package(
            cursor, self.trend_store_d, data_package_2, self.modified
        )


def generate_data_package_a(granularity, timestamp, entities):
    trend_descriptors = [
        TrendDescriptor('CellID', datatype.DataTypeSmallInt, ''),
        TrendDescriptor('CCR', datatype.DataTypeDoublePrecision, ''),
        TrendDescriptor('CCRatts', datatype.DataTypeSmallInt, ''),
        TrendDescriptor('Drops', datatype.DataTypeSmallInt, '')
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
        TrendDescriptor('CellID', datatype.DataTypeSmallInt, ''),
        TrendDescriptor('CCR', datatype.DataTypeDoublePrecision, ''),
        TrendDescriptor('CCRatts', datatype.DataTypeSmallInt, ''),
        TrendDescriptor('Drops', datatype.DataTypeSmallInt, '')
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
        TrendDescriptor('counter_a', datatype.DataTypeSmallInt, ''),
        TrendDescriptor('counter_b', datatype.DataTypeSmallInt, '')
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
        TrendDescriptor('counter_x', datatype.DataTypeSmallInt, ''),
        TrendDescriptor('counter_y', datatype.DataTypeSmallInt, '')
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
        TrendDescriptor('counter_x', datatype.DataTypeSmallInt, ''),
        TrendDescriptor('counter_y', datatype.DataTypeSmallInt, '')
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