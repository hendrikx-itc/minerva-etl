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

from pytz import timezone

from minerva.storage.generic import extract_data_types
from minerva.directory.helpers_v4 import name_to_datasource, \
    name_to_entitytype, dn_to_entity
from minerva.storage.trend.test import DataSet
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.datapackage import DataPackage
from minerva.storage.trend.trendstore import TrendStore, store_copy_from


class TestSetQtr(DataSet):
    def __init__(self):
        self.timezone = timezone("Europe/Amsterdam")
        self.timestamp = self.timezone.localize(datetime(2012, 12, 6, 14, 15))
        self.modified = self.timezone.localize(datetime(2012, 12, 6, 14, 36, 4))
        self.trendstore = None
        self.granularity = create_granularity("900")
        self.entitytype_name = "dummy_type"
        self.dns = ["{}=node_{}".format(self.entitytype_name, i)
                for i in range(63020, 63025)]


class TestSet1Small(TestSetQtr):
    def load(self, cursor):
        self.datasource = name_to_datasource(cursor, "testset1")

        self.entitytype = name_to_entitytype(cursor, self.entitytype_name)

        self.entities = map(partial(dn_to_entity, cursor), self.dns)

        datapackage = generate_datapackage_a(self.granularity,
                self.timestamp, self.entities)

        self.trendstore = TrendStore.get(cursor, self.datasource, self.entitytype,
                self.granularity)

        if not self.trendstore:
            self.trendstore = TrendStore(self.datasource, self.entitytype,
                    self.granularity, partition_size=86400, type="table").create(cursor)

        self.partition = store_datapackage(cursor, self.trendstore,
                datapackage, self.modified)


class TestSet1Large(TestSetQtr):
    def load(self, cursor):
        self.datasource = name_to_datasource(cursor, "testset1")

        self.entitytype = name_to_entitytype(cursor, self.entitytype_name)

        self.entities = map(partial(dn_to_entity, cursor), self.dns)

        datapackage = generate_datapackage_a(self.granularity,
                self.timestamp, self.entities)

        self.trendstore = TrendStore.get(cursor, self.datasource, self.entitytype,
                self.granularity)

        if not self.trendstore:
            self.trendstore = TrendStore(self.datasource, self.entitytype,
                    self.granularity, partition_size=86400, type="table").create(cursor)

        self.partition = store_datapackage(cursor, self.trendstore,
                datapackage, self.modified)


class TestData(object):
    def __init__(self):
        self.entitytype_name = "dummy_type"
        self.dns = ["{}=dummy_{}".format(self.entitytype_name, i)
                for i in range(1020, 1025)]

        self.timezone = timezone("Europe/Amsterdam")

        self.timestamp_1 = self.timezone.localize(datetime(2012, 12, 6, 14, 15))
        self.timestamp_2 = self.timezone.localize(datetime(2012, 12, 7, 14, 15))

        self.modified = self.timezone.localize(datetime(2012, 12, 6, 14, 15))

    def load(self, cursor):
        self.entitytype = name_to_entitytype(cursor, self.entitytype_name)

        self.entities = map(partial(dn_to_entity, cursor), self.dns)

        granularity = create_granularity("900")

        # Data a

        self.datasource_a = name_to_datasource(cursor, "test-source-a")
        self.trendstore_a = TrendStore(self.datasource_a, self.entitytype,
                granularity, partition_size=86400, type="table").create(cursor)
        datapackage = generate_datapackage_a(granularity, self.timestamp_1,
                self.entities)
        self.partition_a = store_datapackage(cursor, self.trendstore_a,
                datapackage, self.modified)

        # Data b

        self.datasource_b = name_to_datasource(cursor, "test-source-b")
        self.trendstore_b = TrendStore(self.datasource_b, self.entitytype,
                granularity, partition_size=86400, type="table").create(cursor)
        datapackage = generate_datapackage_b(granularity, self.timestamp_1,
                self.entities)
        self.partition_b = store_datapackage(cursor, self.trendstore_b,
                datapackage, self.modified)

        # Data c

        self.datasource_c = name_to_datasource(cursor, "test-source-c")
        self.trendstore_c = TrendStore(self.datasource_c, self.entitytype,
                granularity, partition_size=86400, type="table").create(cursor)
        datapackage = generate_datapackage_c(granularity, self.timestamp_1,
                self.entities)
        self.partition_c = store_datapackage(cursor, self.trendstore_c,
                datapackage, self.modified)

        # Data d

        self.datasource_d = name_to_datasource(cursor, "test-source-d")
        self.trendstore_d = TrendStore(self.datasource_d, self.entitytype,
                granularity, partition_size=86400, type="table").create(cursor)
        datapackage_1 = generate_datapackage_d(granularity, self.timestamp_1,
                self.entities)
        self.partition_d_1 = store_datapackage(cursor, self.trendstore_d,
                datapackage_1, self.modified)

        datapackage_2 = generate_datapackage_d(granularity, self.timestamp_2,
                self.entities)
        self.partition_d_2 = store_datapackage(cursor, self.trendstore_d,
                datapackage_2, self.modified)


def store_datapackage(cursor, trendstore, datapackage, modified):
    data_types = extract_data_types(datapackage.rows)

    partition = trendstore.partition(datapackage.timestamp)
    partition.create(cursor)
    partition.check_columns_exist(datapackage.trend_names, data_types)(cursor)

    store_copy_from(cursor, partition.table(), datapackage, modified)

    return partition


def generate_datapackage_a(granularity, timestamp, entities):
    return DataPackage(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=['CellID', 'CCR', 'CCRatts', 'Drops'],
        rows=[
            (entities[0].id, ('10023', '0.9919', '2105', '17')),
            (entities[1].id, ('10047', '0.9963', '4906', '18')),
            (entities[2].id, ('10048', '0.9935', '2448', '16'))
        ])


def generate_datapackage_a_large(granularity, timestamp, entities):
    return DataPackage(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=['CellID', 'CCR', 'CCRatts', 'Drops'],
        rows=[
            (entities[0].id, ('10023', '0.9919', '210500', '17')),
            (entities[1].id, ('10047', '0.9963', '490600', '18')),
            (entities[2].id, ('10048', '0.9935', '244800', '16'))
        ])


def generate_datapackage_b(granularity, timestamp, entities):
    return DataPackage(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=['counter_a', 'counter_b'],
        rows=[
            (entities[0].id, ('443', '21')),
            (entities[1].id, ('124', '34')),
            (entities[2].id, ('783', '15')),
            (entities[3].id, ('309', '11'))
        ])


def generate_datapackage_c(granularity, timestamp, entities):
    return DataPackage(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=['counter_x', 'counter_y'],
        rows=[
            (entities[1].id, ('110', '0')),
            (entities[2].id, ('124', '0')),
            (entities[3].id, ('121', '2'))
        ])


def generate_datapackage_d(granularity, timestamp, entities):
    return DataPackage(
        granularity=granularity,
        timestamp=timestamp,
        trend_names=['counter_x', 'counter_y'],
        rows=[
            (entities[1].id, ('110', '3'))
        ])
