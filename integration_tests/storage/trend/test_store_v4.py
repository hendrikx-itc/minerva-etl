# -*- coding: utf-8 -*-
from contextlib import closing
import datetime

from nose.tools import eq_

from minerva.db.query import Column, Eq, And
from minerva.directory.helpers_v4 import name_to_entitytype, name_to_datasource
from minerva.test import with_conn, with_dataset
from minerva.storage.trend.test import DataSet
from minerva.storage.trend.datapackage import DataPackage
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.partitioning import Partitioning
from minerva.storage.trend.granularity import create_granularity

from minerva_db import clear_database

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2017 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class TestData(DataSet):

    def __init__(self):
        self.granularity = create_granularity("900")

    def load(self, cursor):
        self.datasource = name_to_datasource(cursor, "test-source")
        self.entitytype = name_to_entitytype(cursor, "test_type")


@with_conn(clear_database)
@with_dataset(TestData)
def test_create_trendstore(conn, dataset):
    partition_size = 3600

    trendstore = TrendStore(
        dataset.datasource, dataset.entitytype, dataset.granularity,
        partition_size, "table")

    with closing(conn.cursor()) as cursor:
        trendstore.create(cursor)

    assert isinstance(trendstore, TrendStore)

    assert trendstore.id is not None


@with_conn(clear_database)
@with_dataset(TestData)
def test_create_trendstore_with_children(conn, dataset):
    partition_size = 3600

    trendstore = TrendStore(
        dataset.datasource, dataset.entitytype, dataset.granularity,
        partition_size, "table")

    with closing(conn.cursor()) as cursor:
        trendstore.create(cursor)

        assert trendstore.id is not None

        timestamp = dataset.datasource.tzinfo.localize(
            datetime.datetime(2013, 5, 6, 14, 45))

        partition = trendstore.partition(timestamp)

        partition.create(cursor)


@with_conn(clear_database)
@with_dataset(TestData)
def test_get_trendstore(conn, dataset):
    partition_size = 3600

    with closing(conn.cursor()) as cursor:
        TrendStore(
            dataset.datasource, dataset.entitytype, dataset.granularity,
            partition_size, "table").create(cursor)

        trendstore = TrendStore.get(
            cursor, dataset.datasource,
            dataset.entitytype,
            dataset.granularity)

        eq_(trendstore.datasource.id, dataset.datasource.id)
        eq_(trendstore.partition_size, partition_size)
        assert trendstore.id is not None, "trendstore.id is None"
        eq_(trendstore.version, 4)


@with_conn(clear_database)
@with_dataset(TestData)
def test_store_copy_from(conn, dataset):
    partition_size = 86400

    with closing(conn.cursor()) as cursor:
        trendstore = TrendStore(
            dataset.datasource, dataset.entitytype, dataset.granularity,
            partition_size, "table").create(cursor)

    conn.commit()

    timestamp = dataset.datasource.tzinfo.localize(
        datetime.datetime(2013, 4, 25, 9, 45))

    trends = ["a", "b", "c"]

    def make_row(index):
        return (1234 + index, [1, 2, 3 + index])

    rows = map(make_row, range(100))

    datapackage = DataPackage(dataset.granularity, timestamp, trends, rows)

    transaction = trendstore.store(datapackage)
    transaction.run(conn)

    transaction = trendstore.store(datapackage)
    transaction.run(conn)


@with_conn(clear_database)
@with_dataset(TestData)
def test_store_copy_from_missing_column(conn, dataset):
    partition_size = 86400
    trendstore = TrendStore(
        dataset.datasource, dataset.entitytype, dataset.granularity,
        partition_size, "table")

    with closing(conn.cursor()) as cursor:
        trendstore.create(cursor)

    conn.commit()

    timestamp = dataset.datasource.tzinfo.localize(
        datetime.datetime(2013, 4, 25, 9, 45))

    trends = ["a", "b", "c"]

    def make_row_x(index):
        return (1234 + index, [1, 2, 3 + index])

    rows = map(make_row_x, range(100))

    datapackage = DataPackage(dataset.granularity, timestamp, trends, rows)

    transaction = trendstore.store(datapackage)
    transaction.run(conn)

    # Store second part with one column extra

    timestamp = dataset.datasource.tzinfo.localize(
        datetime.datetime(2013, 4, 25, 10, 00))

    trends = ["a", "b", "c", "d"]

    def make_row_y(index):
        return (1234 + index, [1, 2, 3, 4 + index])

    rows = map(make_row_y, range(100))

    datapackage = DataPackage(dataset.granularity, timestamp, trends, rows)

    transaction = trendstore.store(datapackage)
    transaction.run(conn)


@with_conn(clear_database)
@with_dataset(TestData)
def test_store(conn, dataset):
    partition_size = 86400
    trendstore = TrendStore(
        dataset.datasource, dataset.entitytype, dataset.granularity,
        partition_size, "table")

    with closing(conn.cursor()) as cursor:
        trendstore.create(cursor)

    conn.commit()

    timestamp = dataset.datasource.tzinfo.localize(
        datetime.datetime(2013, 4, 25, 9, 45))

    trends = ["a", "b", "c"]

    rows = [
        (1234, [1, 2, 3]),
        (2345, [4, 5, 6])]

    datapackage = DataPackage(dataset.granularity, timestamp, trends, rows)

    transaction = trendstore.store(datapackage)
    transaction.run(conn)

    table = trendstore.partition(timestamp).table()

    condition = And(
        Eq(Column("entity_id"), 2345),
        Eq(Column("timestamp"), timestamp))

    query = table.select(Column("c")).where_(condition)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        c, = cursor.fetchone()

    eq_(c, 6)

    rows = [
        (1234, [1, 2, 3]),
        (2345, [4, 5, 7])]

    datapackage = DataPackage(dataset.granularity, timestamp, trends, rows)

    transaction = trendstore.store(datapackage)
    transaction.run(conn)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        c, = cursor.fetchone()

    eq_(c, 7)


@with_conn(clear_database)
@with_dataset(TestData)
def test_generate_index(conn, dataset):
    partition_size = 86400

    start = dataset.datasource.tzinfo.localize(
        datetime.datetime(2013, 4, 25, 9, 45))
    end = dataset.datasource.tzinfo.localize(
        datetime.datetime(2013, 4, 27, 9, 45))

    partitioning = Partitioning(partition_size)

    for timestamp in dataset.granularity.range(start, end):
        partition_index = partitioning.index(timestamp)

        args = partition_size, timestamp

        with closing(conn.cursor()) as cursor:
            cursor.callproc("trend.timestamp_to_index", args)

            postgresql_partition_index, = cursor.fetchone()

        eq_(postgresql_partition_index, partition_index)


@with_conn(clear_database)
@with_dataset(TestData)
def test_store_add_column(conn, dataset):
    partition_size = 86400
    trendstore = TrendStore(
        dataset.datasource, dataset.entitytype, dataset.granularity,
        partition_size, "table")

    with closing(conn.cursor()) as cursor:
        trendstore.create(cursor)

    conn.commit()

    timestamp = dataset.datasource.tzinfo.localize(
        datetime.datetime(2013, 4, 25, 10, 45))

    trends = ["a", "b", "c"]

    rows = [
        (1234, [1, 2, 3]),
        (2345, [4, 5, 6])]

    datapackage = DataPackage(dataset.granularity, timestamp, trends, rows)

    transaction = trendstore.store(datapackage)
    transaction.run(conn)

    table = trendstore.partition(timestamp).table()

    condition = And(
        Eq(Column("entity_id"), 2345),
        Eq(Column("timestamp"), timestamp))

    query = table.select(Column("c")).where_(condition)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        c, = cursor.fetchone()

    eq_(c, 6)

    trends = ["a", "b", "c", "d"]

    rows = [
        (2345, [4, 5, 7, "2013-04-25 11:00:00"])]

    datapackage = DataPackage(dataset.granularity, timestamp, trends, rows)

    transaction = trendstore.store(datapackage)
    transaction.run(conn)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        c, = cursor.fetchone()

    eq_(c, 7)


@with_conn(clear_database)
@with_dataset(TestData)
def test_store_alter_column(conn, dataset):
    partition_size = 86400
    trendstore = TrendStore(
        dataset.datasource, dataset.entitytype, dataset.granularity,
        partition_size, "table")

    with closing(conn.cursor()) as cursor:
        trendstore.create(cursor)

    conn.commit()

    timestamp = dataset.datasource.tzinfo.localize(
        datetime.datetime(2013, 4, 25, 11, 00))

    trends = ["a", "b", "c"]

    rows = [
        (1234, [1, 2, 3]),
        (2345, [4, 5, 6])]

    datapackage = DataPackage(dataset.granularity, timestamp, trends, rows)

    transaction = trendstore.store(datapackage)
    transaction.run(conn)

    table = trendstore.partition(timestamp).table()

    condition = And(
        Eq(Column("entity_id"), 2345),
        Eq(Column("timestamp"), timestamp))

    query = table.select(Column("c")).where_(condition)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        c, = cursor.fetchone()

    eq_(c, 6)

    trends = ["a", "b", "c"]

    rows = [
        (2345, [4, 5, "2013-04-25 11:00:00"])]

    datapackage = DataPackage(dataset.granularity, timestamp, trends, rows)

    transaction = trendstore.store(datapackage)
    transaction.run(conn)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        c, = cursor.fetchone()

    eq_(c, datetime.datetime(2013, 4, 25, 11, 0, 0))
