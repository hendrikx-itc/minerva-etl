from functools import partial
from contextlib import closing
import logging

from nose.tools import eq_

from minerva.util import head, unlines
from minerva.db.query import Column
from minerva.db.util import render_result
from datetime import datetime, timedelta
from minerva.directory.helpers_v4 import \
    name_to_datasource, name_to_entitytype
from minerva.storage.trend.storage import retrieve, retrieve_orderedby_time, \
    retrieve_aggregated
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.trendstore import TrendStore

from minerva_db import connect, with_data

from helpers import row_count, render_source
from data import TestData

SCHEMA = "trend"


class TestRetrieve(with_data(TestData)):

    def test_retrieve(self):
        table_names = [self.data.partition_a.table().name]
        start = self.data.timestamp_1
        end = self.data.timestamp_1
        entity = self.data.entities[1]
        entities = [entity.id]

        column_names = [
            "CellID",
            "CCR",
            "CCRatts",
            "Drops"]

        create_column = partial(Column, self.data.partition_a.table())

        columns = map(create_column, column_names)

        r = retrieve(
            self.conn, SCHEMA, table_names, columns, entities, start, end)

        eq_(len(r), 1)

        first_result = head(r)

        entity_id, timestamp, c1, c2, c3, c4 = first_result

        eq_(entity_id, entity.id)
        eq_(c4, 18)

    def test_retrieve_from_v4_trendstore(self):
        table_names = [self.data.partition_a.table().name]
        start = self.data.timestamp_1
        end = self.data.timestamp_1
        entity = self.data.entities[1]
        entities = [entity.id]

        column_names = [
            "CellID",
            "CCR",
            "CCRatts",
            "Drops"]

        create_column = partial(Column, self.data.partition_a.table())

        columns = map(create_column, column_names)

        r = retrieve(
            self.conn, SCHEMA, table_names, columns, entities, start, end)

        eq_(len(r), 1)

        first_result = head(r)

        entity_id, timestamp, c1, c2, c3, c4 = first_result

        eq_(entity_id, entity.id)
        eq_(c4, 18)

    def test_retrieve_ordered_by_time(self):
        table_a = self.data.partition_a.table()

        with closing(self.conn.cursor()) as cursor:
            eq_(row_count(cursor, table_a), 3)

        table_names = [table_a.name]
        start = self.data.timestamp_1
        end = self.data.timestamp_1
        entity = self.data.entities[1]
        entities = [entity.id]

        columns = [
            Column(table_a, "CellID"),
            Column(table_a, "CCR"),
            Column(table_a, "CCRatts"),
            Column(table_a, "Drops")]

        r = retrieve_orderedby_time(
            self.conn, SCHEMA, table_names, columns, entities, start, end)

        eq_(len(r), 1)

        first_result = head(r)

        entity_id, timestamp, c1, c2, c3, c4 = first_result

        eq_(entity_id, entity.id)
        eq_(c4, 18)

    def test_retrieve_multi_table_time(self):
        table_d_1 = self.data.partition_d_1.table()
        table_d_2 = self.data.partition_d_2.table()
        table_names = [table_d_1.name, table_d_2.name]
        start = self.data.timestamp_1 - timedelta(seconds=60)
        end = self.data.timestamp_2
        entity = self.data.entities[1]
        entities = [entity.id]

        column_names = [
            "counter_x"]

        columns = map(Column, column_names)

        r = retrieve(
            self.conn, SCHEMA, table_names, columns, entities, start, end)

        eq_(len(r), 2)

        first_result = head(r)

        entity_id, timestamp, c1 = first_result

        eq_(entity_id, entity.id)
        eq_(c1, 110)


class TestRetrieveAggregate(object):

    def __init__(self):
        self.conn = None

    def setup(self):
        self.conn = connect()

    def teardown(self):
        self.conn.close()

    def test_retrieve_aggregate(self):
        granularity = create_granularity("900")

        with closing(self.conn.cursor()) as cursor:
            datasource = name_to_datasource(cursor, "test")
            entitytype = name_to_entitytype(cursor, "Cell")

            TrendStore(
                datasource, entitytype,
                    granularity, 86400, "table").create(cursor)

        column_expressions = ["COUNT(entity_id)"]

        start = datasource.tzinfo.localize(datetime(2012, 12, 6, 14, 15))
        end = datasource.tzinfo.localize(datetime(2012, 12, 6, 14, 15))

        interval = start, end

        retrieve_aggregated(
            self.conn, datasource, granularity, entitytype,
                column_expressions, interval, group_by="entity_id")


class TestRetrieveMultiSource(with_data(TestData)):

    def test_retrieve(self):
        table_a = self.data.partition_a.table()
        table_b = self.data.partition_b.table()
        table_c = self.data.partition_c.table()

        table_names = [
            table_a.name,
            table_b.name,
            table_c.name]
        start = self.data.timestamp_1
        end = self.data.timestamp_1

        table_a_cols = [
            Column("CellID"),
            Column("CCR"),
            Column("CCRatts"),
            Column("Drops")]

        table_b_cols = [
            Column("counter_a"),
            Column("counter_b")]

        table_c_cols = [
            Column("counter_x"),
            Column("counter_y")]

        system_columns = [Column("entity_id"), Column("timestamp")]

        columns = table_a_cols + table_b_cols + table_c_cols

        with closing(self.conn.cursor()) as cursor:
            table_a.select(system_columns + table_a_cols).execute(cursor)
            logging.debug(unlines(render_result(cursor)))

            table_b.select(system_columns + table_b_cols).execute(cursor)
            logging.debug(unlines(render_result(cursor)))

            table_c.select(system_columns + table_c_cols).execute(cursor)
            logging.debug(unlines(render_result(cursor)))

        r = retrieve(
            self.conn, SCHEMA, table_names, columns, None, start, end,
                entitytype=self.data.entitytype)

        data = [["entity_id", "timestamp"] + [c.name for c in columns]] + r

        logging.debug(unlines(render_source(data)))

        eq_(len(r), 5)
