from functools import partial
from contextlib import closing
import logging
from datetime import datetime, timedelta

from nose.tools import eq_

from minerva.util import head, unlines
from minerva.db.query import Column, Call
from minerva.db.util import render_result
from minerva.storage.trend.storage_v4 import retrieve, retrieve_aggregated

from minerva_db import with_data
from helpers import render_source
from data import TestData


class TestRetrieve(with_data(TestData)):
    def test_retrieve(self):
        table_a = self.data.partition_a.table()

        start = self.data.timestamp_1
        end = self.data.timestamp_1
        entity = self.data.entities[1]
        entities = [entity.id]

        column_names = [
            "CellID",
            "CCR",
            "CCRatts",
            "Drops"]

        create_column = partial(Column, table_a)

        columns = map(create_column, column_names)

        with closing(self.conn.cursor()) as cursor:
            r = retrieve(cursor, [table_a], columns, entities, start, end)

        eq_(len(r), 1)

        first_result = head(r)

        entity_id, timestamp, c1, c2, c3, c4 = first_result

        eq_(entity_id, entity.id)
        eq_(c4, 18)

    def test_retrieve_multi_table_time(self):
        tables = [self.data.partition_d_1.table(), self.data.partition_d_2.table()]
        start = self.data.timestamp_1 - timedelta(seconds=60)
        end = self.data.timestamp_2
        entity = self.data.entities[1]
        entities = [entity.id]

        column_names = [
            "counter_x"]

        columns = map(Column, column_names)

        with closing(self.conn.cursor()) as cursor:
            r = retrieve(cursor, tables, columns, entities, start, end)

        eq_(len(r), 2)

        first_result = head(r)

        entity_id, timestamp, c1 = first_result

        eq_(entity_id, entity.id)
        eq_(c1, 110)


class TestRetrieveAggregate(with_data(TestData)):
    def test_retrieve_aggregate(self):
        column_expressions = [Call("sum", Column("Drops"))]

        start = self.data.datasource_a.tzinfo.localize(
                datetime(2012, 12, 6, 14, 15))
        end = self.data.datasource_a.tzinfo.localize(
                datetime(2012, 12, 6, 14, 15))

        interval = start, end

        with closing(self.conn.cursor()) as cursor:
            result = retrieve_aggregated(cursor, self.data.trendstore_a,
                    column_expressions, interval, group_by=["entity_id"])

        assert result is not None


class TestRetrieveMultiSource(with_data(TestData)):
    def test_retrieve(self):
        table_a = self.data.partition_a.table()
        table_b = self.data.partition_b.table()
        table_c = self.data.partition_c.table()

        tables = [table_a, table_b, table_c]
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

            r = retrieve(cursor, tables, columns, None, start, end,
                    entitytype=self.data.entitytype)

        data = [["entity_id", "timestamp"] + [c.name for c in columns]] + r

        logging.debug(unlines(render_source(data)))

        eq_(len(r), 5)
