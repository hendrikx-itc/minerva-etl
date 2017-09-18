from functools import partial
from contextlib import closing
import logging
from datetime import datetime, timedelta
import unittest

import pytz

from minerva.util import head, unlines
from minerva.db.util import render_result
from minerva.directory.datasource import DataSource
from minerva.directory.entitytype import EntityType
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.trendstore import TrendStore

from minerva.db.query import Column, Call
from minerva.storage.trend.storage_v4 import retrieve
from minerva.test import connect

from .minerva_db import with_data_context
from .data import TestData
from .helpers import row_count, render_source


SCHEMA = "trend"


class TestRetrieve(unittest.TestCase):
    def setUp(self):
        self.conn = connect()

    def tearDown(self):
        self.conn.close()

    def test_retrieve(self):
        with with_data_context(self.conn, TestData) as data:
            table_names = [data.partition_a.table().name]
            start = data.timestamp_1
            end = data.timestamp_1
            entity = data.entities[1]
            entities = [entity.id]

            column_names = [
                "CellID",
                "CCR",
                "CCRatts",
                "Drops"
            ]

            create_column = partial(Column, data.partition_a.table())

            columns = map(create_column, column_names)

            r = retrieve(
                self.conn, SCHEMA, table_names, columns, entities, start, end
            )

            self.assertEqual(len(r), 1)

            first_result = head(r)

            entity_id, timestamp, c1, c2, c3, c4 = first_result

            self.assertEqual(entity_id, entity.id)
            self.assertEqual(c4, 18)

    def test_retrieve_from_v4_trendstore(self):
        with with_data_context(self.conn, TestData) as data:
            table_names = [data.partition_a.table().name]
            start = data.timestamp_1
            end = data.timestamp_1
            entity = data.entities[1]
            entities = [entity.id]
    
            column_names = [
                "CellID",
                "CCR",
                "CCRatts",
                "Drops"]
    
            create_column = partial(Column, data.partition_a.table())
    
            columns = map(create_column, column_names)
    
            r = retrieve(
                self.conn, SCHEMA, table_names, columns, entities, start, end
            )
    
            self.assertEqual(len(r), 1)
    
            first_result = head(r)
    
            entity_id, timestamp, c1, c2, c3, c4 = first_result
    
            self.assertEqual(entity_id, entity.id)
            self.assertEqual(c4, 18)

    def test_retrieve_ordered_by_time(self):
        with with_data_context(self.conn, TestData) as data:
            table_a = data.partition_a.table()
    
            with closing(self.conn.cursor()) as cursor:
                self.assertEqual(row_count(cursor, table_a), 3)
    
            table_names = [table_a.name]
            start = data.timestamp_1
            end = data.timestamp_1
            entity = data.entities[1]
            entities = [entity.id]
    
            columns = [
                Column(table_a, "CellID"),
                Column(table_a, "CCR"),
                Column(table_a, "CCRatts"),
                Column(table_a, "Drops")]
    
            r = retrieve_orderedby_time(
                self.conn, SCHEMA, table_names, columns, entities, start, end
            )
    
            self.assertEqual(len(r), 1)
    
            first_result = head(r)
    
            entity_id, timestamp, c1, c2, c3, c4 = first_result
    
            self.assertEqual(entity_id, entity.id)
            self.assertEqual(c4, 18)

    def test_retrieve_multi_table_time(self):
        with with_data_context(self.conn, TestData) as data:
            table_d_1 = data.partition_d_1.table()
            table_d_2 = data.partition_d_2.table()
            table_names = [table_d_1.name, table_d_2.name]
            start = data.timestamp_1 - timedelta(seconds=60)
            end = data.timestamp_2
            entity = data.entities[1]
            entities = [entity.id]
    
            column_names = [
                "counter_x"
            ]
    
            columns = [Column(column_name) for column_name in column_names]
    
            r = retrieve(
                self.conn, SCHEMA, table_names, columns, entities, start, end
            )
    
            self.assertEqual(len(r), 2)
    
            first_result = head(r)
    
            entity_id, timestamp, c1 = first_result
    
            self.assertEqual(entity_id, entity.id)
            self.assertEqual(c1, 110)
    
    def test_retrieve_aggregate(self):
        granularity = create_granularity("900")

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.get_by_name("test")(cursor)
            entity_type = EntityType.get_by_name("Cell")(cursor)

            TrendStore(
                data_source, entity_type, granularity, 86400, "table"
            ).create(cursor)

        column_expressions = ["COUNT(entity_id)"]

        start = data_source.tzinfo.localize(datetime(2012, 12, 6, 14, 15))
        end = data_source.tzinfo.localize(datetime(2012, 12, 6, 14, 15))

        interval = start, end

        retrieve_aggregated(
            self.conn, data_source, granularity, entity_type,
            column_expressions, interval, group_by="entity_id"
        )


class TestRetrieveMultiSource(unittest.TestCase):
    def setUp(self):
        self.conn = connect()

    def tearDown(self):
        self.conn.close()

    def test_retrieve(self):
        with with_data_context(self.conn, TestData) as data:

            table_a = data.partition_a.table()
            table_b = data.partition_b.table()
            table_c = data.partition_c.table()

            table_names = [
                table_a.name,
                table_b.name,
                table_c.name
            ]
            start = data.timestamp_1
            end = data.timestamp_1

            table_a_cols = [
                Column("CellID"),
                Column("CCR"),
                Column("CCRatts"),
                Column("Drops")
            ]

            table_b_cols = [
                Column("counter_a"),
                Column("counter_b")
            ]

            table_c_cols = [
                Column("counter_x"),
                Column("counter_y")
            ]

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
                    entitytype=data.entity_type
            )

            data = [["entity_id", "timestamp"] + [c.name for c in columns]] + r

            logging.debug(unlines(render_source(data)))

            self.assertEqual(len(r), 5)


class TestRetrieve(unittest.TestCase):
    def setUp(self):
        self.conn = connect()

    def tearDown(self):
        self.conn.close()

    def test_retrieve(self):
        with with_data_context(self.conn, TestData) as data:
            table_a = data.partition_a.table()

            start = data.timestamp_1
            end = data.timestamp_1
            entity = data.entities[1]
            entities = [entity.id]

            column_names = [
                "CellID",
                "CCR",
                "CCRatts",
                "Drops"
            ]

            create_column = partial(Column, table_a)

            columns = map(create_column, column_names)

            with closing(self.conn.cursor()) as cursor:
                r = retrieve(cursor, [table_a], columns, entities, start, end)

            self.assertEqual(len(r), 1)

            first_result = head(r)

            entity_id, timestamp, c1, c2, c3, c4 = first_result

            self.assertEqual(entity_id, entity.id)
            self.assertEqual(c4, 18)

    def test_retrieve_multi_table_time(self):
        with with_data_context(self.conn, TestData) as data:
            tables = [data.partition_d_1.table(), data.partition_d_2.table()]
            start = data.timestamp_1 - timedelta(seconds=60)
            end = data.timestamp_2
            entity = data.entities[1]
            entities = [entity.id]

            column_names = [
                "counter_x"
            ]

            columns = map(Column, column_names)

            with closing(self.conn.cursor()) as cursor:
                r = retrieve(cursor, tables, columns, entities, start, end)

            self.assertEqual(len(r), 2)

            first_result = head(r)

            entity_id, timestamp, c1 = first_result

            self.assertEqual(entity_id, entity.id)
            self.assertEqual(c1, '110')

    def test_retrieve_aggregate(self):
        with with_data_context(self.conn, TestData) as data:
            column_expressions = [Call("sum", Column("Drops"))]

            start = pytz.UTC.localize(
                datetime(2012, 12, 6, 14, 15)
            )

            end = pytz.UTC.localize(
                datetime(2012, 12, 6, 14, 15)
            )

            interval = start, end

            with closing(self.conn.cursor()) as cursor:
                result = retrieve_aggregated(
                    cursor, data.trend_store_a, column_expressions,
                    interval, group_by=["entity_id"]
                )

            self.assertIsNotNone(result)

    def test_retrieve(self):
        with with_data_context(self.conn, TestData) as data:
            table_a = data.partition_a.table()
            table_b = data.partition_b.table()
            table_c = data.partition_c.table()

            tables = [table_a, table_b, table_c]
            start = data.timestamp_1
            end = data.timestamp_1

            table_a_cols = [
                Column("CellID"),
                Column("CCR"),
                Column("CCRatts"),
                Column("Drops")
            ]

            table_b_cols = [
                Column("counter_a"),
                Column("counter_b")
            ]

            table_c_cols = [
                Column("counter_x"),
                Column("counter_y")
            ]

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
                    cursor, tables, columns, None, start, end,
                    entitytype=data.entity_type
                )

            data = [["entity_id", "timestamp"] + [c.name for c in columns]] + r

            logging.debug(unlines(render_source(data)))

            self.assertEqual(len(r), 5)
