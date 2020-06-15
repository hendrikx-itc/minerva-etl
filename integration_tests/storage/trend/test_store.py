import time
from datetime import timedelta, datetime
from contextlib import closing
from functools import partial
from operator import contains
import unittest
import datetime

import pytz

from minerva.db.query import Table, Call, Column, Eq, And
from minerva.db.error import DataTypeMismatch
from minerva.storage.trend.trendstorepart import TrendStorePart
from minerva.test import connect, clear_database, with_data_context
from minerva.directory import DataSource, EntityType
from minerva.storage import datatype
from minerva.storage.trend.datapackage import \
    refined_package_type_for_entity_type
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.trend import Trend, NoSuchTrendError
from minerva.storage.trend.partitioning import Partitioning
from minerva.storage.trend.granularity import create_granularity

from minerva.storage.trend.test import DataSet

modified_table = Table("trend_directory", "modified")


class TestStore(unittest.TestCase):
    def setUp(self):
        self.conn = clear_database(connect())

    def tearDown(self):
        self.conn.close()

    def test_store_copy_from_1(self):
        trend_descriptors = [
            Trend.Descriptor('CellID', datatype.registry['integer'], ''),
            Trend.Descriptor('CCR', datatype.registry['double precision'], ''),
            Trend.Descriptor('CCRatts', datatype.registry['smallint'], ''),
            Trend.Descriptor('Drops', datatype.registry['smallint'], '')
        ]

        trend_store_part_descr = TrendStorePart.Descriptor(
            'test-trend-store', trend_descriptors
        )

        trend_names = [t.name for t in trend_descriptors]

        data_rows = [
            (10023, (10023, 0.9919, 2105, 17)),
            (10047, (10047, 0.9963, 4906, 18)),
            (10048, (10048, 0.9935, 2448, 16)),
            (10049, (10049, 0.9939, 5271, 32)),
            (10050, (10050, 0.9940, 3693, 22)),
            (10051, (10051, 0.9944, 3753, 21)),
            (10052, (10052, 0.9889, 2168, 24)),
            (10053, (10053, 0.9920, 2372, 19)),
            (10085, (10085, 0.9987, 2282, 3)),
            (10086, (10086, 0.9972, 1763, 5)),
            (10087, (10087, 0.9931, 1453, 10))
        ]

        timestamp = pytz.utc.localize(datetime.datetime(2013, 1, 2, 10, 45, 0))
        granularity = create_granularity("900")
        modified = pytz.utc.localize(datetime.datetime.now())

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.from_name("test-src009")(cursor)
            entity_type = EntityType.from_name("test-type001")(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [trend_store_part_descr], 86400
            ))(cursor)

            partition = trend_store.partition(timestamp)
            partition.create(cursor)

            table = partition.table()

            trend_store.store_copy_from(
                refined_package_type_for_entity_type('Node')(
                    granularity, timestamp, trend_names, data_rows
                ),
                modified
            )(cursor)

            self.assertEqual(row_count(cursor, table), 11)

            table.select(Call("max", Column("modified"))).execute(cursor)

            max_modified, = cursor.fetchone()

            self.assertEqual(max_modified, modified)

    def test_store_copy_from_2(self):
        trend_descriptors = [
            Trend.Descriptor(
                'CCR', datatype.registry['integer'], ''),
            Trend.Descriptor(
                'CCRatts', datatype.registry['smallint'], ''),
            Trend.Descriptor(
                'Drops', datatype.registry['smallint'], '')
        ]

        trend_names = [t.name for t in trend_descriptors]

        data_rows = [
            (10023, (0.9919, 2105, 17))
        ]

        timestamp = pytz.utc.localize(datetime.datetime(2013, 1, 2, 10, 45, 0))
        modified = pytz.utc.localize(datetime.datetime.now())
        granularity = create_granularity("900")

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.from_name("test-src010")(cursor)
            entity_type = EntityType.from_name("test-type002")(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [TrendStorePart.Descriptor(
                    'test-trend-store', trend_descriptors
                )], 86400
            ))(cursor)
            partition = trend_store.partition(timestamp)
            partition.create(cursor)

            with self.assertRaises(DataTypeMismatch) as cm:
                trend_store.store_copy_from(
                    refined_package_type_for_entity_type('Node')(
                        granularity, timestamp, trend_names, data_rows
                    ),
                    modified
                )(cursor)

    def test_store_using_tmp(self):
        granularity = create_granularity(900)

        trend_descriptors = [
            Trend.Descriptor('CellID', datatype.registry['smallint'], ''),
            Trend.Descriptor('CCR', datatype.registry['double precision'], ''),
            Trend.Descriptor('RadioFail', datatype.registry['smallint'], ''),
            Trend.Descriptor('RFOldHo', datatype.registry['smallint'], ''),
            Trend.Descriptor('AbisFailCall', datatype.registry['smallint'], '')
        ]

        trend_names = [t.name for t in trend_descriptors]

        data_rows = [
            (10023, (10023, 0.9919, 10, 3, 3)),
            (10047, (10047, 0.9963, 11, 5, 0)),
            (10048, (10048, 0.9935, 12, 3, 0)),
            (10049, (10049, 0.9939, 20, 3, 4)),
            (10050, (10050, 0.9940, 18, 3, 0)),
            (10051, (10051, 0.9944, 17, 2, 2)),
            (10052, (10052, 0.9889, 18, 2, 0)),
            (10053, (10053, 0.9920, 15, 3, 1)),
            (10085, (10085, 0.9987, 3, 0, 0)),
            (10086, (10086, 0.9972, 3, 2, 0))
        ]

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.from_name("test-src009")(cursor)
            entity_type = EntityType.from_name("test-type001")(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [TrendStorePart.Descriptor(
                    'test-trend-store', trend_descriptors
                )], 86400
            ))(cursor)

            self.conn.commit()

            timestamp = pytz.utc.localize(datetime.datetime(2013, 1, 2, 10, 45, 0))

            partition = trend_store.partition(timestamp)

            table = partition.table()

            table.drop().if_exists().execute(cursor)

            partition.create(cursor)

            modified = pytz.utc.localize(datetime.datetime.now())

            trend_store.store_update(
                refined_package_type_for_entity_type('Node')(
                    granularity, timestamp, trend_names, data_rows
                ),
                modified
            )(cursor)

            self.conn.commit()

            self.assertEqual(row_count(cursor, table), 10)

            table.select(Call("max", Column("modified"))).execute(cursor)

            max_modified, = cursor.fetchone()

            self.assertEqual(max_modified, modified)

    def test_store_insert_rows(self):
        granularity = create_granularity("900")

        trend_descriptors = [
            Trend.Descriptor('CellID', datatype.registry['smallint'], ''),
            Trend.Descriptor('CCR', datatype.registry['double precision'], ''),
            Trend.Descriptor('Drops', datatype.registry['smallint'], ''),
        ]

        trend_names = [t.name for t in trend_descriptors]

        data_rows = [
            (10023, (10023, 0.9919, 17)),
            (10047, (10047, 0.9963, 18))
        ]

        modified = pytz.utc.localize(datetime.datetime.now())
        time1 = pytz.utc.localize(datetime.datetime(2013, 1, 2, 10, 45, 0))
        time2 = time1 - timedelta(days=1)

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.from_name("test-src009")(cursor)
            entity_type = EntityType.from_name("test-type001")(cursor)

            trend_store = TrendStore.create(
                TrendStore.Descriptor(
                    data_source, entity_type,
                    granularity, [TrendStorePart.Descriptor(
                        'test-trend-store', trend_descriptors
                    )], 86400 * 7
                )
            )(cursor)

            partition = trend_store.partition(time1)

            table = partition.table()

            table.drop().if_exists().execute(cursor)

            partition.create(cursor)

            trend_store.store_batch_insert(
                refined_package_type_for_entity_type('Node')(
                    granularity, time1, trend_names, data_rows
                ), modified
            )(cursor)
            self.conn.commit()

            self.assertEqual(row_count(cursor, table), 2)

            trend_store.store_batch_insert(
                refined_package_type_for_entity_type('Node')(
                    granularity, time2, trend_names, data_rows
                ), modified
            )(cursor)
            self.conn.commit()

            self.assertEqual(row_count(cursor, table), 4)

            self.conn.commit()

            self.assertEqual(row_count(cursor, table), 4)

            table.select(Call("max", Column("modified"))).execute(cursor)

            max_modified, = cursor.fetchone()

            self.assertEqual(max_modified, modified)

    def test_update_modified_column(self):
        trend_descriptors = [
            Trend.Descriptor('CellID', datatype.registry['smallint'], ''),
            Trend.Descriptor('CCR', datatype.registry['double precision'], ''),
            Trend.Descriptor('Drops', datatype.registry['smallint'], ''),
        ]

        trend_names = [t.name for t in trend_descriptors]

        data_rows = [
            (10023, (10023, 0.9919, 17)),
            (10047, (10047, 0.9963, 18))
        ]

        update_data_rows = [
            (10023, (10023, 0.9919, 17))
        ]

        timestamp = pytz.utc.localize(datetime.datetime.now())
        granularity = create_granularity("900")

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.from_name("test-src009")(cursor)
            entity_type = EntityType.from_name("test-type001")(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type,
                granularity, [TrendStorePart.Descriptor(
                    'test-trend-store', trend_descriptors
                )], 86400
            ))(cursor)

            self.conn.commit()

            partition = trend_store.partition(timestamp)

            table = partition.table()

            partition.create(cursor)

            trend_store.store(
                refined_package_type_for_entity_type('Node')(
                    granularity, timestamp, trend_names, data_rows
                )
            ).run(self.conn)

            time.sleep(1)

            trend_store.store(
                refined_package_type_for_entity_type('Node')(
                    granularity, timestamp, trend_names, update_data_rows
                )
            ).run(self.conn)
            self.conn.commit()

            query = table.select([Column("modified")])

            query.execute(cursor)
            modified_list = [modified for modified in cursor.fetchall()]
            self.assertNotEqual(modified_list[0], modified_list[1])

            table.select(Call("max", Column("modified"))).execute(cursor)

            max_modified, = cursor.fetchone()

            modified_table.select(Column("end")).where_(
                Eq(Column("table_trend_store_id"), trend_store.id)
            ).execute(cursor)

            end, = cursor.fetchone()

            self.assertEqual(end, max_modified)

    def test_update(self):
        trend_descriptors = [
            Trend.Descriptor("CellID", datatype.registry['smallint'], ''),
            Trend.Descriptor("CCR", datatype.registry['double precision'], ''),
            Trend.Descriptor("Drops", datatype.registry['smallint'], '')
        ]

        trend_names = [t.name for t in trend_descriptors]

        data_rows = [
            (10023, (10023, 0.9919, 17)),
            (10047, (10047, 0.9963, 18))
        ]

        update_data_rows = [
            (10023, (10023, 0.5555, 17))
        ]

        timestamp = datetime.datetime.now()
        granularity = create_granularity("900")

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.from_name("test-src009")(cursor)
            entity_type = EntityType.from_name("test-type001")(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [TrendStorePart.Descriptor(
                    'test-trend-store', trend_descriptors
                )], 86400
                ))(cursor)

            partition = trend_store.partition(timestamp)
            partition.create(cursor)

            table = partition.table()

        trend_store.store(
            refined_package_type_for_entity_type('Node')(
                granularity, timestamp, trend_names, data_rows
            )
        ).run(self.conn)

        trend_store.store(
            refined_package_type_for_entity_type('Node')(
                granularity, timestamp, trend_names, update_data_rows
            )
        ).run(self.conn)

        self.conn.commit()

        query = table.select([Column("modified"), Column("CCR")])

        with closing(self.conn.cursor()) as cursor:
            query.execute(cursor)
            rows = cursor.fetchall()

        self.assertNotEqual(rows[0][0], rows[1][0])
        self.assertNotEqual(rows[0][1], rows[1][1])

    def test_update_and_modify_columns_fractured(self):
        granularity = create_granularity("900")
        timestamp = pytz.utc.localize(datetime.datetime(2013, 1, 2, 10, 45, 0))
        entity_ids = range(1023, 1023 + 100)

        trend_descriptors = [
            Trend.Descriptor("CellID", datatype.registry['smallint'], ''),
            Trend.Descriptor("CCR", datatype.registry['double precision'], ''),
            Trend.Descriptor("Drops", datatype.registry['smallint'], '')
        ]

        trend_names_a = ["CellID", "CCR", "Drops"]

        data_rows_a = [
            (i, (10023, 0.9919, 17))
            for i in entity_ids
        ]

        trend_names_b = ["CellID", "Drops"]
        data_rows_b = [
            (i, (10023, 19))
            for i in entity_ids
        ]

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.from_name("test-src009")(cursor)
            entity_type = EntityType.from_name("test-type001")(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [TrendStorePart.Descriptor(
                    'test-trend-store', trend_descriptors
                )], 86400
            ))(cursor)

            partition = trend_store.partition(timestamp)
            partition.create(cursor)

            table = partition.table()

            self.conn.commit()

        trend_store.store(
            refined_package_type_for_entity_type('Node')(
                granularity, timestamp, trend_names_a, data_rows_a
            )
        ).run(self.conn)
        time.sleep(0.2)

        check_columns = list(map(Column, ["modified", "Drops"]))
        query = table.select(check_columns)

        with closing(self.conn.cursor()) as cursor:
            query.execute(cursor)
            row_before = cursor.fetchone()

        trend_store.store(
            refined_package_type_for_entity_type('Node')(
                granularity, timestamp, trend_names_b, data_rows_b
            )
        ).run(self.conn)

        query = table.select(check_columns)

        with closing(self.conn.cursor()) as cursor:
            query.execute(cursor)
            row_after = cursor.fetchone()

        self.assertNotEqual(row_before[0], row_after[0])
        self.assertNotEqual(row_before[1], row_after[1])

    def test_create_trend_store(self):
        granularity = create_granularity('900 seconds')

        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            create_trend_store = TrendStore.create(
                TrendStore.Descriptor(
                    data_source, entity_type, granularity,
                    [TrendStorePart.Descriptor('test-trend-store', [])],
                    partition_size
                )
            )

            trend_store = create_trend_store(cursor)

        assert isinstance(trend_store, TrendStore)

        assert trend_store.id is not None

    def test_create_trend_store_with_children(self):
        granularity = create_granularity('900 seconds')

        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [TrendStorePart.Descriptor('test-trend-store', [])],
                partition_size
            ))(cursor)

            assert trend_store.id is not None

            timestamp = pytz.utc.localize(
                datetime.datetime(2013, 5, 6, 14, 45)
            )

            partition = trend_store.partition(timestamp)

            partition.create(cursor)

    def test_get_trend_store(self):
        partition_size = 3600

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)
            granularity = create_granularity('900 seconds')

            TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [TrendStorePart.Descriptor('test-trend-store', [])],
                partition_size
            ))(cursor)

            trend_store = TrendStore.get(
                data_source, entity_type, granularity
            )(cursor)

            self.assertEqual(trend_store.data_source.id, data_source.id)
            self.assertEqual(trend_store.partition_size, partition_size)
            assert trend_store.id is not None, "trend_store.id is None"

    def test_store_copy_from(self):
        granularity = create_granularity('900 seconds')

        partition_size = 86400

        trend_descriptors = [
            Trend.Descriptor('a', datatype.registry['smallint'], ''),
            Trend.Descriptor('b', datatype.registry['smallint'], ''),
            Trend.Descriptor('c', datatype.registry['smallint'], '')
        ]

        trend_names = [t.name for t in trend_descriptors]

        timestamp = pytz.utc.localize(
            datetime.datetime(2013, 4, 25, 9, 45)
        )

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [TrendStorePart.Descriptor(
                    'test-trend-store', trend_descriptors
                )], partition_size
            ))(cursor)

            trend_store.partition(timestamp).create(cursor)

        self.conn.commit()

        def make_row(index):
            return 1234 + index, [1, 2, 3 + index]

        rows = list(map(make_row, range(100)))

        data_package = refined_package_type_for_entity_type('Node')(
            granularity, timestamp, trend_names, rows
        )

        transaction = trend_store.store(data_package)
        transaction.run(self.conn)

        transaction = trend_store.store(data_package)
        transaction.run(self.conn)

    def test_store_copy_from_missing_column(self):
        granularity = create_granularity('900 seconds')

        partition_size = 86400

        trend_descriptors = [
            Trend.Descriptor('a', datatype.registry['smallint'], ''),
            Trend.Descriptor('b', datatype.registry['smallint'], ''),
            Trend.Descriptor('c', datatype.registry['smallint'], '')
        ]

        trend_names = [t.name for t in trend_descriptors]

        timestamp = pytz.utc.localize(
            datetime.datetime(2013, 4, 25, 9, 45)
        )

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [TrendStorePart.Descriptor(
                    'test-trend-store', trend_descriptors
                )], partition_size
            ))(cursor)

            trend_store.partition(timestamp).create(cursor)

        self.conn.commit()

        rows = [
            (1234 + index, [1, 2, 3 + index])
            for index in range(100)
        ]

        data_package = refined_package_type_for_entity_type('Node')(
            granularity, timestamp, trend_names, rows
        )

        transaction = trend_store.store(data_package)
        transaction.run(self.conn)

        # Store second part with one column extra

        timestamp = pytz.utc.localize(
            datetime.datetime(2013, 4, 25, 10, 00)
        )

        trends = ["a", "b", "c", "d"]

        def make_row_y(index):
            return 1234 + index, [1, 2, 3, 4 + index]

        rows = list(map(make_row_y, range(100)))

        data_package = refined_package_type_for_entity_type('Node')(
            granularity, timestamp, trends, rows
        )

        with self.assertRaises(NoSuchTrendError) as cm:
            transaction = trend_store.store(data_package)
            transaction.run(self.conn)

    def test_store(self):
        granularity = create_granularity('900 seconds')

        partition_size = 86400

        trend_descriptors = [
            Trend.Descriptor('a', datatype.registry['smallint'], ''),
            Trend.Descriptor('b', datatype.registry['smallint'], ''),
            Trend.Descriptor('c', datatype.registry['smallint'], '')
        ]

        trend_names = [t.name for t in trend_descriptors]

        timestamp = pytz.utc.localize(
            datetime.datetime(2013, 4, 25, 9, 45)
        )

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [
                    TrendStorePart.Descriptor(
                        'test-trend-store', trend_descriptors
                    )
                ], partition_size
            ))(cursor)

            trend_store.partition(timestamp).create(cursor)

        self.conn.commit()

        rows = [
            (1234, [1, 2, 3]),
            (2345, [4, 5, 6])
        ]

        data_package = refined_package_type_for_entity_type('Node')(
            granularity, timestamp, trend_names, rows
        )

        transaction = trend_store.store(data_package)
        transaction.run(self.conn)

        table = trend_store.partition(timestamp).table()

        condition = And(
            Eq(Column("entity_id"), 2345),
            Eq(Column("timestamp"), timestamp)
        )

        query = table.select(Column("c")).where_(condition)

        with closing(self.conn.cursor()) as cursor:
            query.execute(cursor)

            c, = cursor.fetchone()

        self.assertEqual(c, 6)

        rows = [
            (1234, [1, 2, 3]),
            (2345, [4, 5, 7])
        ]

        data_package = refined_package_type_for_entity_type('Node')(
            granularity, timestamp, trend_names, rows
        )

        transaction = trend_store.store(data_package)
        transaction.run(self.conn)

        with closing(self.conn.cursor()) as cursor:
            query.execute(cursor)

            c, = cursor.fetchone()

        self.assertEqual(c, 7)

    def test_generate_index(self):
        granularity = create_granularity('900 seconds')

        partition_size = 86400

        start = pytz.utc.localize(datetime.datetime(2013, 4, 25, 9, 45))
        end = pytz.utc.localize(datetime.datetime(2013, 4, 27, 9, 45))

        partitioning = Partitioning(partition_size)

        for timestamp in granularity.range(start, end):
            partition_index = partitioning.index(timestamp)

            args = partition_size, timestamp

            with closing(self.conn.cursor()) as cursor:
                cursor.callproc("trend_directory.timestamp_to_index", args)

                postgresql_partition_index, = cursor.fetchone()

            self.assertEqual(postgresql_partition_index, partition_index)

    def test_store_alter_column(self):
        granularity = create_granularity('900 seconds')
        partition_size = 86400

        trend_descriptors = [
            Trend.Descriptor('a', datatype.registry['smallint'], ''),
            Trend.Descriptor('b', datatype.registry['smallint'], ''),
            Trend.Descriptor('c', datatype.registry['smallint'], ''),
        ]

        trend_names = [t.name for t in trend_descriptors]

        timestamp = pytz.utc.localize(
            datetime.datetime(2013, 4, 25, 11, 00)
        )

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [
                    TrendStorePart.Descriptor(
                        'test-trend-store', trend_descriptors
                    )
                ], partition_size
            ))(cursor)

            trend_store.partition(timestamp).create(cursor)

        self.conn.commit()

        rows = [
            (1234, [1, 2, 3]),
            (2345, [4, 5, 6])
        ]

        data_package = refined_package_type_for_entity_type('Node')(
            granularity, timestamp, trend_names, rows
        )

        transaction = trend_store.store(data_package)
        transaction.run(self.conn)

        table = trend_store.partition(timestamp).table()

        condition = And(
            Eq(Column("entity_id"), 2345),
            Eq(Column("timestamp"), timestamp)
        )

        query = table.select(Column("c")).where_(condition)

        with closing(self.conn.cursor()) as cursor:
            query.execute(cursor)

            c, = cursor.fetchone()

        self.assertEqual(c, 6)

        rows = [
            (2345, [4, 5, "2013-04-25 11:00:00"])
        ]

        data_package = refined_package_type_for_entity_type('Node')(
            granularity, timestamp, trend_names, rows
        )

        with self.assertRaises(DataTypeMismatch) as cm:
            transaction = trend_store.store(data_package)
            transaction.run(self.conn)

        with closing(self.conn.cursor()) as cursor:
            query.execute(cursor)

            c, = cursor.fetchone()

        self.assertEqual(c, datetime.datetime(2013, 4, 25, 11, 0, 0))

    def test_store_ignore_column(self):
        partition_size = 86400

        trend_descriptors = [
            Trend.Descriptor('x', datatype.registry['smallint'], ''),
            Trend.Descriptor('y', datatype.registry['smallint'], ''),
        ]

        timestamp = pytz.utc.localize(datetime.datetime(2013, 4, 25, 10, 45))

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.create("test-source", '')(cursor)
            entity_type = EntityType.create("test_type", '')(cursor)
            granularity = create_granularity('900 seconds')

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [
                    TrendStorePart.Descriptor(
                        'test-trend-store', trend_descriptors
                    )
                ], partition_size
            ))(cursor)

            trend_store.partition(timestamp).create(cursor)

        self.conn.commit()

        data_package = refined_package_type_for_entity_type('Node')(
            granularity,
            timestamp,
            ['x', 'y', 'z'],
            [
                (1234, [1, 2, 3]),
                (2345, [4, 5, 6])
            ]
        )

        trend_names = [t.name for t in trend_store.trends]

        transaction = trend_store.store(
            data_package.filter_trends(partial(contains, set(trend_names)))
        )

        transaction.run(self.conn)


def row_count(cursor, table):
    cursor.execute("SELECT count(*) FROM {}".format(table.render()))

    count, = cursor.fetchone()

    return count


class TestData(DataSet):
    def __init__(self):
        self.granularity = create_granularity("900")
        self.data_source = None
        self.entity_type = None

    def load(self, conn):
        with closing(conn.cursor()) as cursor:
            self.data_source = DataSource.from_name("test-source")(cursor)
            self.entity_type = EntityType.from_name("test_type")(cursor)


class TestStore(unittest.TestCase):
    def setUp(self):
        self.conn = clear_database(connect())

    def tearDown(self):
        self.conn.close()

    def test_create_trend_store(self):
        with with_data_context(self.conn, TestData) as data_set:
            partition_size = 3600
            retention_period = 86400 * 7

            trend_store_descr = TrendStore.Descriptor(
                data_set.data_source, data_set.entity_type,
                data_set.granularity, [
                    TrendStorePart.Descriptor('test-store', [])
                ], partition_size
            )

            with closing(self.conn.cursor()) as cursor:
                trend_store = TrendStore.create(trend_store_descr)(cursor)

            self.assertIsInstance(trend_store, TrendStore)

            self.assertIsNotNone(trend_store.id)

    def test_create_trend_store_with_children(self):
        with with_data_context(self.conn, TestData) as data_set:
            partition_size = 3600

            trend_store_descr = TrendStore.Descriptor(
                data_set.data_source, data_set.entity_type,
                data_set.granularity,
                [TrendStorePart.Descriptor('test_store', [])],
                partition_size
            )

            with closing(self.conn.cursor()) as cursor:
                trend_store = TrendStore.create(trend_store_descr)(cursor)

                self.assertIsNotNone(trend_store.id)

                timestamp = pytz.utc.localize(
                    datetime.datetime(2013, 5, 6, 14, 45)
                )

                partition = trend_store.partition('test_store', timestamp)

                partition.create(cursor)

    def test_get_trend_store(self):
        with with_data_context(self.conn, TestData) as data_set:
            partition_size = 3600

            with closing(self.conn.cursor()) as cursor:
                trend_store_descr = TrendStore.Descriptor(
                    data_set.data_source, data_set.entity_type,
                    data_set.granularity, [
                        TrendStorePart.Descriptor('test_store', [])
                    ], partition_size
                )

                TrendStore.create(trend_store_descr)(cursor)

                trend_store = TrendStore.get(
                    data_set.data_source, data_set.entity_type,
                    data_set.granularity
                )(cursor)

                self.assertEqual(
                    trend_store.data_source.id, data_set.data_source.id
                )
                self.assertEqual(trend_store.partition_size, partition_size)
                self.assertIsNotNone(trend_store.id)

    def test_store_copy_from(self):
        with with_data_context(self.conn, TestData) as data_set:
            partition_size = 86400

            with closing(self.conn.cursor()) as cursor:
                trend_store_descr = TrendStore.Descriptor(
                    data_set.data_source, data_set.entity_type,
                    data_set.granularity, [
                        TrendStorePart.Descriptor('test_store', [])
                    ], partition_size
                )

                trend_store = TrendStore.create(trend_store_descr)(cursor)

            self.conn.commit()

            timestamp = pytz.utc.localize(
                datetime.datetime(2013, 4, 25, 9, 45)
            )

            trends = ["a", "b", "c"]

            def make_row(index):
                return 1234 + index, [1, 2, 3 + index]

            rows = list(map(make_row, range(100)))

            data_package = refined_package_type_for_entity_type('TestType')(
                data_set.granularity, timestamp, trends, rows
            )

            trend_store.store(data_package)(self.conn)

            trend_store.store(data_package)(self.conn)

    def test_store_copy_from_missing_column(self):
        with with_data_context(self.conn, TestData) as dataset:
            partition_size = 86400

            trend_store_descr = TrendStore.Descriptor(
                dataset.data_source, dataset.entity_type,
                dataset.granularity, [
                    TrendStorePart.Descriptor('test_store', [])
                ], partition_size
            )

            with closing(self.conn.cursor()) as cursor:
                trend_store = TrendStore.create(trend_store_descr)(cursor)

            self.conn.commit()

            timestamp = pytz.utc.localize(
                datetime.datetime(2013, 4, 25, 9, 45)
            )

            trends = ["a", "b", "c"]

            def make_row_x(index):
                return 1234 + index, [1, 2, 3 + index]

            rows = list(map(make_row_x, range(100)))

            data_package = refined_package_type_for_entity_type('TestType')(
                dataset.granularity, timestamp, trends, rows
            )

            trend_store.store(data_package)(self.conn)

            # Store second part with one column extra

            timestamp = pytz.utc.localize(
                datetime.datetime(2013, 4, 25, 10, 00)
            )

            trends = ["a", "b", "c", "d"]

            def make_row_y(index):
                return 1234 + index, [1, 2, 3, 4 + index]

            rows = list(map(make_row_y, range(100)))

            data_package = refined_package_type_for_entity_type('TestType')(
                dataset.granularity, timestamp, trends, rows
            )

            trend_store.store(data_package)(self.conn)

    def test_store(self):
        with with_data_context(self.conn, TestData) as dataset:
            partition_size = 86400

            timestamp = pytz.utc.localize(
                datetime.datetime(2013, 4, 25, 9, 45)
            )

            trend_store_descr = TrendStore.Descriptor(
                dataset.data_source, dataset.entity_type,
                dataset.granularity, [
                    TrendStorePart.Descriptor('TestStore', [])
                ], partition_size
            )

            with closing(self.conn.cursor()) as cursor:
                trend_store = TrendStore.create(trend_store_descr)(cursor)
                trend_store.partition('TestStore', timestamp).create(cursor)

            self.conn.commit()

            trends = ["a", "b", "c"]

            rows = [
                (1234, [1, 2, 3]),
                (2345, [4, 5, 6])
            ]

            data_package = refined_package_type_for_entity_type('TestType')(
                dataset.granularity, timestamp, trends, rows
            )

            trend_store.store(data_package)(self.conn)

            table = trend_store.partition('TestStore', timestamp).table()

            condition = And(
                Eq(Column("entity_id"), 2345),
                Eq(Column("timestamp"), timestamp)
            )

            query = table.select(Column("c")).where_(condition)

            with closing(self.conn.cursor()) as cursor:
                query.execute(cursor)

                c, = cursor.fetchone()

            self.assertEqual(c, 6)

            rows = [
                (1234, [1, 2, 3]),
                (2345, [4, 5, 7])
            ]

            data_package = refined_package_type_for_entity_type('TestType')(
                dataset.granularity, timestamp, trends, rows
            )

            trend_store.store(data_package)(self.conn)

            with closing(self.conn.cursor()) as cursor:
                query.execute(cursor)

                c, = cursor.fetchone()

            self.assertEqual(c, 7)
