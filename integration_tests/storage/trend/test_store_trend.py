import time
import unittest
from contextlib import closing
from datetime import datetime

from pytz import timezone

from minerva.storage.trend import datapackage
from minerva.storage.trend.tabletrendstorepart import TableTrendStorePart
from minerva.util import first
from minerva.db.query import Table, Column, Call, Eq
from minerva.storage.generic import extract_data_types
from minerva.directory.datasource import DataSource
from minerva.directory.entitytype import EntityType
from minerva.test import connect, clear_database, row_count
from minerva.storage.trend.tabletrendstore import TableTrendStore
from minerva.storage.trend.granularity import create_granularity

SCHEMA = 'trend'

modified_table = Table(SCHEMA, "modified")


class TestStoreTrend(unittest.TestCase):
    def setUp(self):
        self.conn = clear_database(connect())

    def tearDown(self):
        self.conn.close()

    def test_store_copy_from_1(self):
        trend_names = ['CellID', 'CCR', 'CCRatts', 'Drops']

        data_rows = [
            (10023, ('10023', '0.9919', '2105', '17')),
            (10047, ('10047', '0.9963', '4906', '18')),
            (10048, ('10048', '0.9935', '2448', '16')),
            (10049, ('10049', '0.9939', '5271', '32')),
            (10050, ('10050', '0.9940', '3693', '22')),
            (10051, ('10051', '0.9944', '3753', '21')),
            (10052, ('10052', '0.9889', '2168', '24')),
            (10053, ('10053', '0.9920', '2372', '19')),
            (10085, ('10085', '0.9987', '2282', '3')),
            (10086, ('10086', '0.9972', '1763', '5')),
            (10087, ('10087', '0.9931', '1453', '10'))
        ]

        curr_timezone = timezone("Europe/Amsterdam")
        data_types = extract_data_types(data_rows)
        timestamp = curr_timezone.localize(datetime(2013, 1, 2, 10, 45, 0))
        granularity = create_granularity("900")
        modified = curr_timezone.localize(datetime.now())

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.from_name("test-src009")(cursor)
            entity_type = EntityType.from_name("test-type001")(cursor)

            trend_store = TableTrendStore.create(TableTrendStore.Descriptor(
                data_source, entity_type, granularity, [
                    TableTrendStorePart.Descriptor('test_store', [])
                ], 86400
            ))(cursor)
            partition = trend_store.partition('test_store', timestamp)

            table = partition.table()

            partition.create(cursor)

            partition.check_columns_exist(trend_names, data_types)(cursor)

            T = datapackage.refined_package_type_for_entity_type('Cell')

            data_package = T(granularity, timestamp, trend_names, data_rows)

            part = trend_store.part_by_name['test_store']
            part.store_copy_from(data_package, modified)(self.conn)

            self.conn.commit()

            self.assertEqual(row_count(cursor, table), 11)

            table.select(Call("max", Column("modified"))).execute(cursor)

            max_modified = first(cursor.fetchone())

            self.assertEqual(max_modified, modified)

    def test_store_copy_from_2(self):
        trend_names = ['CCR', 'CCRatts', 'Drops']
        data_rows = [
            (10023, ('0.9919', '2105', '17'))
        ]

        data_types = ['integer', 'smallint', 'smallint']

        curr_timezone = timezone("Europe/Amsterdam")
        timestamp = curr_timezone.localize(datetime(2013, 1, 2, 10, 45, 0))
        modified = curr_timezone.localize(datetime.now())
        granularity = create_granularity("900")

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.from_name("test-src010")(cursor)
            entity_type = EntityType.from_name("test-type002")(cursor)
            trend_store = TableTrendStore.create(TableTrendStore.Descriptor(
                data_source, entity_type, granularity, [
                    TableTrendStorePart.Descriptor('test_store', [])
                ], 86400
            ))(cursor)
            partition = trend_store.partition(timestamp)
            partition.create(cursor)
            partition.check_columns_exist(trend_names, data_types)(cursor)
            table = partition.table()

            with self.assertRaises(DataTypeMismatch) as cm:
                store_copy_from(
                    self.conn, SCHEMA, table.name, trend_names, timestamp,
                    modified, data_rows
                )

            self.conn.commit()

            self.assertEqual(row_count(cursor, table), 1)

            table.select(Call("max", Column("modified"))).execute(cursor)

            max_modified = first(cursor.fetchone())

            self.assertEqual(max_modified, modified)

    def test_store_using_tmp(self):
        table = Table(SCHEMA, 'storage_tmp_test_table')
        trend_names = ['CellID', 'CCR', 'RadioFail', 'RFOldHo', 'AbisFailCall']

        data_rows = [
            (10023, ('10023', '0.9919', '10', '3', '3')),
            (10047, ('10047', '0.9963', '11', '5', '0')),
            (10048, ('10048', '0.9935', '12', '3', '0')),
            (10049, ('10049', '0.9939', '20', '3', '4')),
            (10050, ('10050', '0.9940', '18', '3', '0')),
            (10051, ('10051', '0.9944', '17', '2', '2')),
            (10052, ('10052', '0.9889', '18', '2', '0')),
            (10053, ('10053', '0.9920', '15', '3', '1')),
            (10023, ('10023', '0.9931', '9', '0', '1')),
            (10085, ('10085', '0.9987', '3', '0', '0')),
            (10086, ('10086', '0.9972', '3', '2', '0'))
        ]

        data_types = extract_data_types(data_rows)

        with closing(self.conn.cursor()) as cursor:
            table.drop().if_exists().execute(cursor)

            create_trend_table(
                self.conn, SCHEMA, table.name, trend_names, data_types
            )
            curr_timezone = timezone("Europe/Amsterdam")
            timestamp = curr_timezone.localize(datetime(2013, 1, 2, 10, 45, 0))
            modified = curr_timezone.localize(datetime.now())
            store_using_tmp(
                self.conn, SCHEMA, table.name, trend_names, timestamp,
                modified, data_rows
            )

            self.conn.commit()

            self.assertEqual(row_count(cursor, table), 10)

            table.select(Call("max", Column("modified"))).execute(cursor)

            max_modified = first(cursor.fetchone())

            self.assertEqual(max_modified, modified)

    def test_update_modified_column(self):
        curr_timezone = timezone("Europe/Amsterdam")

        trend_names = ['CellID', 'CCR', 'Drops']
        data_rows = [
            (10023, ('10023', '0.9919', '17')),
            (10047, ('10047', '0.9963', '18'))
        ]
        data_types = extract_data_types(data_rows)

        update_data_rows = [(10023, ('10023', '0.9919', '17'))]
        timestamp = curr_timezone.localize(datetime.now())
        granularity = create_granularity("900")

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.from_name("test-src009")(cursor)
            entity_type = EntityType.from_name("test-type001")(cursor)

            trend_store = TableTrendStore.create(TableTrendStore.Descriptor(
                data_source, entity_type, granularity, [
                    TableTrendStorePart.Descriptor('test-store', [])
                ], 86400
            ))(cursor)
            partition = trend_store.partition('test-store', timestamp)

            table = partition.table()

            partition.create(cursor)

            partition.check_columns_exist(trend_names, data_types)(cursor)

            trend_store.store(self.conn, SCHEMA, table.name, trend_names, timestamp, data_rows)
            time.sleep(1)
            trend_store.store(self.conn, SCHEMA, table.name, trend_names, timestamp, update_data_rows)
            self.conn.commit()

            query = table.select([Column("modified")])

            query.execute(cursor)
            modified_list = [modified for modified in cursor.fetchall()]
            self.assertNotEqual(modified_list[0], modified_list[1])

            table.select(Call("max", Column("modified"))).execute(cursor)

            max_modified = first(cursor.fetchone())

            modified_table.select(Column("end")).where_(
                Eq(Column("table_name"), table.name)
            ).execute(cursor)

            end = first(cursor.fetchone())

            self.assertEqual(end, max_modified)

    def test_update(self):
        trend_names = ["CellID", "CCR", "Drops"]
        data_rows = [
            (10023, ("10023", "0.9919", "17")),
            (10047, ("10047", "0.9963", "18"))
        ]
        data_types = extract_data_types(data_rows)
        update_data_rows = [(10023, ("10023", "0.5555", "17"))]
        timestamp = datetime.now()
        granularity = create_granularity("900")

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.from_name("test-src009")(cursor)
            entity_type = EntityType.from_name("test-type001")(cursor)

            trend_store = TableTrendStore.create(TableTrendStore.Descriptor(
                data_source, entity_type, granularity, [
                    TableTrendStorePart.Descriptor('test-store', [])
                ], 86400
            ))(cursor)

            partition = trend_store.partition('test-store', timestamp)

            table = partition.table()

            partition.create(cursor)

            partition.check_columns_exist(trend_names, data_types)(cursor)

        trend_store.store(self.conn, SCHEMA, table.name, trend_names, timestamp, data_rows)

        store(self.conn, SCHEMA, table.name, trend_names, timestamp, update_data_rows)
        self.conn.commit()

        query = table.select([Column("modified"), Column("CCR")])

        with closing(self.conn.cursor()) as cursor:
            query.execute(cursor)
            rows = cursor.fetchall()

        self.assertNotEqual(rows[0][0], rows[1][0])
        self.assertNotEqual(rows[0][1], rows[1][1])
