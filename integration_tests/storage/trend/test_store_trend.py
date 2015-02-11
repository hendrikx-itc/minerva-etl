import time
from contextlib import closing
from datetime import datetime, timedelta

from nose.tools import eq_, raises, assert_not_equal
import pytz

from minerva.util import first
from minerva.db.query import Table, Column, Call, Eq
from minerva.db.error import DataTypeMismatch
from minerva.directory import DataSource, EntityType
from minerva.storage.generic import extract_data_types
from minerva.test import with_conn
from minerva.storage.trend.trendstore import TrendStore, TrendStoreDescriptor, \
    store_copy_from, store_update, store_batch_insert
from minerva.storage.trend.trend import TrendDescriptor
from minerva.storage.trend.datapackage import DataPackage
from minerva.storage.trend.granularity import create_granularity

from minerva_db import clear_database
from helpers import row_count


modified_table = Table("trend_directory", "modified")


@with_conn(clear_database)
def test_store_copy_from_1(conn):
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

    data_types = extract_data_types(data_rows)
    timestamp = pytz.utc.localize(datetime(2013, 1, 2, 10, 45, 0))
    granularity = create_granularity("900")
    modified = pytz.utc.localize(datetime.now())

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name("test-type001")(cursor)

        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_source, entity_type, granularity,
            [
                TrendDescriptor(trend_name, data_type, '')
                for trend_name, data_type in zip(trend_names, data_types)
            ],
            86400
        ))(cursor)

        partition = trend_store.partition(timestamp)
        partition.create(cursor)

        table = partition.table()

        store_copy_from(
            cursor,
            table,
            DataPackage(granularity, timestamp, trend_names, data_rows),
            modified
        )

        eq_(row_count(cursor, table), 11)

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        eq_(max_modified, modified)


@raises(DataTypeMismatch)
@with_conn(clear_database)
def test_store_copy_from_2(conn):
    trend_names = ['CCR', 'CCRatts', 'Drops']
    data_types = ['integer', 'smallint', 'smallint']

    data_rows = [
        (10023, ('0.9919', '2105', '17'))
    ]

    timestamp = pytz.utc.localize(datetime(2013, 1, 2, 10, 45, 0))
    modified = pytz.utc.localize(datetime.now())
    granularity = create_granularity("900")

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src010")(cursor)
        entity_type = EntityType.from_name("test-type002")(cursor)

        trend_descriptors = [
            TrendDescriptor(trend_name, data_type, '')
            for trend_name, data_type in zip(trend_names, data_types)
        ]

        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_source, entity_type, granularity, trend_descriptors, 86400
        ))(cursor)
        partition = trend_store.partition(timestamp)
        partition.create(cursor)
        table = partition.table()

        store_copy_from(
            cursor,
            table,
            DataPackage(granularity, timestamp, trend_names, data_rows),
            modified
        )

        eq_(row_count(cursor, table), 1)

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        eq_(max_modified, modified)


@with_conn(clear_database)
def test_store_using_tmp(conn):
    granularity = create_granularity(900)
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

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name("test-type001")(cursor)

        trend_descriptors = [
            TrendDescriptor(trend_name, data_type, '')
            for trend_name, data_type in zip(trend_names, data_types)
        ]

        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_source, entity_type, granularity, trend_descriptors, 86400
        ))(cursor)

        timestamp = pytz.utc.localize(datetime(2013, 1, 2, 10, 45, 0))

        partition = trend_store.partition(timestamp)

        table = partition.table()

        table.drop().if_exists().execute(cursor)

        partition.create(cursor)

        modified = pytz.utc.localize(datetime.now())

        store_update(
            cursor,
            table,
            DataPackage(granularity, timestamp, trend_names, data_rows),
            modified
        )

        conn.commit()

        eq_(row_count(cursor, table), 10)

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        eq_(max_modified, modified)


@with_conn(clear_database)
def test_store_insert_rows(conn):
    granularity = create_granularity("900")
    trend_names = ['CellID', 'CCR', 'Drops']
    data_rows = [
        (10023, ('10023', '0.9919', '17')),
        (10047, ('10047', '0.9963', '18'))
    ]

    modified = pytz.utc.localize(datetime.now())
    time1 = pytz.utc.localize(datetime(2013, 1, 2, 10, 45, 0))
    time2 = time1 - timedelta(days=1)

    data_types = extract_data_types(data_rows)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name("test-type001")(cursor)

        trend_descriptors = [
            TrendDescriptor(trend_name, data_type, '')
            for trend_name, data_type in zip(trend_names, data_types)
        ]

        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_source, entity_type, granularity, trend_descriptors, 86400 * 7
        ))(cursor)

        partition = trend_store.partition(time1)

        table = partition.table()

        table.drop().if_exists().execute(cursor)

        partition.create(cursor)

        store_batch_insert(
            cursor,
            table,
            DataPackage(granularity, time1, trend_names, data_rows), modified
        )
        conn.commit()

        eq_(row_count(cursor, table), 2)

        store_batch_insert(
            cursor,
            table,
            DataPackage(granularity, time2, trend_names, data_rows), modified
        )
        conn.commit()

        eq_(row_count(cursor, table), 4)

        conn.commit()

        eq_(row_count(cursor, table), 4)

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        eq_(max_modified, modified)


@with_conn(clear_database)
def test_update_modified_column(conn):
    trend_names = ['CellID', 'CCR', 'Drops']
    data_rows = [
        (10023, ('10023', '0.9919', '17')),
        (10047, ('10047', '0.9963', '18'))
    ]
    data_types = extract_data_types(data_rows)

    update_data_rows = [(10023, ('10023', '0.9919', '17'))]
    timestamp = pytz.utc.localize(datetime.now())
    granularity = create_granularity("900")

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name("test-type001")(cursor)

        trend_descriptors = [
            TrendDescriptor(trend_name, data_type, '')
            for trend_name, data_type in zip(trend_names, data_types)
        ]

        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_source, entity_type, granularity, trend_descriptors, 86400
        ))(cursor)

        partition = trend_store.partition(timestamp)

        table = partition.table()

        partition.create(cursor)

        trend_store.store(
            DataPackage(
                granularity, timestamp, trend_names, data_rows
            )
        ).run(conn)

        time.sleep(1)
        trend_store.store(
            DataPackage(
                granularity, timestamp, trend_names, update_data_rows
            )
        ).run(conn)
        conn.commit()

        query = table.select([Column("modified")])

        query.execute(cursor)
        modified_list = [modified for modified in cursor.fetchall()]
        assert_not_equal(modified_list[0], modified_list[1])

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        modified_table.select(Column("end")).where_(
            Eq(Column("trendstore_id"), trend_store.id)
        ).execute(cursor)

        end = first(cursor.fetchone())

        eq_(end, max_modified)


@with_conn(clear_database)
def test_update(conn):
    trend_names = ["CellID", "CCR", "Drops"]
    data_rows = [
        (10023, ("10023", "0.9919", "17")),
        (10047, ("10047", "0.9963", "18"))
    ]
    data_types = extract_data_types(data_rows)
    update_data_rows = [(10023, ("10023", "0.5555", "17"))]
    timestamp = datetime.now()
    granularity = create_granularity("900")

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name("test-type001")(cursor)

        trend_descriptors = [
            TrendDescriptor(trend_name, data_type, '')
            for trend_name, data_type in zip(trend_names, data_types)
        ]

        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_source, entity_type, granularity, trend_descriptors, 86400
        ))(cursor)

        partition = trend_store.partition(timestamp)
        partition.create(cursor)

        table = partition.table()

    trend_store.store(
        DataPackage(granularity, timestamp, trend_names, data_rows)
    ).run(conn)

    trend_store.store(
        DataPackage(granularity, timestamp, trend_names, update_data_rows)
    ).run(conn)

    conn.commit()

    query = table.select([Column("modified"), Column("CCR")])

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)
        rows = cursor.fetchall()

    assert_not_equal(rows[0][0], rows[1][0])
    assert_not_equal(rows[0][1], rows[1][1])


@with_conn(clear_database)
def test_update_and_modify_columns_fractured(conn):
    granularity = create_granularity("900")
    timestamp = pytz.utc.localize(datetime(2013, 1, 2, 10, 45, 0))
    entity_ids = range(1023, 1023 + 100)

    trend_names_a = ["CellID", "CCR", "Drops"]
    data_rows_a = [(i, ("10023", "0.9919", "17")) for i in entity_ids]
    data_types_a = extract_data_types(data_rows_a)

    trend_names_b = ["CellID", "Drops"]
    data_rows_b = [(i, ("10023", "19")) for i in entity_ids]
    data_types_b = extract_data_types(data_rows_b)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name("test-type001")(cursor)

        trend_descriptors = [
            TrendDescriptor(trend_name, data_type, '')
            for trend_name, data_type in zip(trend_names_a, data_types_a)
        ]

        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_source, entity_type, granularity, trend_descriptors, 86400
        ))(cursor)

        partition = trend_store.partition(timestamp)
        partition.create(cursor)

        table = partition.table()

        conn.commit()

    trend_store.store(
        DataPackage(
            granularity, timestamp, trend_names_a, data_rows_a
        )
    ).run(conn)
    time.sleep(0.2)

    check_columns = map(Column, ["modified", "Drops"])
    query = table.select(check_columns)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)
        row_before = cursor.fetchone()

    trend_store.store(
        DataPackage(
            granularity, timestamp, trend_names_b, data_rows_b
        )
    ).run(conn)

    query = table.select(check_columns)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)
        row_after = cursor.fetchone()

    assert_not_equal(row_before[0], row_after[0])
    assert_not_equal(row_before[1], row_after[1])
