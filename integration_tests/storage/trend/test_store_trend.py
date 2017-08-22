import time
from contextlib import closing
from pytz import timezone
from datetime import datetime, timedelta

from nose.tools import eq_, raises, assert_not_equal

from minerva.util import first
from minerva.db.query import Table, Column, Call, Eq
from minerva.directory.helpers_v4 import name_to_datasource, name_to_entitytype
from minerva.storage.generic import extract_data_types
from minerva.test import with_conn
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.storage import DataTypeMismatch, \
    create_trend_table, store_copy_from, store_using_tmp, \
    store_insert_rows, store, create_temp_table_from

from minerva_db import clear_database
from helpers import row_count

SCHEMA = 'trend'

modified_table = Table(SCHEMA, "modified")


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

    curr_timezone = timezone("Europe/Amsterdam")
    data_types = extract_data_types(data_rows)
    timestamp = curr_timezone.localize(datetime(2013, 1, 2, 10, 45, 0))
    granularity = create_granularity("900")
    modified = curr_timezone.localize(datetime.now())

    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, "test-src009")
        entitytype = name_to_entitytype(cursor, "test-type001")

        trendstore = TrendStore(
            datasource, entitytype,
                granularity, 86400, "table").create(cursor)
        partition = trendstore.partition(timestamp)

        table = partition.table()

        partition.create(cursor)

        partition.check_columns_exist(trend_names, data_types)(cursor)

        store_copy_from(
            conn, SCHEMA, table.name, trend_names,
                timestamp, modified, data_rows)

        conn.commit()

        eq_(row_count(cursor, table), 11)

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        eq_(max_modified, modified)


@raises(DataTypeMismatch)
@with_conn(clear_database)
def test_store_copy_from_2(conn):
    trend_names = ['CCR', 'CCRatts', 'Drops']
    data_rows = [
        (10023, ('0.9919', '2105', '17'))
    ]

    data_types = ['integer', 'smallint', 'smallint']

    curr_timezone = timezone("Europe/Amsterdam")
    timestamp = curr_timezone.localize(datetime(2013, 1, 2, 10, 45, 0))
    modified = curr_timezone.localize(datetime.now())
    granularity = create_granularity("900")

    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, "test-src010")
        entitytype = name_to_entitytype(cursor, "test-type002")
        trendstore = TrendStore(
            datasource, entitytype,
                granularity, 86400, "table").create(cursor)
        partition = trendstore.partition(timestamp)
        partition.create(cursor)
        partition.check_columns_exist(trend_names, data_types)(cursor)
        table = partition.table()

        store_copy_from(
            conn, SCHEMA, table.name, trend_names,
                timestamp, modified, data_rows)

        conn.commit()

        eq_(row_count(cursor, table), 1)

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        eq_(max_modified, modified)


@with_conn(clear_database)
def test_store_using_tmp(conn):
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

    with closing(conn.cursor()) as cursor:
        table.drop().if_exists().execute(cursor)

        create_trend_table(conn, SCHEMA, table.name, trend_names, data_types)
        curr_timezone = timezone("Europe/Amsterdam")
        timestamp = curr_timezone.localize(datetime(2013, 1, 2, 10, 45, 0))
        modified = curr_timezone.localize(datetime.now())
        store_using_tmp(
            conn, SCHEMA, table.name, trend_names,
                timestamp, modified, data_rows)

        conn.commit()

        eq_(row_count(cursor, table), 10)

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        eq_(max_modified, modified)


@with_conn(clear_database)
def test_store_insert_rows(conn):
    table = Table(SCHEMA, 'storage_tmp_test_table')
    trend_names = ['CellID', 'CCR', 'Drops']
    data_rows = [
        (10023, ('10023', '0.9919', '17')),
        (10047, ('10047', '0.9963', '18'))
    ]
    curr_timezone = timezone("Europe/Amsterdam")
    modified = curr_timezone.localize(datetime.now())
    time1 = curr_timezone.localize(datetime.now())
    time2 = time1 - timedelta(days=1)

    data_types = extract_data_types(data_rows)

    with closing(conn.cursor()) as cursor:
        table.drop().if_exists().execute(cursor)

        create_trend_table(conn, SCHEMA, table.name, trend_names, data_types)

        store_insert_rows(
            conn, SCHEMA, table.name, trend_names, time1, modified,
                data_rows)
        conn.commit()

        eq_(row_count(cursor, table), 2)

        store_insert_rows(
            conn, SCHEMA, table.name, trend_names, time2, modified,
                data_rows)
        conn.commit()

        eq_(row_count(cursor, table), 4)

        store_insert_rows(
            conn, SCHEMA, table.name, trend_names, time1, modified,
                data_rows)
        conn.commit()

        eq_(row_count(cursor, table), 4)

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        eq_(max_modified, modified)


@with_conn(clear_database)
def test_update_modified_column(conn):
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

    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, "test-src009")
        entitytype = name_to_entitytype(cursor, "test-type001")

        trendstore = TrendStore(
            datasource, entitytype,
                granularity, 86400, "table").create(cursor)
        partition = trendstore.partition(timestamp)

        table = partition.table()

        partition.create(cursor)

        partition.check_columns_exist(trend_names, data_types)(cursor)

        store(conn, SCHEMA, table.name, trend_names, timestamp, data_rows)
        time.sleep(1)
        store(
            conn, SCHEMA, table.name, trend_names,
                timestamp, update_data_rows)
        conn.commit()

        query = table.select([Column("modified")])

        query.execute(cursor)
        modified_list = [modified for modified in cursor.fetchall()]
        assert_not_equal(modified_list[0], modified_list[1])

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        modified_table.select(Column("end")).where_(
            Eq(Column("table_name"), table.name)).execute(cursor)

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
        datasource = name_to_datasource(cursor, "test-src009")
        entitytype = name_to_entitytype(cursor, "test-type001")

        trendstore = TrendStore(
            datasource, entitytype,
                granularity, 86400, "table").create(cursor)
        partition = trendstore.partition(timestamp)

        table = partition.table()

        partition.create(cursor)

        partition.check_columns_exist(trend_names, data_types)(cursor)

    store(conn, SCHEMA, table.name, trend_names, timestamp, data_rows)

    store(conn, SCHEMA, table.name, trend_names, timestamp, update_data_rows)
    conn.commit()

    query = table.select([Column("modified"), Column("CCR")])

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)
        rows = cursor.fetchall()

    assert_not_equal(rows[0][0], rows[1][0])
    assert_not_equal(rows[0][1], rows[1][1])


@with_conn(clear_database)
def test_create_temp_table_from(conn):
    table = Table(SCHEMA, "storage_tmp_test_table")
    trend_names = ["CellID", "CCR", "Drops"]
    data_types = ["float", "smallint", "smallint"]

    with closing(conn.cursor()) as cursor:
        table.drop().if_exists().execute(cursor)

        create_trend_table(conn, SCHEMA, table, trend_names, data_types)

        create_temp_table_from(conn, SCHEMA, table)


@with_conn(clear_database)
def test_update_and_modify_columns_fractured(conn):
    curr_timezone = timezone("Europe/Amsterdam")
    granularity = create_granularity("900")
    timestamp = curr_timezone.localize(datetime(2013, 1, 2, 10, 45, 0))
    entity_ids = range(1023, 1023 + 100)

    trend_names_a = ["CellID", "CCR", "Drops"]
    data_rows_a = [(i, ("10023", "0.9919", "17")) for i in entity_ids]
    data_types_a = extract_data_types(data_rows_a)

    trend_names_b = ["CellID", "Drops"]
    data_rows_b = [(i, ("10023", "19")) for i in entity_ids]
    # data_types_b = extract_data_types(data_rows_b)

    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, "test-src009")
        entitytype = name_to_entitytype(cursor, "test-type001")

        trendstore = TrendStore(
            datasource, entitytype,
                granularity, 86400, "table").create(cursor)
        partition = trendstore.partition(timestamp)

        table = partition.table()

        partition.create(cursor)

        partition.check_columns_exist(trend_names_a, data_types_a)(cursor)
        conn.commit()

    store(conn, SCHEMA, table.name, trend_names_a, timestamp, data_rows_a)
    time.sleep(0.2)

    check_columns = map(Column, ["modified", "Drops"])
    query = table.select(check_columns)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)
        row_before = cursor.fetchone()

    store(conn, SCHEMA, table.name, trend_names_b, timestamp, data_rows_b)

    query = table.select(check_columns)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)
        row_after = cursor.fetchone()

    assert_not_equal(row_before[0], row_after[0])
    assert_not_equal(row_before[1], row_after[1])
