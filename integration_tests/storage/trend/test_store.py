import time
from datetime import timedelta, datetime
from contextlib import closing
from functools import partial
from operator import contains
import datetime

import pytz

import pytest

from psycopg2 import sql

from minerva.db.query import Table, Call, Column, Eq, And
from minerva.db.error import DataTypeMismatch
from minerva.storage.trend.trendstorepart import TrendStorePart
from minerva.test import clear_database
from minerva.directory import DataSource, EntityType
from minerva.storage import datatype, DataPackage
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.trend import Trend
from minerva.storage.trend.granularity import create_granularity

from minerva.test.trend import refined_package_type_for_entity_type

modified_table = Table("trend_directory", "modified")


def test_store_copy_from_1(start_db_container):
    conn = clear_database(start_db_container)

    trend_descriptors = [
        Trend.Descriptor('CellID', datatype.registry['integer'], ''),
        Trend.Descriptor('CCR', datatype.registry['double precision'], ''),
        Trend.Descriptor('CCRatts', datatype.registry['smallint'], ''),
        Trend.Descriptor('Drops', datatype.registry['smallint'], '')
    ]

    trend_store_part_descr = TrendStorePart.Descriptor(
        'test-trend-store', trend_descriptors
    )

    timestamp = pytz.utc.localize(datetime.datetime(2013, 1, 2, 10, 45, 0))

    data_rows = [
        (10023, timestamp, (10023, 0.9919, 2105, 17)),
        (10047, timestamp, (10047, 0.9963, 4906, 18)),
        (10048, timestamp, (10048, 0.9935, 2448, 16)),
        (10049, timestamp, (10049, 0.9939, 5271, 32)),
        (10050, timestamp, (10050, 0.9940, 3693, 22)),
        (10051, timestamp, (10051, 0.9944, 3753, 21)),
        (10052, timestamp, (10052, 0.9889, 2168, 24)),
        (10053, timestamp, (10053, 0.9920, 2372, 19)),
        (10085, timestamp, (10085, 0.9987, 2282, 3)),
        (10086, timestamp, (10086, 0.9972, 1763, 5)),
        (10087, timestamp, (10087, 0.9931, 1453, 10))
    ]

    partition_size = timedelta(seconds=86400)
    granularity = create_granularity("900s")
    modified = pytz.utc.localize(datetime.datetime.now())
    entity_type_name = "test-type001"

    data_package_type = refined_package_type_for_entity_type(entity_type_name)

    data_package = DataPackage(data_package_type, granularity, trend_descriptors, data_rows)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name(entity_type_name)(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity,
            [trend_store_part_descr], partition_size
        ))(cursor)

        trend_store.create_partitions_for_timestamp(conn, timestamp)

        table = Table('trend', trend_store_part_descr.name)

        trend_store_part = trend_store.parts[0]

        job_id = 42

        trend_store_part.store_copy_from(data_package, modified, job_id)(cursor)

        assert row_count(cursor, table) == 11

        table.select(Call("max", Column("job_id"))).execute(cursor)

        max_job_id, = cursor.fetchone()

        assert max_job_id == job_id


def test_store_copy_from_2(start_db_container):
    conn = clear_database(start_db_container)

    trend_descriptors = [
        Trend.Descriptor(
            'CCR', datatype.registry['integer'], ''
        ),
        Trend.Descriptor(
            'CCRatts', datatype.registry['smallint'], ''
        ),
        Trend.Descriptor(
            'Drops', datatype.registry['smallint'], ''
        ),
    ]

    timestamp = pytz.utc.localize(datetime.datetime(2013, 1, 2, 10, 45, 0))

    data_rows = [
        (10023, timestamp, (0.9919, 2105, 17))
    ]

    granularity = create_granularity("900s")
    partition_size = timedelta(seconds=86400)
    entity_type_name = "test-type002"

    data_package_type = refined_package_type_for_entity_type(entity_type_name)
    data_package = DataPackage(
        data_package_type, granularity, trend_descriptors, data_rows
    )

    with conn.cursor() as cursor:
        data_source = DataSource.from_name("test-src010")(cursor)
        entity_type = EntityType.from_name(entity_type_name)(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity,
            [TrendStorePart.Descriptor(
                'test-trend-store', trend_descriptors
            )], partition_size
        ))(cursor)

    trend_store.create_partitions_for_timestamp(conn, timestamp)
    conn.commit()

    with pytest.raises(DataTypeMismatch):
        trend_store.store(data_package, {'job': 'test-job'})(conn)

    conn.rollback()


def test_update_modified_column(start_db_container):
    conn = clear_database(start_db_container)

    trend_descriptors = [
        Trend.Descriptor('CellID', datatype.registry['smallint'], ''),
        Trend.Descriptor('CCR', datatype.registry['double precision'], ''),
        Trend.Descriptor('Drops', datatype.registry['smallint'], ''),
    ]

    timestamp = pytz.utc.localize(datetime.datetime.now())

    data_rows = [
        (10023, timestamp, (10023, 0.9919, 17)),
        (10047, timestamp, (10047, 0.9963, 18))
    ]

    update_data_rows = [
        (10023, timestamp, (10023, 0.9919, 17))
    ]

    granularity = create_granularity("900s")
    partition_size = timedelta(seconds=86400)
    entity_type_name = "test-type001"

    with conn.cursor() as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name(entity_type_name)(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type,
            granularity, [TrendStorePart.Descriptor(
                'test-trend-store', trend_descriptors
            )], partition_size
        ))(cursor)

        trend_store.create_partitions_for_timestamp(conn, timestamp)

        conn.commit()

        table = Table('trend', 'test-trend-store')

        data_package_type = refined_package_type_for_entity_type(entity_type_name)

        data_package = DataPackage(data_package_type, granularity, trend_descriptors, data_rows)

        trend_store.store(
            data_package, {'job': 'test-job'}
        )(conn)

        time.sleep(1)

        data_package = DataPackage(data_package_type, granularity, trend_descriptors, update_data_rows)

        trend_store.store(
            data_package, {'job': 'test-job'}
        )(conn)
        conn.commit()

        query = table.select([Column("job_id")])

        query.execute(cursor)
        job_id_list = [job_id for job_id, in cursor.fetchall()]

        assert job_id_list[0] != job_id_list[1]

        trend_store_part = trend_store.part_by_name['test-trend-store']

        query = Table('trend_directory', 'modified_log').select([Column("modified")]).where_(
            Eq(Column("trend_store_part_id"), trend_store_part.id)
        )
        query.execute(cursor)
        modified_list = [modified for modified, in cursor.fetchall()]

        max_modified = max(modified_list)

        # The modified table is no longer directly populated, but indirectly
        # by the processing of the modified_log table.

        cursor.execute('select * from trend_directory.process_modified_log()')

        modified_table.select(Column("last")).where_(
            Eq(Column("trend_store_part_id"), trend_store_part.id)
        ).execute(cursor)

        end, = cursor.fetchone()

        assert end == max_modified


def test_update(start_db_container):
    conn = clear_database(start_db_container)

    trend_descriptors = [
        Trend.Descriptor("CellID", datatype.registry['smallint'], ''),
        Trend.Descriptor("CCR", datatype.registry['double precision'], ''),
        Trend.Descriptor("Drops", datatype.registry['smallint'], '')
    ]

    timestamp = datetime.datetime.now()

    data_rows = [
        (10023, timestamp, (10023, 0.9919, 17)),
        (10047, timestamp, (10047, 0.9963, 18)),
    ]

    update_data_rows = [
        (10023, timestamp, (10023, 0.5555, 17)),
    ]

    granularity = create_granularity("900s")
    partition_size = timedelta(seconds=86400)
    entity_type_name = "test-type001"

    with conn.cursor() as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name(entity_type_name)(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity,
            [TrendStorePart.Descriptor(
                'test-trend-store', trend_descriptors
            )], partition_size
        ))(cursor)

    trend_store.create_partitions_for_timestamp(conn, timestamp)

    table = Table('trend', 'test-trend-store')

    data_package_type = refined_package_type_for_entity_type(entity_type_name)

    data_package = DataPackage(data_package_type, granularity, trend_descriptors, data_rows)

    trend_store.store(
        data_package, {'job': 'test-job'}
    )(conn)

    data_package = DataPackage(data_package_type, granularity, trend_descriptors, update_data_rows)

    trend_store.store(
        data_package, {'job': 'test-job'}
    )(conn)

    conn.commit()

    query = table.select([Column("job_id"), Column("CCR")])

    with conn.cursor() as cursor:
        query.execute(cursor)
        rows = cursor.fetchall()

    assert rows[0][0] != rows[1][0]
    assert rows[0][1] != rows[1][1]


def test_update_and_modify_columns_fractured(start_db_container):
    conn = clear_database(start_db_container)

    granularity = create_granularity("900s")
    timestamp = pytz.utc.localize(datetime.datetime(2013, 1, 2, 10, 45, 0))
    entity_ids = range(1023, 1023 + 100)

    trend_descriptors_a = [
        Trend.Descriptor("CellID", datatype.registry['smallint'], ''),
        Trend.Descriptor("CCR", datatype.registry['double precision'], ''),
        Trend.Descriptor("Drops", datatype.registry['smallint'], ''),
    ]

    data_rows_a = [
        (i, timestamp, (10023, 0.9919, 17))
        for i in entity_ids
    ]

    trend_descriptors_b = [
        Trend.Descriptor("CellID", datatype.registry['smallint'], ''),
        Trend.Descriptor("Drops", datatype.registry['smallint'], ''),
    ]
    data_rows_b = [
        (i, timestamp, (10023, 19))
        for i in entity_ids
    ]

    entity_type_name = "test-type001"
    partition_size = timedelta(seconds=86400)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name(entity_type_name)(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity,
            [TrendStorePart.Descriptor(
                'test-trend-store', trend_descriptors_a
            )], partition_size
        ))(cursor)

        trend_store.create_partitions_for_timestamp(conn, timestamp)

        table = Table('trend', 'test-trend-store')

        conn.commit()

    data_package_type = refined_package_type_for_entity_type(entity_type_name)

    data_package = DataPackage(data_package_type, granularity, trend_descriptors_a, data_rows_a)

    trend_store.store(
        data_package,
        {'job': 'test-job-a'}
    )(conn)
    time.sleep(0.2)

    check_columns = list(map(Column, ["job_id", "Drops"]))
    query = table.select(check_columns)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)
        row_before = cursor.fetchone()

    data_package = DataPackage(data_package_type, granularity, trend_descriptors_b, data_rows_b)

    trend_store.store(
        data_package,
        {'job': 'test-job-b'}
    )(conn)

    query = table.select(check_columns)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)
        row_after = cursor.fetchone()

    assert row_before[0] != row_after[0]
    assert row_before[1] != row_after[1]


def test_create_trend_store(start_db_container):
    conn = clear_database(start_db_container)

    granularity = create_granularity('900 seconds')

    partition_size = timedelta(seconds=3600)

    with closing(conn.cursor()) as cursor:
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


def test_create_trend_store_with_children(start_db_container):
    conn = clear_database(start_db_container)

    granularity = create_granularity('900 seconds')

    partition_size = timedelta(seconds=3600)

    with closing(conn.cursor()) as cursor:
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

        trend_store.create_partitions_for_timestamp(conn, timestamp)


def test_get_trend_store(start_db_container):
    conn = clear_database(start_db_container)

    partition_size = timedelta(seconds=3600)

    with closing(conn.cursor()) as cursor:
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

        assert trend_store.data_source.id == data_source.id
        assert trend_store.partition_size == partition_size
        assert trend_store.id is not None, "trend_store.id is None"


def test_store_copy_from(start_db_container):
    conn = clear_database(start_db_container)

    granularity = create_granularity('900 seconds')

    partition_size = timedelta(seconds=86400)

    trend_descriptors = [
        Trend.Descriptor('a', datatype.registry['smallint'], ''),
        Trend.Descriptor('b', datatype.registry['smallint'], ''),
        Trend.Descriptor('c', datatype.registry['smallint'], '')
    ]

    trend_names = [t.name for t in trend_descriptors]

    timestamp = pytz.utc.localize(
        datetime.datetime(2013, 4, 25, 9, 45)
    )

    entity_type_name = "test_type"

    with conn.cursor() as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create(entity_type_name, '')(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity,
            [TrendStorePart.Descriptor(
                'test-trend-store', trend_descriptors
            )], partition_size
        ))(cursor)

    trend_store.create_partitions_for_timestamp(conn, timestamp)

    conn.commit()

    def make_row(index):
        return 1234 + index, timestamp, [1, 2, 3 + index]

    rows = list(map(make_row, range(100)))

    data_package_type = refined_package_type_for_entity_type(entity_type_name)

    data_package = DataPackage(data_package_type, granularity, trend_descriptors, rows)

    transaction = trend_store.store(data_package, {'job': 'test-job'})
    transaction(conn)

    transaction = trend_store.store(data_package, {'job': 'test-job'})
    transaction(conn)


def test_store_copy_from_missing_column(start_db_container):
    conn = clear_database(start_db_container)

    granularity = create_granularity('900 seconds')

    partition_size = timedelta(seconds=86400)

    trend_descriptors = [
        Trend.Descriptor('a', datatype.registry['smallint'], ''),
        Trend.Descriptor('b', datatype.registry['smallint'], ''),
        Trend.Descriptor('c', datatype.registry['smallint'], ''),
    ]

    timestamp = pytz.utc.localize(
        datetime.datetime(2013, 4, 25, 9, 45)
    )

    entity_type_name = "test_type"

    with conn.cursor() as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create(entity_type_name, '')(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity,
            [TrendStorePart.Descriptor(
                'test-trend-store', trend_descriptors
            )], partition_size
        ))(cursor)

        trend_store.create_partitions_for_timestamp(conn, timestamp)

    conn.commit()

    rows = [
        (1234 + index, timestamp, [1, 2, 3 + index])
        for index in range(100)
    ]

    data_package_type = refined_package_type_for_entity_type(entity_type_name)

    data_package = DataPackage(data_package_type, granularity, trend_descriptors, rows)

    transaction = trend_store.store(data_package, {})
    transaction(conn)

    # Store second part with one column extra

    timestamp = pytz.utc.localize(
        datetime.datetime(2013, 4, 25, 10, 00)
    )

    trend_descriptors = [
        Trend.Descriptor('a', datatype.registry['smallint'], ''),
        Trend.Descriptor('b', datatype.registry['smallint'], ''),
        Trend.Descriptor('c', datatype.registry['smallint'], ''),
        Trend.Descriptor('d', datatype.registry['smallint'], ''),
    ]

    def make_row_y(index):
        return 1234 + index, timestamp, [1, 2, 3, 4 + index]

    rows = list(map(make_row_y, range(100)))

    data_package = DataPackage(data_package_type, granularity, trend_descriptors, rows)

    # Storing should succeed and just store what can be placed in the trend
    # store parts.
    transaction = trend_store.store(data_package, {})
    transaction(conn)


def test_store(start_db_container):
    conn = clear_database(start_db_container)

    granularity = create_granularity('900 seconds')

    partition_size = timedelta(seconds=86400)

    trend_descriptors = [
        Trend.Descriptor('a', datatype.registry['smallint'], ''),
        Trend.Descriptor('b', datatype.registry['smallint'], ''),
        Trend.Descriptor('c', datatype.registry['smallint'], ''),
    ]

    timestamp = pytz.utc.localize(
        datetime.datetime(2013, 4, 25, 9, 45)
    )

    entity_type_name = "test_type"

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create(entity_type_name, '')(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity,
            [
                TrendStorePart.Descriptor(
                    'test-trend-store', trend_descriptors
                )
            ], partition_size
        ))(cursor)

        trend_store.create_partitions_for_timestamp(conn, timestamp)

    conn.commit()

    rows = [
        (1234, timestamp, [1, 2, 3]),
        (2345, timestamp, [4, 5, 6]),
    ]

    data_package_type = refined_package_type_for_entity_type(entity_type_name)

    data_package = DataPackage(data_package_type, granularity, trend_descriptors, rows)

    transaction = trend_store.store(data_package, {})
    transaction(conn)

    table = Table('trend', 'test-trend-store')

    condition = And(
        Eq(Column("entity_id"), 2345),
        Eq(Column("timestamp"), timestamp)
    )

    query = table.select(Column("c")).where_(condition)

    with conn.cursor() as cursor:
        query.execute(cursor)

        c, = cursor.fetchone()

    assert c == 6

    rows = [
        (1234, timestamp, [1, 2, 3]),
        (2345, timestamp, [4, 5, 7]),
    ]

    data_package = DataPackage(data_package_type, granularity, trend_descriptors, rows)

    transaction = trend_store.store(data_package, {})
    transaction(conn)

    with conn.cursor() as cursor:
        query.execute(cursor)

        c, = cursor.fetchone()

    assert c == 7


def test_store_ignore_column(start_db_container):
    conn = clear_database(start_db_container)

    partition_size = timedelta(seconds=86400)
    granularity = create_granularity('900 seconds')

    trend_descriptors = [
        Trend.Descriptor('x', datatype.registry['smallint'], ''),
        Trend.Descriptor('y', datatype.registry['smallint'], ''),
    ]

    timestamp = pytz.utc.localize(datetime.datetime(2013, 4, 25, 10, 45))
    entity_type_name = "test_type"

    with conn.cursor() as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create(entity_type_name, '')(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity,
            [
                TrendStorePart.Descriptor(
                    'test-trend-store', trend_descriptors
                )
            ], partition_size
        ))(cursor)

    trend_store.create_partitions_for_timestamp(conn, timestamp)

    conn.commit()

    data_package_type = refined_package_type_for_entity_type(entity_type_name)

    trend_descriptors = [
        Trend.Descriptor('x', datatype.registry['smallint'], ''),
        Trend.Descriptor('y', datatype.registry['smallint'], ''),
        Trend.Descriptor('z', datatype.registry['smallint'], ''),
    ]

    rows = [
        (1234, timestamp, [1, 2, 3]),
        (2345, timestamp, [4, 5, 6])
    ]

    data_package = DataPackage(data_package_type, granularity, trend_descriptors, rows)

    part = trend_store.part_by_name['test-trend-store']
    trend_names = [t.name for t in part.trends]

    transaction = trend_store.store(
        data_package.filter_trends(partial(contains, set(trend_names))), {}
    )

    transaction(conn)


def row_count(cursor, table: Table):
    query = sql.SQL("SELECT count(*) FROM {}").format(table.identifier())

    cursor.execute(query)

    count, = cursor.fetchone()

    return count
