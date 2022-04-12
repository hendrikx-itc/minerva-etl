import time
from datetime import datetime, timedelta

from minerva.storage import DataPackage, datatype
from minerva.storage.trend.trend import Trend
from minerva.test.trend import refined_package_type_for_entity_type
from pytz import timezone

import pytest

from minerva.db.error import DataTypeMismatch
from minerva.storage.trend.trendstorepart import TrendStorePart
from minerva.util import first
from minerva.db.query import Table, Column, Call
from minerva.directory.datasource import DataSource
from minerva.directory.entitytype import EntityType
from minerva.test import clear_database, row_count
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.granularity import create_granularity


def test_store_copy_from_1(start_db_container):
    conn = clear_database(start_db_container)

    trends = [
        Trend.Descriptor('CellID', datatype.registry['integer'], ''),
        Trend.Descriptor('CCR', datatype.registry['numeric'], ''),
        Trend.Descriptor('CCRatts', datatype.registry['integer'], ''),
        Trend.Descriptor('Drops', datatype.registry['integer'], ''),
    ]

    curr_timezone = timezone("Europe/Amsterdam")
    timestamp = curr_timezone.localize(datetime(2013, 1, 2, 10, 45, 0))

    data_rows = [
        (10023, timestamp, ('10023', '0.9919', '2105', '17')),
        (10047, timestamp, ('10047', '0.9963', '4906', '18')),
        (10048, timestamp, ('10048', '0.9935', '2448', '16')),
        (10049, timestamp, ('10049', '0.9939', '5271', '32')),
        (10050, timestamp, ('10050', '0.9940', '3693', '22')),
        (10051, timestamp, ('10051', '0.9944', '3753', '21')),
        (10052, timestamp, ('10052', '0.9889', '2168', '24')),
        (10053, timestamp, ('10053', '0.9920', '2372', '19')),
        (10085, timestamp, ('10085', '0.9987', '2282', '3')),
        (10086, timestamp, ('10086', '0.9972', '1763', '5')),
        (10087, timestamp, ('10087', '0.9931', '1453', '10'))
    ]

    granularity = create_granularity("900s")
    modified = curr_timezone.localize(datetime.now())
    partition_size = timedelta(seconds=86400)
    entity_type_name = "Cell"

    with conn.cursor() as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name(entity_type_name)(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity, [
                TrendStorePart.Descriptor('test_store', trends)
            ], partition_size
        ))(cursor)

        trend_store.create_partitions_for_timestamp(conn, timestamp)

        data_package_type = refined_package_type_for_entity_type(entity_type_name)

        data_package = DataPackage(data_package_type, granularity, trends, data_rows)

        part = trend_store.part_by_name['test_store']
        job_id = 1
        part.store_copy_from(data_package, modified, job_id)(cursor)

        conn.commit()

        table = Table('trend', part.name)

        assert row_count(cursor, table.identifier()) == 11

        table.select(Call("max", Column("job_id"))).execute(cursor)

        max_job_id = first(cursor.fetchone())

        assert max_job_id == job_id


def test_store_copy_from_2(start_db_container):
    conn = clear_database(start_db_container)

    trends = [
        Trend.Descriptor('CCR', datatype.registry['integer'], ''),
        Trend.Descriptor('CCRatts', datatype.registry['integer'], ''),
        Trend.Descriptor('Drops', datatype.registry['integer'], ''),
    ]

    curr_timezone = timezone("Europe/Amsterdam")
    timestamp = curr_timezone.localize(datetime(2013, 1, 2, 10, 45, 0))

    data_rows = [
        (10023, timestamp, ('0.9919', '2105', '17'))
    ]

    modified = curr_timezone.localize(datetime.now())
    granularity = create_granularity('900s')
    partition_size = timedelta(seconds=86400)
    entity_type_name = "test-type002"

    with conn.cursor() as cursor:
        data_source = DataSource.from_name("test-src010")(cursor)
        entity_type = EntityType.from_name(entity_type_name)(cursor)
        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity, [
                TrendStorePart.Descriptor('test_store', trends)
            ], partition_size
        ))(cursor)

        trend_store.create_partitions_for_timestamp(conn, timestamp)

        table = Table('trend', 'test_store')

        data_package_type = refined_package_type_for_entity_type(entity_type_name)

        data_package = DataPackage(data_package_type, granularity, trends, data_rows)

        trend_store_part = trend_store.part_by_name['test_store']

        conn.commit()

        with pytest.raises(DataTypeMismatch):
            trend_store_part.store_copy_from(data_package, modified, 1)(cursor)

        conn.rollback()

        assert row_count(cursor, table.identifier()) == 0


def test_update_modified_column(start_db_container):
    conn = clear_database(start_db_container)

    curr_timezone = timezone("Europe/Amsterdam")

    trends = [
        Trend.Descriptor('CellID', datatype.registry['integer'], ''),
        Trend.Descriptor('CCR', datatype.registry['numeric'], ''),
        Trend.Descriptor('DROPS', datatype.registry['integer'], ''),
    ]

    timestamp = curr_timezone.localize(datetime.now())

    data_rows = [
        (10023, timestamp, ('10023', '0.9919', '17')),
        (10047, timestamp, ('10047', '0.9963', '18'))
    ]

    update_data_rows = [(10023, timestamp, ('10023', '0.9919', '17'))]
    granularity = create_granularity("900s")
    partition_size = timedelta(seconds=86400)
    entity_type_name = "test-type001"

    with conn.cursor() as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name(entity_type_name)(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity, [
                TrendStorePart.Descriptor('test-store', trends)
            ], partition_size
        ))(cursor)

        trend_store.create_partitions_for_timestamp(conn, timestamp)

        conn.commit()

        table = Table('trend', 'test-store')

        data_package_type = refined_package_type_for_entity_type(entity_type_name)
        data_package = DataPackage(
            data_package_type, granularity, trends, data_rows
        )

        job_id = 10

        trend_store.store(data_package, job_id)(conn)
        time.sleep(1)
        data_package = DataPackage(
            data_package_type, granularity, trends, update_data_rows
        )

        job_id = 11
        trend_store.store(data_package, job_id)(conn)
        conn.commit()

        query = table.select([Column("job_id")])

        query.execute(cursor)
        job_id_list = [job_id for job_id, in cursor.fetchall()]

        assert job_id_list[0] != job_id_list[1]


def test_update(start_db_container):
    conn = clear_database(start_db_container)

    trend_descriptors = [
        Trend.Descriptor("CellID", datatype.registry['integer'], ''),
        Trend.Descriptor("CCR", datatype.registry['numeric'], ''),
        Trend.Descriptor("Drops", datatype.registry['integer'], ''),
    ]

    timestamp = datetime.now()

    data_rows = [
        (10023, timestamp, ("10023", "0.9919", "17")),
        (10047, timestamp, ("10047", "0.9963", "18"))
    ]
    update_data_rows = [(10023, timestamp, ("10023", "0.5555", "17"))]
    granularity = create_granularity("900s")
    partition_size = timedelta(seconds=86400)
    entity_type_name = "test-type001"

    with conn.cursor() as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name(entity_type_name)(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity, [
                TrendStorePart.Descriptor('test-store', trend_descriptors)
            ], partition_size
        ))(cursor)

    trend_store.create_partitions_for_timestamp(conn, timestamp)

    table = Table('trend', 'test-store')

    data_package_type = refined_package_type_for_entity_type(entity_type_name)
    data_package = DataPackage(
        data_package_type, granularity, trend_descriptors, data_rows
    )

    job_id = 10

    trend_store.store(data_package, job_id)(conn)

    data_package = DataPackage(
        data_package_type, granularity, trend_descriptors, update_data_rows
    )

    job_id = 11

    trend_store.store(data_package, job_id)(conn)
    conn.commit()

    query = table.select([Column("job_id"), Column("CCR")])

    with conn.cursor() as cursor:
        query.execute(cursor)
        rows = cursor.fetchall()

    assert rows[0][0] != rows[1][0]
    assert rows[0][1] != rows[1][1]
