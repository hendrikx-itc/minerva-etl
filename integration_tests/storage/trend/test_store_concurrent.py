# -*- coding: utf-8 -*-
"""Test concurrent storing of trend data using multiple threads."""
from contextlib import closing
from time import sleep
from threading import Thread
from functools import partial
from datetime import datetime

import pytz

from minerva.storage.trend.trendstorepart import TrendStorePart
from minerva.test.trend import refined_package_type_for_entity_type
from minerva.util import head
from minerva.directory import EntityType, DataSource
from minerva.test import connect, with_conn, clear_database
from minerva.storage import datatype
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.datapackage import DataPackage
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.trend import Trend


def query(sql):
    def f(cursor, *args):
        cursor.execute(sql, args)

    return f


drop_table = query("DROP TABLE IF EXISTS concurrency_test")


create_table = query(
    "CREATE TABLE concurrency_test("
    "id integer, "
    "timestamp timestamp with time zone"
    ")"
)


add_initial_timestamp = query(
    "INSERT INTO concurrency_test (id, timestamp) "
    "VALUES (1, now()) RETURNING timestamp"
)


get_timestamp = query("SELECT timestamp FROM concurrency_test WHERE id = 1")


update_timestamp = query(
    "UPDATE concurrency_test "
    "SET timestamp = greatest(timestamp, %s) "
    "WHERE id = 1"
)


@with_conn()
def store_batch(conn, trend_store, data_package, job_id: int):
    trend_store.store(data_package, job_id)(conn)


def test_store_concurrent(start_db_container):
    """
    Concurrent storing of the same dataset should cause no problems.
    """
    conn = clear_database(start_db_container)
    timestamp = pytz.utc.localize(datetime(2013, 8, 27, 18, 0, 0))

    trend_descriptors = [
        Trend.Descriptor('c1', datatype.registry['smallint'], ''),
        Trend.Descriptor('c2', datatype.registry['smallint'], ''),
        Trend.Descriptor('c3', datatype.registry['smallint'], ''),
    ]

    rows = [
        (i, timestamp, ("1", "2", "3"))
        for i in range(100)
    ]

    granularity = create_granularity("900s")

    entity_type_name = "test_type"

    data_package_type = refined_package_type_for_entity_type(entity_type_name)

    data_package = DataPackage(
        data_package_type, granularity, trend_descriptors, rows
    )

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-source")(cursor)
        entity_type = EntityType.from_name(entity_type_name)(cursor)
        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity, [
                TrendStorePart.Descriptor(
                    'test-trend-store',
                    [
                        Trend.Descriptor('c1', datatype.registry['smallint'], ''),
                        Trend.Descriptor('c2', datatype.registry['smallint'], ''),
                        Trend.Descriptor('c3', datatype.registry['smallint'], '')
                    ]
                )
            ],
            86400
        ))(cursor)

        trend_store.create_partitions_for_timestamp(conn, timestamp)

    conn.commit()

    threads = [
        Thread(
            target=partial(
                store_batch, trend_store, data_package, 10
            )
        ),
        Thread(
            target=partial(
                store_batch, trend_store, data_package, 11
            )
        ),
        Thread(
            target=partial(
                store_batch, trend_store, data_package, 12
            )
        ),
        Thread(
            target=partial(
                store_batch, trend_store, data_package, 13
            )
        )
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()


def test_store_copy_from():
    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            drop_table(cursor)
            create_table(cursor)
            add_initial_timestamp(cursor)
            initial_timestamp = get_scalar(cursor)

        conn.commit()

    print(f"initial timestamp: {initial_timestamp}")

    task1 = start_after(0, task("task 1", 4))
    thread1 = Thread(target=task1)
    thread1.start()

    task2 = start_after(1, task("task 2", 1))
    thread2 = Thread(target=task2)
    thread2.start()

    thread1.join()
    thread2.join()

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            get_timestamp(cursor)
            final_timestamp = get_scalar(cursor)

    print(f"final timestamp: {final_timestamp}")


def task(name, duration):
    def f():
        print(f"{name} start")

        with closing(connect()) as conn:
            with closing(conn.cursor()) as cursor:
                timestamp = now(cursor)
                print(f"{name} timestamp: {timestamp}")

                sleep(duration)

                update_timestamp(cursor, timestamp)

            conn.commit()

        print(f"{name} commit")

    return f


def start_after(t, fn):
    def f():
        sleep(t)

        fn()

    return f


def now(cursor):
    cursor.execute("SELECT now()")

    return get_scalar(cursor)


def get_scalar(cursor):
    return head(cursor.fetchone())
