# -*- coding: utf-8 -*-
from contextlib import closing
from time import sleep
from threading import Thread
from functools import partial
from datetime import datetime

import pytz

from minerva.util import head
from minerva.directory import EntityType, DataSource
from minerva.test import connect, with_conn, clear_database
from minerva.storage import datatype
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.datapackage import DefaultPackage
from minerva.storage.trend.trendstore import TableTrendStore
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
def store_batch(conn, trend_store, data_package):
    trend_store.store(data_package).run(conn)


@with_conn(clear_database)
def test_store_concurrent(conn):
    """
    Concurrent storing of the same dataset should cause no problems.
    """
    timestamp = pytz.utc.localize(datetime(2013, 8, 27, 18, 0, 0))

    trend_names = ["c1", "c2", "c3"]
    rows = [
        ("Cell={}".format(i), ("1", "2", "3"))
        for i in range(100)
    ]

    granularity = create_granularity("900")

    data_package = DefaultPackage(
        granularity, timestamp, trend_names, rows
    )

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-source")(cursor)
        entity_type = EntityType.from_name("test_type")(cursor)
        trend_store = TableTrendStore.create(TableTrendStore.Descriptor(
            'test-trend-store', data_source, entity_type, granularity, [
                Trend.Descriptor('c1', datatype.registry['smallint'], ''),
                Trend.Descriptor('c2', datatype.registry['smallint'], ''),
                Trend.Descriptor('c3', datatype.registry['smallint'], '')
            ],
            86400
        ))(cursor)

        trend_store.partition(timestamp).create(cursor)

    conn.commit()

    threads = [
        Thread(
            target=partial(
                store_batch, trend_store, data_package
            )
        ),
        Thread(
            target=partial(
                store_batch, trend_store, data_package
            )
        ),
        Thread(
            target=partial(
                store_batch, trend_store, data_package
            )
        ),
        Thread(
            target=partial(
                store_batch, trend_store, data_package
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

    print("initial timestamp: {}".format(initial_timestamp))

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

    print("final timestamp: {}".format(final_timestamp))


def task(name, duration):
    def f():
        print("{} start".format(name))

        with closing(connect()) as conn:
            with closing(conn.cursor()) as cursor:
                timestamp = now(cursor)
                print("{} timestamp: {}".format(name, timestamp))

                sleep(duration)

                update_timestamp(cursor, timestamp)

            conn.commit()

        print("{} commit".format(name))

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
