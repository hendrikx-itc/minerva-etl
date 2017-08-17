# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing
from time import sleep
from threading import Thread
from functools import partial

from minerva.util import head
from minerva.directory.helpers_v4 import name_to_entitytype, name_to_datasource
from minerva.test import connect, with_conn, with_dataset
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.test import DataSet
from minerva.storage.trend.rawdatapackage import RawDataPackage
from minerva.storage.trend.trendstore import store_raw

from minerva_db import clear_database


def query(sql):
    def f(cursor, *args):
        cursor.execute(sql, args)

    return f


drop_table = query(
    "DROP TABLE IF EXISTS concurrency_test")


create_table = query(
    "CREATE TABLE concurrency_test("
    "id integer, "
    "timestamp timestamp with time zone"
    ")")


add_initial_timestamp = query(
    "INSERT INTO concurrency_test (id, timestamp) "
    "VALUES (1, now()) RETURNING timestamp")


get_timestamp = query(
    "SELECT timestamp FROM concurrency_test WHERE id = 1")


update_timestamp = query(
    "UPDATE concurrency_test "
    "SET timestamp = greatest(timestamp, %s) "
    "WHERE id = 1")


@with_conn()
def store_raw_batch(conn, datasource, raw_datapackage):
    txn = store_raw(datasource, raw_datapackage)
    txn.run(conn)


class TestData(DataSet):
    def __init__(self):
        self.granularity = create_granularity("900")

    def load(self, cursor):
        self.datasource = name_to_datasource(cursor, "test-source")
        self.entitytype = name_to_entitytype(cursor, "test_type")


@with_conn(clear_database)
@with_dataset(TestData)
def test_store_concurrent(conn, dataset):
    """
    Concurrent storing of the same dataset should cause no problems.
    """
    timestamp = "2013-08-27T18:00:00"

    trend_names = ["c1", "c2", "c3"]
    rows = [("Cell={}".format(i), ("1", "2", "3")) for i in range(100)]

    raw_datapackage = RawDataPackage(dataset.granularity, timestamp, trend_names, rows)

    threads = [
            Thread(target=partial(store_raw_batch, dataset.datasource, raw_datapackage)),
            Thread(target=partial(store_raw_batch, dataset.datasource, raw_datapackage)),
            Thread(target=partial(store_raw_batch, dataset.datasource, raw_datapackage)),
            Thread(target=partial(store_raw_batch, dataset.datasource, raw_datapackage))]

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
