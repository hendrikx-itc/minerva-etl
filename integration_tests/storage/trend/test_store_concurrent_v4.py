# -*- coding: utf-8 -*-
from contextlib import closing
from time import sleep
from threading import Thread
from functools import partial
import unittest

from minerva.util import head
from minerva.directory.helpers import name_to_entity_type, name_to_data_source
from minerva.test import connect, with_conn
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.test import DataSet
from minerva.storage.trend.datapackage import DefaultPackage

from .minerva_db import clear_database, with_data_context


def query(sql):
    def f(cursor, *args):
        cursor.execute(sql, args)

    return f


drop_table = query(
    "DROP TABLE IF EXISTS concurrency_test"
)


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
        self.data_source = None
        self.entity_type = None

    def load(self, cursor):
        self.data_source = name_to_data_source(cursor, "test-source")
        self.entity_type = name_to_entity_type(cursor, "test_type")


class TestStoreConcurrent(unittest.TestCase):
    def setUp(self):
        self.conn = clear_database(connect())

    def tearDown(self):
        self.conn.close()

    def test_store_concurrent(self):
        """
        Concurrent storing of the same dataset should cause no problems.
        """
        with with_data_context(self.conn, TestData) as data_set:
            timestamp = "2013-08-27T18:00:00"

            trend_names = ["c1", "c2", "c3"]
            rows = [("Cell={}".format(i), ("1", "2", "3")) for i in range(100)]

            raw_data_package = DefaultPackage(
                data_set.granularity, timestamp, trend_names, rows
            )

            threads = [
                Thread(
                    target=partial(store_raw_batch, data_set.datasource, raw_data_package)
                ),
                Thread(target=partial(store_raw_batch, data_set.datasource, raw_data_package)),
                Thread(target=partial(store_raw_batch, data_set.datasource, raw_data_package)),
                Thread(target=partial(store_raw_batch, data_set.datasource, raw_data_package))
            ]

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()

    def test_store_copy_from(self):
        with closing(connect()) as conn:
            with closing(conn.cursor()) as cursor:
                drop_table(cursor)
                create_table(cursor)
                add_initial_timestamp(cursor)
                initial_timestamp = get_scalar(cursor)

            conn.commit()

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


def task(name, duration):
    def f():
        with closing(connect()) as conn:
            with closing(conn.cursor()) as cursor:
                timestamp = now(cursor)

                sleep(duration)

                update_timestamp(cursor, timestamp)

            conn.commit()

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
