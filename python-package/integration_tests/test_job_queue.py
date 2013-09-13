from contextlib import closing
import json
from threading import Thread
import time

from nose.tools import eq_, assert_raises
import psycopg2

from minerva_db import with_connection, connect
from minerva.system.jobqueue import enqueue_job, get_job, NoJobAvailable
from minerva.system.helpers import add_job_source, get_job_source
from minerva.directory.helpers_v4 import name_to_datasource


@with_connection
def test_enqueue_job(conn):
    clear(conn)

    job_source_name = "dummy-job-src"
    path = "/data/kpi_1.csv"
    job_type = "dummy"
    filesize = 1000
    description = {"uri": path}
    description_json = json.dumps(description)

    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, "dummy-src")

        job_source_id = get_job_source(cursor, job_source_name)

        if not job_source_id:
            job_source_id = add_job_source(conn, job_source_name, "dummy", '{}')

    enqueue_job(conn, job_type, description_json, filesize, job_source_id)

    with closing(conn.cursor()) as cursor:
        cursor.execute(
            "SELECT j.description "
            "FROM system.job_queue jq "
            "JOIN system.job j ON j.id = jq.job_id")

        eq_(cursor.rowcount, 1)

        job_description, = cursor.fetchone()

    eq_(json.loads(job_description)["uri"], path)


@with_connection
def test_get_job(conn):
    clear(conn)

    job_source_name = "dummy-job-src"
    path = "/data/kpi_2.csv"
    job_type = 'dummy'
    filesize = 1060
    description = {"uri": path}
    description_json = json.dumps(description)

    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, "dummy-src")

        job_source_id = get_job_source(cursor, job_source_name)

        if not job_source_name:
            job_source_id = add_job_source(conn, job_source_name, "dummy", '{}')

    enqueue_job(conn, job_type, description_json, filesize, job_source_id)

    conn.commit()

    with closing(conn.cursor()) as cursor:
        job = get_job(cursor)

    conn.commit()

    job_id, job_type, description, size, parser_config = job

    eq_(path, json.loads(description)["uri"])

    with closing(conn.cursor()) as cursor:
        job = get_job(cursor)

        eq_(job, None)


def test_concurrent_locks():
    thread1 = Thread(target=lock_job_queue(1))
    thread2 = Thread(target=get_job_task)

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()


def get_job_task():
    none_tuple = (None, None, None, None, None)

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.callproc("system.get_job")

            row = cursor.fetchone()

            eq_(row, none_tuple)


def lock_job_queue(duration):
    def f():
        obtain_lock = query(
            "LOCK TABLE system.job_queue "
            "IN SHARE UPDATE EXCLUSIVE MODE NOWAIT;")

        with closing(connect()) as conn:
            try:
                with closing(conn.cursor()) as cursor:
                    obtain_lock(cursor)

                time.sleep(duration)
            except psycopg2.OperationalError:
                conn.rollback()
            else:
                conn.commit()
    
    return f


def query(sql):
    def f(cursor, *args):
        cursor.execute(sql, args)

    return f


@with_connection
def test_waiting_locks(conn):
    clear(conn)

    job_source_name = "dummy-job-src"
    path = "/data/kpi_2.csv"
    job_type = 'dummy'
    filesize = 1060
    description_json = '{{"uri": "{}"}}'.format(path)

    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, "dummy-src")

        job_source_id = get_job_source(cursor, job_source_name)

        if not job_source_id:
            job_source_id = add_job_source(cursor, job_source_name, "dummy", '{}')

    enqueue_job(conn, job_type, description_json, filesize, job_source_id)

    conn.commit()

    with closing(conn.cursor()) as cursor:
        job = get_job(cursor)

    conn.commit()

    with closing(conn.cursor()) as cursor:
        cursor.execute("select relation::regclass, * FROM pg_locks WHERE NOT GRANTED")

        rows = cursor.fetchall()

    job_id, job_type, description, size, parser_config = job

    eq_(path, json.loads(description)["uri"])


def clear(conn):
    with closing(conn.cursor()) as cursor:
        cursor.execute("TRUNCATE system.job CASCADE")

