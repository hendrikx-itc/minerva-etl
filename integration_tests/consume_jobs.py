from __future__ import print_function
from contextlib import closing
from time import sleep

from minerva_db import connect
from minerva.system.jobqueue import get_job, NoJobAvailable
from minerva.system.helpers import add_datasource, add_process


TIMEOUT = 1.0


def main():
    with closing(connect()) as conn:
        run(conn)


def run(conn):
    datasource_id = add_dummy_datasource(conn)

    minerva_proc_id = add_dummy_process(conn)

    each(print, jobs(conn, minerva_proc_id))


def each(func, items):
    for item in items:
        func(item)


def jobs(conn, minerva_proc_id):
    while True:
        try:
            job = get_job(conn, minerva_proc_id)

            conn.commit()
        except NoJobAvailable:
            print("no job")
            sleep(TIMEOUT)
        else:
            yield job


def add_dummy_datasource(conn):
    return add_datasource(conn, "job_generater", "dummy_data",
            "/data", ".*", False, "do_nothing", "do_nothing", "{}")

def add_dummy_process(conn):
    return add_process(conn, "integration_test", "localhost", 1234, 567)


if __name__ == "__main__":
    main()
