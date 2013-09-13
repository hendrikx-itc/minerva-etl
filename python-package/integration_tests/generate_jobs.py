import random
from contextlib import closing

from minerva_db import connect
from minerva.system.jobqueue import enqueue_job
from minerva.system.helpers import add_datasource, add_process


def main():
    with closing(connect()) as conn:
        run(conn)


def run(conn):
    datasource_id = add_dummy_datasource(conn)

    minerva_proc_id = add_dummy_process(conn)

    for path, filesize in job_generator():
        enqueue_job(conn, datasource_id, minerva_proc_id, path, filesize)


def job_generator():
    while True:
        path = "/data/kpi_1.csv"
        filesize = random.randint(100, 100000)

        yield path, filesize


def add_dummy_datasource(conn):
    return add_datasource(conn, "job_generater", "dummy_data",
            "/data", ".*", False, "do_nothing", "do_nothing", "{}")

def add_dummy_process(conn):
    return add_process(conn, "integration_test", "localhost", 1234, 567)


if __name__ == "__main__":
    main()
