import random
from contextlib import closing

from minerva.test import connect
from minerva.system.jobqueue import enqueue_job
from minerva.system.helpers import add_job_source, get_job_source


def main():
    with closing(connect()) as conn:
        run(conn)


def run(conn):
    jobsource_id = add_dummy_jobsource(conn)

    for path, size in job_generator():
        description = '{{"uri": {}}}'.format(path)
        enqueue_job(conn, 'dummy', description, size, jobsource_id)
        conn.commit()


def job_generator():
    while True:
        path = "/data/kpi_1.csv"
        size = random.randint(100, 100000)

        yield path, size


def add_dummy_jobsource(conn):
    name = "job_generator"
    config = "{}"

    with closing(conn.cursor()) as cursor:
        return (
            get_job_source(cursor, name) or
            add_job_source(cursor, name, "dummy", config))


if __name__ == "__main__":
    main()
