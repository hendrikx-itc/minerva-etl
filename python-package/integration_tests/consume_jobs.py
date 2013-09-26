from __future__ import print_function
from contextlib import closing
from time import sleep

from minerva.util import each
from minerva.test import connect
from minerva.system.jobqueue import get_job, NoJobAvailable


TIMEOUT = 1.0


def main():
    with closing(connect()) as conn:
        run(conn)


def run(conn):
    each(print, jobs(conn))


def jobs(conn):
    while True:
        try:
            with closing(conn.cursor()) as cursor:
                job = get_job(cursor)
        except:
            conn.rollback()
        else:
            conn.commit()
            yield job


if __name__ == "__main__":
    main()
