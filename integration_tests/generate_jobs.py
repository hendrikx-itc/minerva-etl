# -*- coding: utf-8 -*-
"""
Generates jobs in the test database.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import random
from contextlib import closing
from functools import partial

from minerva.util import each, expand_args
from minerva.test import connect
from minerva.system.jobqueue import enqueue_job
from minerva.system.helpers import add_job_source, get_job_source


def main():
    """Script entry point."""
    with closing(connect()) as conn:
        jobsource_id = add_dummy_jobsource(conn)

        run(conn, jobsource_id)


def run(conn, jobsource_id):
    """The actual job generation loop."""
    each(expand_args(partial(enqueue, conn, jobsource_id)), job_generator())


def enqueue(conn, jobsource_id, description, size):
    enqueue_job(conn, 'dummy', description, size, jobsource_id)
    conn.commit()


def job_generator():
    while True:
        path = "/data/kpi_1.csv"
        size = random.randint(100, 100000)
        description = '{{"uri": {}}}'.format(path)

        yield description, size


def add_dummy_jobsource(conn):
    name = "job_generator"
    config = "{}"

    with closing(conn.cursor()) as cursor:
        return (
            get_job_source(cursor, name) or
            add_job_source(cursor, name, "dummy", config))


if __name__ == "__main__":
    main()
