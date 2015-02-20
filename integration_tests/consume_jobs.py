# -*- coding: utf-8 -*-
"""
Consumes all jobs in the test database.
"""
from __future__ import print_function
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing
from time import sleep

from minerva.util import each
from minerva.test import connect
from minerva.system.jobqueue import get_job


TIMEOUT = 1.0


def main():
    with closing(connect()) as conn:
        run(conn)


def run(conn):
    each(print, jobs(conn))


def jobs(conn):
    while True:
        with closing(conn.cursor()) as cursor:
            job = get_job(cursor)

        conn.commit()
        yield job
        sleep(TIMEOUT)


if __name__ == "__main__":
    main()
