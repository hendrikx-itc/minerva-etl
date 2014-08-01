# -*- coding: utf-8 -*-
"""
Provides helper functions for interacting with the system schema.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class NoResultFound(Exception):
    pass


def get_job_source(cursor, name):
    """
    Retrieve the id of a job_source by its name.
    """
    query = "SELECT id FROM system.job_source WHERE name=%s"

    args = (name, )

    cursor.execute(query, args)

    if cursor.rowcount == 1:
        (datasource_id, ) = cursor.fetchone()

        return datasource_id


def add_job_source(cursor, name, job_type, config):
    """
    :param cursor: A psycopg2 cursor on a Minerva Directory database.
    """
    args = name, job_type, config

    cursor.callproc("system.add_job_source", args)

    (jobsource_id, ) = cursor.fetchone()

    return jobsource_id
