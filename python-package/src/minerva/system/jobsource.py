# -*- coding: utf-8 -*-
"""
Provides the JobSource class.
"""
__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import json


class JobSource(object):
    def __init__(self, id, name, job_type, config):
        self.id = id
        self.name = name
        self.job_type = job_type
        self.config = json.loads(config)

    @staticmethod
    def get_by_name(cursor, name):
        """
        Retrieve the a job_source by its name.
        """
        query = "SELECT id FROM system.job_source WHERE name=%s"

        args = (name, )

        cursor.execute(query, args)

        if cursor.rowcount == 1:
            (datasource_id, ) = cursor.fetchone()

            return datasource_id

    def create(self, cursor):
        """
        :param cursor: A psycopg2 cursor on a Minerva Directory database.
        """
        args = self.name, self.job_type, json.dumps(self.config)

        cursor.callproc("system.add_job_source", args)

        (jobsource_id, ) = cursor.fetchone()

        self.id = jobsource_id

        return self
