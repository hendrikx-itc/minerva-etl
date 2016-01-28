# -*- coding: utf-8 -*-
"""Provides the JobSource class."""
__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import json

from minerva.system.struct import Struct


class JobSourceDescriptor:
    def __init__(self, name, job_type, config):
        self.name = name
        self.job_type = job_type
        self.config = config


class JobSource:

    """
    Encapsulates job source and provides loading and creating functionality.

    The default config serialization an deserialization functionality can be
    overridden in sub-classes to provide more elegant access to the type
    specific configuration.

    """

    def __init__(self, id_, name, job_type, config):
        self.id = id_
        self.name = name
        self.job_type = job_type
        self.config = self.deserialize_config(config)

    @staticmethod
    def deserialize_config(config):
        """Parse as JSON and return dictionary wrapped in Struct."""
        return Struct(json.loads(config))

    @staticmethod
    def serialize_config(config):
        """Serialize as JSON and return string."""
        return json.dumps(config)

    @staticmethod
    def get_by_name(name):
        """Retrieve the a job_source by its name and return Id."""
        def f(cursor):
            query = (
                "SELECT id, name, job_type, config "
                "FROM system.job_source WHERE name=%s"
            )

            args = (name, )

            cursor.execute(query, args)

            if cursor.rowcount == 1:
                (job_source_id, name_, job_type, config) = cursor.fetchone()

                return JobSource(job_source_id, name_, job_type, config)

        return f

    @staticmethod
    def create(job_source_descriptor):
        """
        Create the job source in the database and return self.
        """
        def f(cursor):
            args = (
                job_source_descriptor.name,
                job_source_descriptor.job_type,
                job_source_descriptor.config
            )

            cursor.callproc("system.create_job_source", args)

            (job_source_id, name, job_type, config) = cursor.fetchone()

            return JobSource(job_source_id, name, job_type, config)

        return f
