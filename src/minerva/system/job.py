# -*- coding: utf-8 -*-
"""Provides the Job class."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import json

from minerva.system.jobqueue import enqueue_job


class Job(object):
    """
    Represents a Minerva job for processing by a node.
    """
    def __init__(self, type, description, size=0,
                 created=None, started=None, finished=None, state=None):
        self.type = type
        self.description = description
        self.size = size
        self.created = created
        self.started = started
        self.finished = finished
        self.state = state

    def enqueue(self, conn):
        enqueue_job(conn, self.type, json.dumps(self.description), self.size,
                    self.job_source_id)
