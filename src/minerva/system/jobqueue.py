# -*- coding: utf-8 -*-
"""
Provides helper functions for interacting with the job queue in the database.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from functools import partial

from minerva.db.util import stored_procedure


class NoJobAvailable(Exception):
    pass


enqueue_job = partial(stored_procedure, "system.create_job")


finish_job = partial(stored_procedure, "system.finish_job")


fail_job = partial(stored_procedure, "system.fail_job")


def get_job(cursor):
    cursor.callproc("system.get_job")

    result = cursor.fetchone()

    if result[0] is None:
        return None
    else:
        return result
