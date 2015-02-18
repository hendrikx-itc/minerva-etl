# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing

from minerva.util import first

from minerva.storage.attribute import schema


def clear_database(conn):
    with closing(conn.cursor()) as cursor:
        cursor.execute("DELETE FROM directory.datasource")
        cursor.execute("DELETE FROM directory.entitytype")
        cursor.execute("DELETE FROM directory.tag")
