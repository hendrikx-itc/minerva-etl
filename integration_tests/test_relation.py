# -*- coding: utf-8 -*-
from minerva.test import with_conn
from minerva.directory import relation

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2017 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


@with_conn()
def test_create_relationtype(conn):
    relation.create_relationtype(conn, "a->b")

    conn.commit()
