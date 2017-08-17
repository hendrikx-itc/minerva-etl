# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from nose.tools import eq_

from minerva.test import with_conn

from minerva.storage import get_plugin


def test_get_plugin():
    plugin = get_plugin("attribute")

    eq_('create', plugin.__name__)


@with_conn()
def test_load_plugin(conn):
    plugin = get_plugin("attribute")(conn)

    assert not plugin is None
