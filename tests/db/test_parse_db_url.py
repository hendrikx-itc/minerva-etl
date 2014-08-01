# -*- coding: utf-8 -*-
"""
Unit tests for the storing of data packages.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from nose.tools import eq_

from minerva.db import parse_db_url


def test_parse_db_url():
    url = "postgresql://tester:pwd@localhost/test_db"

    scheme, username, password, host, port, database = parse_db_url(url)

    eq_(scheme, "postgresql")
    eq_(username, "tester")
    eq_(password, "pwd")
    eq_(host, "localhost")
    eq_(port, 5432)
    eq_(database, "test_db")


def test_parse_db_url_port():
    url = "postgresql://tester:pwd@localhost:1234/test_db"

    _scheme, _username, _password, _host, port, _database = parse_db_url(url)

    eq_(port, 1234)
