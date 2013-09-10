# -*- coding: utf-8 -*-
"""
Unit tests for compiling Minerva Queries to SQL.
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
from datetime import datetime

import pytz

from minerva.directory.query import compile_sql

def test_simple():
    minerva_query = [{"type": "C", "value": ["Cell"]}]

    sql = compile_sql(minerva_query)

    expected_sql = (
        "FROM directory.entitytaglink tl_0_0 "
        "JOIN directory.tag t_0_0 ON t_0_0.id = tl_0_0.tag_id "
        "AND lower(t_0_0.name) = lower(%s)")

    expected_args = ["Cell"]

    expected_entity_id_column = "tl_0_0.entity_id"

    expected = (expected_sql, expected_args, expected_entity_id_column)

    eq_(sql, expected)


def test_starting_with_specifier():
    minerva_query = [{"type": "S", "value": "11030"}]

    sql = compile_sql(minerva_query)

    expected_sql = (
        "")

    expected_args = ["11030"]

    expected_entity_id_column = "tl_0_0.entity_id"

    expected = (expected_sql, expected_args, expected_entity_id_column)

    eq_(sql, expected)
