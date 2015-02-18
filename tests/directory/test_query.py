# -*- coding: utf-8 -*-
"""
Unit tests for compiling Minerva Queries to SQL.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.test import eq_, raises
from minerva.directory.query import compile_sql, QueryError


def test_simple():
    minerva_query = [{"type": "C", "value": ["Cell"]}]
    relation_group_name = "test"

    sql = compile_sql(minerva_query, relation_group_name)

    expected_sql = (
        ' FROM (VALUES(NULL)) dummy '
        'JOIN directory.entity_link_denorm eld ON %s <@ eld.tags'
    )

    expected_args = [[u"cell"]]

    expected_entity_id_column = "eld.entity_id"

    expected = (expected_sql, expected_args, expected_entity_id_column)

    eq_(sql, expected)


@raises(QueryError)
def test_starting_with_specifier():
    """
    Compiling a Minerva query starting with a specifier should raise an
    exception.
    """
    minerva_query = [{"type": "S", "value": "11030"}]
    relation_group_name = "test"

    compile_sql(minerva_query, relation_group_name)
