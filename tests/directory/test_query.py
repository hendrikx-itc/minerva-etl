# -*- coding: utf-8 -*-
"""
Unit tests for compiling Minerva Queries to SQL.
"""
import unittest

from minerva.directory.query import compile_sql, QueryError


class TestQuery(unittest.TestCase):
    def test_simple(self):
        minerva_query = [{"type": "C", "value": ["Cell"]}]
        relation_group_name = "test"

        sql = compile_sql(minerva_query, relation_group_name)

        expected_sql = (
            ' FROM (VALUES(NULL)) dummy '
            'JOIN directory.entity_tag_link_denorm eld ON %s <@ eld.tags'
        )

        expected_args = [[u"cell"]]

        expected_entity_id_column = "eld.entity_id"

        expected = (expected_sql, expected_args, expected_entity_id_column)

        self.assertEqual(sql, expected)

    def test_starting_with_specifier(self):
        """
        Compiling a Minerva query starting with a specifier should raise an
        exception.
        """
        minerva_query = [{"type": "S", "value": "11030"}]
        relation_group_name = "test"

        with self.assertRaises(QueryError) as cm:
            compile_sql(minerva_query, relation_group_name)
