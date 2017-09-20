# -*- coding: utf-8 -*-
from contextlib import closing
import unittest

from minerva.test import connect, clear_database

from minerva.directory.relationtype import RelationType


class TestEntityTags(unittest.TestCase):
    def setUp(self):
        self.conn = clear_database(connect())

    def tearDown(self):
        self.conn.close()

    def test_create_relationtype(self):
        with closing(self.conn.cursor()) as cursor:
            RelationType.create(
                RelationType.Descriptor('test-relation-type', 'one-to-one')
            )(cursor)
