# -*- coding: utf-8 -*-
"""
Unit tests for helper functions
"""
import unittest

from minerva.directory.helpers import none_or

def NoneFunc():
    return 'None'

class TestQuery(unittest.TestCase):
    def test_none_or(self):
        noneor = none_or()
        self.assertEqual(noneor(None), None)
        self.assertEqual(noneor('test'), 'test')
        noneor = none_or(NoneFunc, int)
        self.assertEqual(noneor(None), 'None')
        self.assertEqual(noneor('1'), 1)
