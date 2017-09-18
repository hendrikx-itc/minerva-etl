import unittest

from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage import datatype


class TestValueDescriptor(unittest.TestCase):
    def test_constructor(self):
        value_descriptor = ValueDescriptor(
            'x',
            datatype.registry['smallint']
        )

        self.assertIsNotNone(value_descriptor)
