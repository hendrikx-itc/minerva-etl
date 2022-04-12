from datetime import datetime
import unittest

from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage.inputdescriptor import InputDescriptor
from minerva.storage import datatype


class TestInputDescriptor(unittest.TestCase):
    def test_constructor(self):
        value_descriptor = ValueDescriptor("x", datatype.registry["smallint"])

        input_descriptor = InputDescriptor(value_descriptor)

        assert input_descriptor is not None

    def test_parse_smallint(self):
        input_descriptor = InputDescriptor(
            ValueDescriptor("x", datatype.registry["smallint"])
        )

        assert input_descriptor.parse("42") == 42

    def test_parse_smallint_out_of_range(self):
        input_descriptor = InputDescriptor(
            ValueDescriptor("x", datatype.registry["smallint"])
        )

        with self.assertRaises(datatype.ParseError):
            input_descriptor.parse("7800900")

    def test_parse_timestamp(self):
        input_descriptor = InputDescriptor(
            ValueDescriptor("t", datatype.registry["timestamp"])
        )

        assert input_descriptor.parse("2015-01-13T13:00:00") == datetime(
            2015, 1, 13, 13, 0, 0
        )

    def test_load_from_config(self):
        config = {"name": "x", "data_type": "smallint", "parser_config": {}}
        input_descriptor = InputDescriptor.load(config)

        self.assertIs(
            input_descriptor.value_descriptor.data_type, datatype.registry["smallint"]
        )

        self.assertEqual(input_descriptor.value_descriptor.name, "x")

        self.assertEqual(config, input_descriptor.to_dict())
