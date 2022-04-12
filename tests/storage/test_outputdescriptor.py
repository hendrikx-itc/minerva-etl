import unittest

from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage.outputdescriptor import OutputDescriptor
from minerva.storage import datatype


class TestOutputDescriptor(unittest.TestCase):
    def test_constructor(self):
        value_descriptor = ValueDescriptor("x", datatype.registry["smallint"])

        output_descriptor = OutputDescriptor(value_descriptor)

        assert output_descriptor is not None

    def test_serialize_smallint(self):
        output_descriptor = OutputDescriptor(
            ValueDescriptor("x", datatype.registry["smallint"])
        )

        assert output_descriptor.serialize(43) == "43"

    def test_load_from_config(self):
        config = {"name": "x", "data_type": "smallint", "serializer_config": {}}
        output_descriptor = OutputDescriptor.load(config)

        self.assertIs(
            output_descriptor.value_descriptor.data_type, datatype.registry["smallint"]
        )

        self.assertEqual(output_descriptor.value_descriptor.name, "x")

        self.assertEqual(config, output_descriptor.to_dict())
