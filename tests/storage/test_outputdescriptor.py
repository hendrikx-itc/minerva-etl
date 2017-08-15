from nose.tools import assert_equal, assert_is

from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage.outputdescriptor import OutputDescriptor
from minerva.storage import datatype


def test_constructor():
    value_descriptor = ValueDescriptor(
        'x',
        datatype.registry['smallint']
    )

    output_descriptor = OutputDescriptor(
        value_descriptor
    )

    assert output_descriptor is not None


def test_serialize_smallint():
    output_descriptor = OutputDescriptor(
        ValueDescriptor('x', datatype.registry['smallint'])
    )

    assert output_descriptor.serialize(43) == '43'


def test_load_from_config():
    config = {
        'name': 'x',
        'data_type': 'smallint',
        'serializer_config': {
        }
    }
    output_descriptor = OutputDescriptor.load(config)

    assert_is(output_descriptor.value_descriptor.data_type, datatype.registry[
        'smallint'])

    assert_equal(output_descriptor.value_descriptor.name, 'x')

    assert_equal(config, output_descriptor.to_dict())
