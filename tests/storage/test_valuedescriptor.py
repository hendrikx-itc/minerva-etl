from datetime import datetime

from nose.tools import assert_raises, assert_equal, assert_is

from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage import datatype


def test_constructor():
    value_descriptor = ValueDescriptor(
        'x',
        datatype.registry['smallint'],
        {},
        {}
    )

    assert value_descriptor is not None


def test_parse_smallint():
    value_descriptor = ValueDescriptor('x', datatype.registry['smallint'])

    assert value_descriptor.parse('42') == 42


def test_parse_smallint_out_of_range():
    value_descriptor = ValueDescriptor('x', datatype.registry['smallint'])

    assert_raises(ValueError, value_descriptor.parse, '7800900')


def test_serialize_smallint():
    value_descriptor = ValueDescriptor('x', datatype.registry['smallint'])

    assert value_descriptor.serialize(43) == '43'


def test_parse_timestamp():
    value_descriptor = ValueDescriptor('t', datatype.registry['timestamp'])

    assert value_descriptor.parse('2015-01-13T13:00:00') == datetime(
        2015, 1, 13, 13, 0, 0
    )


def test_load_from_config():
    config = {
        'name': 'x',
        'data_type': 'smallint',
        'parser_config': {
        },
        'serializer_config': {
        }
    }
    value_descriptor = ValueDescriptor.from_config(config)

    assert_is(value_descriptor.data_type, datatype.registry['smallint'])

    assert_equal(value_descriptor.name, 'x')

    assert_equal(config, value_descriptor.to_config())
