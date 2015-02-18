from datetime import datetime
from nose.tools import assert_raises

from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage.datatype import DataTypeSmallInt, DataTypeTimestamp


def test_constructor():
    value_descriptor = ValueDescriptor(
        'x',
        DataTypeSmallInt,
        {},
        {}
    )


def test_parse_smallint():
    value_descriptor = ValueDescriptor('x', DataTypeSmallInt)

    assert value_descriptor.parse('42') == 42


def test_parse_smallint_out_of_range():
    value_descriptor = ValueDescriptor('x', DataTypeSmallInt)

    assert_raises(ValueError, value_descriptor.parse, '7800900')


def test_serialize_smallint():
    value_descriptor = ValueDescriptor('x', DataTypeSmallInt)

    assert value_descriptor.serialize(43) == '43'


def test_parse_timestamp():
    value_descriptor = ValueDescriptor('t', DataTypeTimestamp)

    assert value_descriptor.parse('2015-01-13T13:00:00') == datetime(
        2015, 1, 13, 13, 0, 0
    )
