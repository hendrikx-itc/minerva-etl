from nose.tools import assert_is_not_none

from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage import datatype


def test_constructor():
    value_descriptor = ValueDescriptor(
        'x',
        datatype.registry['smallint']
    )

    assert_is_not_none(value_descriptor)
