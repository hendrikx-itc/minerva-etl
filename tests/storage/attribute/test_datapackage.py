# -*- coding: utf-8 -*-
"""Tests for methods of the DataPackage class."""
from datetime import datetime

from nose.tools import assert_equal
import pytz

from minerva.storage import datatype
from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage.attribute.datapackage import DataPackage

TIMESTAMP = pytz.utc.localize(datetime(2013, 8, 30, 15, 30))


simple_package = DataPackage(
    ["power", "height", "state", "remark"],
    [
        (123001, TIMESTAMP, ("405", "0.0", "enabled", "")),
        (123003, TIMESTAMP, ("41033", "22.3", "enabled", "")),
        (123004, TIMESTAMP, ("880", "30.0", "enabled", ""))
    ]
)


array_package = DataPackage(
    ["curve"],
    [
        (123001, TIMESTAMP, ([0,1,2,4,7,4,2,1,0],)),
        (123002, TIMESTAMP, ([0,1,2,5,8,4,2,1,0],)),
        (123003, TIMESTAMP, ([0,1,3,5,7,4,3,1,0],)),
        (123004, TIMESTAMP, ([0,1,2,4,9,4,2,1,0],))
    ]
)


package_array_list_a = DataPackage(
    ["curve"],
    [
        (123001, TIMESTAMP, ([0, 1, 2],)),
        (123002, TIMESTAMP, ([0, 1, 2],)),
        (123003, TIMESTAMP, ([None, None, None],))
    ]
)


package_array_list_b = DataPackage(
    ["curve"],
    [
        (123001, TIMESTAMP, (['', ''],)),
        (123002, TIMESTAMP, (['', ''],))
    ]
)


package_array_list_c = DataPackage(
    ["curve"],
    [
        (123001, (['e=34,c=1', 'e=45,c=3', 'e=33,c=2'],)),
        (123002, (['', '', ''],))
    ]
)


def test_constructor():
    """Test creation of a new DataPackage instance."""
    data_package = simple_package

    assert_equal(len(data_package.attribute_names), 4)
    assert_equal(len(data_package.rows), 3)


def test_deduce_value_descriptors():
    """The max data types should be deduced from the package."""
    data_package = DataPackage(
        ["power", "height", "state", "remark"],
        [
            (123001, TIMESTAMP, ("405", "0.0", "enabled", "")),
            (123003, TIMESTAMP, ("41033", "22.3", "enabled", "")),
            (123004, TIMESTAMP, ("880", "30.0", "enabled", ""))
        ]
    )

    value_descriptors = data_package.deduce_value_descriptors()

    assert_equal(
        value_descriptors[0],
        ValueDescriptor("power", datatype.Integer)
    )
    assert_equal(
        value_descriptors[1],
        ValueDescriptor("height", datatype.Real)
    )
    assert_equal(
        value_descriptors[2],
        ValueDescriptor("state", datatype.Text)
    )
    assert_equal(
        value_descriptors[3],
        ValueDescriptor("remark", datatype.SmallInt)
    )


def test_deduce_data_types_array():
    """The max data types should be deduced from the package."""
    data_package = DataPackage(
        ["curve"],
        [
            (123001, TIMESTAMP, ('0,1,2,4,7,4,2,1,0',)),
            (123002, TIMESTAMP, ('0,1,2,5,8,4,2,1,0',)),
            (123003, TIMESTAMP, ('0,1,3,5,7,4,3,1,0',)),
            (123004, TIMESTAMP, ('0,1,2,4,9,4,2,1,0',))
        ]
    )

    data_types = data_package.deduce_value_descriptors()

    attr_type_dict = dict(zip(data_package.attribute_names, data_types))

    assert_equal(
        attr_type_dict["curve"],
        ValueDescriptor('curve', datatype.Text)
    )


def test_deduce_data_types_empty():
    data_package = DataPackage(
        attribute_names=('height', 'power', 'refs'),
        rows=[]
    )

    value_descriptors = data_package.deduce_value_descriptors()

    assert_equal(
        value_descriptors[0], ValueDescriptor('height', datatype.SmallInt)
    )


def test_to_dict():
    data_package = DataPackage(
        attribute_names=('height', 'power'),
        rows=[
            (10034, TIMESTAMP, ['15.6', '68'])
        ]
    )

    json_data = data_package.to_dict()

    expected_json = {
        "attribute_names": ["height", "power"],
        "rows": [
            [10034, "2013-08-30T15:30:00+00:00", ["15.6", "68"]]
        ]
    }

    assert_equal(json_data, expected_json)


def test_from_dict():
    json_data = {
        "timestamp": "2013-09-16T16:55:00+00:00",
        "attribute_names": ["tilt", "azimuth"],
        "rows": [
            [13403, ["4", "180"]]
        ]
    }

    data_package = DataPackage.from_dict(json_data)

    assert_equal(data_package.attribute_names[1], "azimuth")
    assert_equal(data_package.rows[0][0], 13403)
    assert_equal(data_package.rows[0][1][1], "180")


def test_deduce_attributes():
    data_package = simple_package

    attributes = data_package.deduce_attributes()

    attr_dict = {
        attribute.name: attribute
        for attribute in attributes
    }

    assert_equal(attr_dict["power"].data_type, datatype.Integer)
    assert_equal(attr_dict["height"].data_type, datatype.Real)
    assert_equal(attr_dict["state"].data_type, datatype.Text)


def test_create_copy_from_lines():
    """
    The format of the copy-from-file should be acceptable by PostgreSQL.
    """
    data_package = DataPackage(
        ["power", "height", "state", "remark"],
        [
            (123001, TIMESTAMP, (405, 0.0, True, "")),
            (123003, TIMESTAMP, (41033, 22.3, True, "")),
            (123004, TIMESTAMP, (880, 30.0, True, ""))
        ]
    )

    value_descriptors = [
        ValueDescriptor(
            'power',
            datatype.Integer,
            {},
            datatype.copy_from_serializer_config(datatype.Integer)
        ),
        ValueDescriptor(
            'height',
            datatype.Real,
            {},
            datatype.copy_from_serializer_config(datatype.Integer)
        ),
        ValueDescriptor(
            'state',
            datatype.Boolean,
            {},
            datatype.copy_from_serializer_config(datatype.Boolean)
        ),
        ValueDescriptor(
            'remark',
            datatype.Text,
            {},
            datatype.copy_from_serializer_config(datatype.Text)
        )
    ]

    lines = data_package._create_copy_from_lines(value_descriptors)

    assert_equal(
        lines[0],
        "123001\t2013-08-30 15:30:00+00:00\t405\t0.0\ttrue\t\n"
    )

    data_package = DataPackage(
        ["curve"],
        [
            (123001, TIMESTAMP, ([0,1,2,4,7,4,2,1,0],)),
            (123002, TIMESTAMP, ([0,1,2,5,8,4,2,1,0],)),
            (123003, TIMESTAMP, ([0,1,3,5,7,4,3,1,0],)),
            (123004, TIMESTAMP, ([0,1,2,4,9,4,2,1,0],))
        ]
    )

    value_descriptors = [
        ValueDescriptor(
            data_package.attribute_names[0],
            datatype.array_of(datatype.SmallInt),
            {},
            datatype.copy_from_serializer_config(
                datatype.array_of(datatype.SmallInt)
            )
        )
    ]

    lines = data_package._create_copy_from_lines(value_descriptors)

    assert_equal(lines[0], "123001\t2013-08-30 15:30:00+00:00\t{0,1,2,4,7,4,2,1,0}\n")

    data_package = DataPackage(
        ["curve"],
        [
            (123001, TIMESTAMP, ([0, 1, 2],)),
            (123002, TIMESTAMP, ([0, 1, 2],)),
            (123003, TIMESTAMP, ([None, None, None],))
        ]
    )

    serializer_config = datatype.copy_from_serializer_config(
        datatype.array_of(datatype.SmallInt)
    )

    value_descriptors = [
        ValueDescriptor(
            'curve',
            datatype.array_of(datatype.SmallInt),
            serializer_config=serializer_config
        )
    ]

    lines = data_package._create_copy_from_lines(value_descriptors)

    assert_equal(lines[1], "123002\t2013-08-30 15:30:00+00:00\t{0,1,2}\n")
    assert_equal(lines[2], "123003\t2013-08-30 15:30:00+00:00\t{\\N,\\N,\\N}\n")

    data_package = DataPackage(
        ["curve"],
        [
            (123001, TIMESTAMP, ([None, None],)),
            (123002, TIMESTAMP, ([None, None],))
        ]
    )

    value_descriptors = [
        ValueDescriptor(
            'curve',
            datatype.array_of(datatype.SmallInt),
            {},
            datatype.copy_from_serializer_config(
                datatype.array_of(datatype.SmallInt)
            )
        )
    ]

    lines = data_package._create_copy_from_lines(value_descriptors)

    assert_equal(lines[0], "123001\t2013-08-30 15:30:00+00:00\t{\\N,\\N}\n")
    assert_equal(lines[1], "123002\t2013-08-30 15:30:00+00:00\t{\\N,\\N}\n")

    f = data_package._create_copy_from_file(value_descriptors)
