# -*- coding: utf-8 -*-
"""Tests for methods of the DataPackage class."""
__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import json
from datetime import datetime

import pytz

from minerva.test import eq_
from minerva.storage import datatype
from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage.attribute.datapackage import DataPackage

TIMESTAMP = pytz.utc.localize(datetime(2013, 8, 30, 15, 30))


simple_package = DataPackage(
    ["power", "height", "state", "freetext"],
    [
        (123001, TIMESTAMP, ("405", "0.0", "enabled", "")),
        (123003, TIMESTAMP, ("41033", "22.3", "enabled", "")),
        (123004, TIMESTAMP, ("880", "30.0", "enabled", ""))
    ]
)


array_package = DataPackage(
    ["curve"],
    [
        (123001, TIMESTAMP, ('0,1,2,4,7,4,2,1,0',)),
        (123002, TIMESTAMP, ('0,1,2,5,8,4,2,1,0',)),
        (123003, TIMESTAMP, ('0,1,3,5,7,4,3,1,0',)),
        (123004, TIMESTAMP, ('0,1,2,4,9,4,2,1,0',))
    ]
)


package_array_list_a = DataPackage(
    ["curve"],
    [
        (123001, TIMESTAMP, (['0', '1', '2'],)),
        (123002, TIMESTAMP, (['0', '1', '2'],)),
        (123003, TIMESTAMP, (['', '', ''],))
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

    eq_(len(data_package.attribute_names), 4)
    eq_(len(data_package.rows), 3)


def test_deduce_value_descriptors():
    """The max data types should be deduced from the package."""
    data_package = simple_package

    data_types = data_package.deduce_value_descriptors()

    eq_(data_types[0], ValueDescriptor())
    eq_(data_types[1], ValueDescriptor())
    eq_(data_types[2], ValueDescriptor())
    eq_(data_types[3], ValueDescriptor())


def test_deduce_data_types_array():
    """The max data types should be deduced from the package."""
    data_package = array_package

    data_types = data_package.deduce_value_descriptors()

    attr_type_dict = dict(zip(data_package.attribute_names, data_types))

    eq_(attr_type_dict["curve"], ValueDescriptor())


def test_deduce_datatypes_empty():
    data_package = DataPackage(
        attribute_names=('height', 'power', 'refs'),
        rows=[]
    )

    data_types = data_package.deduce_value_descriptors()

    eq_(data_types, [
        ValueDescriptor(datatype.DataTypeSmallInt),
        ValueDescriptor(datatype.DataTypeSmallInt),
        ValueDescriptor(datatype.DataTypeSmallInt)
    ])


def test_to_dict():
    data_package = DataPackage(
        attribute_names=('height', 'power'),
        rows=[
            (10034, TIMESTAMP, ['15.6', '68'])
        ]
    )

    json_data = data_package.to_dict()

    expected_json = (
        '{"attribute_names": ["height", "power"], '
        '"rows": ['
        '[10034, "2013-08-30T15:30:00+00:00", ["15.6", "68"]]'
        ']'
        '}')

    eq_(json.dumps(json_data), expected_json)


def test_from_dict():
    json_data = {
        "timestamp": "2013-09-16T16:55:00+00:00",
        "attribute_names": ["tilt", "azimuth"],
        "rows": [
            [13403, ["4", "180"]]
        ]
    }

    data_package = DataPackage.from_dict(json_data)

    eq_(data_package.attribute_names[1], "azimuth")
    eq_(data_package.rows[0][0], 13403)
    eq_(data_package.rows[0][1][1], "180")


def test_deduce_attributes():
    data_package = simple_package

    attributes = data_package.deduce_attributes()

    attr_dict = {
        attribute.name: attribute
        for attribute in attributes
    }

    eq_(attr_dict["power"].data_type, datatype.DataTypeInteger)
    eq_(attr_dict["height"].data_type, datatype.DataTypeReal)
    eq_(attr_dict["state"].data_type, datatype.DataTypeText)


def test_create_copy_from_lines():
    """
    The format of the copy-from-file should be acceptable by PostgreSQL.
    """
    data_package = simple_package
    value_descriptors = [
        ValueDescriptor(
            value_descriptor.name,
            value_descriptor.data_type,
            {},
            datatype.copy_from_serializer_config[value_descriptor.data_type]
        )
        for value_descriptor in data_package.deduce_value_descriptors()
    ]

    lines = data_package._create_copy_from_lines(value_descriptors)

    eq_(
        lines[0],
        "123001\t2013-08-30 15:30:00+00:00\t405\t0.0\tenabled\t\\N\n"
    )

    data_package = array_package
    value_descriptors = data_package.deduce_value_descriptors()

    lines = data_package._create_copy_from_lines(value_descriptors)

    eq_(lines[0], "123001\t2013-08-30 15:30:00+00:00\t{0,1,2,4,7,4,2,1,0}\n")

    data_package = package_array_list_a
    value_descriptors = data_package.deduce_value_descriptors()

    lines = data_package._create_copy_from_lines(value_descriptors)

    eq_(lines[1], "123002\t2013-08-30 15:30:00+00:00\t{0,1,2}\n")
    eq_(lines[2], "123003\t2013-08-30 15:30:00+00:00\t{NULL,NULL,NULL}\n")

    data_package = package_array_list_b
    data_types = data_package.deduce_data_types()

    lines = data_package._create_copy_from_lines(data_types)

    eq_(lines[0], "123001\t2013-08-30 15:30:00+00:00\t{NULL,NULL}\n")
    eq_(lines[1], "123002\t2013-08-30 15:30:00+00:00\t{NULL,NULL}\n")

    f = data_package._create_copy_from_file(data_types)
