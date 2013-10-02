# pylint: disable=W0212
# -*- coding: utf-8 -*-
"""Tests for methods of the DataPackage class."""
__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import json
from datetime import datetime

import pytz
from nose.tools import eq_

from minerva.storage.attribute.datapackage import DataPackage

TIMESTAMP = pytz.utc.localize(datetime(2013, 8, 30, 15, 30))


def create_simple_package():
    """Return new DataPackage instance with a simple set of data."""
    attribute_names = ["power", "height", "state"]
    rows = [
        (123001, (405, 0.0, "enabled")),
        (123002, (300, 10.5, "enabled")),
        (123003, (41033, 22.3, "enabled")),
        (123004, (880, 30.0, "enabled"))]

    return DataPackage(TIMESTAMP, attribute_names, rows)


def create_package_array():
    """Return new DataPackage instance with a simple set of data."""
    attribute_names = ["curve"]
    rows = [
        (123001, ('0,1,2,4,7,4,2,1,0',)),
        (123002, ('0,1,2,5,8,4,2,1,0',)),
        (123003, ('0,1,3,5,7,4,3,1,0',)),
        (123004, ('0,1,2,4,9,4,2,1,0',))]

    return DataPackage(TIMESTAMP, attribute_names, rows)


def test_constructor():
    """Test creation of a new DataPackage instance."""
    datapackage = create_simple_package()

    eq_(datapackage.timestamp, TIMESTAMP)
    eq_(len(datapackage.attribute_names), 3)
    eq_(len(datapackage.rows), 4)


def test_deduce_data_types():
    """The max data types should be deduced from the package."""
    datapackage = create_simple_package()

    data_types = datapackage.deduce_data_types()

    eq_(data_types[0], "integer")
    eq_(data_types[1], "real")
    eq_(data_types[2], "text")

    attr_type_dict = dict(zip(datapackage.attribute_names, data_types))

    eq_(attr_type_dict["state"], "text")
    eq_(attr_type_dict["power"], "integer")
    eq_(attr_type_dict["height"], "real")


def test_deduce_data_types_array():
    """The max data types should be deduced from the package."""
    datapackage = create_package_array()

    data_types = datapackage.deduce_data_types()

    attr_type_dict = dict(zip(datapackage.attribute_names, data_types))

    eq_(attr_type_dict["curve"], "integer[]")


def test_deduce_datatypes_empty():
    datapackage = DataPackage(
        timestamp=TIMESTAMP,
        attribute_names=('height', 'power', 'refs'),
        rows=[]
    )

    data_types = datapackage.deduce_data_types()

    assert data_types == ['smallint', 'smallint', 'smallint']


def test_to_dict():
    datapackage = DataPackage(
        timestamp=TIMESTAMP,
        attribute_names=('height', 'power'),
        rows=[
            (10034, ['15.6', '68'])
        ]
    )

    json_data = datapackage.to_dict()

    expected_json = (
        '{"timestamp": "2013-08-30T15:30:00+00:00", '
        '"attribute_names": ["height", "power"], '
        '"rows": ['
        '[10034, ["15.6", "68"]]'
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

    datapackage = DataPackage.from_dict(json_data)

    eq_(datapackage.attribute_names[1], "azimuth")
    eq_(datapackage.rows[0][0], 13403)
    eq_(datapackage.rows[0][1][1], "180")


def test_deduce_attributes():
    datapackage = create_simple_package()

    attributes = datapackage.deduce_attributes()

    attr_dict = dict((attribute.name, attribute)
                     for attribute in attributes)

    eq_(attr_dict["power"].datatype, "integer")
    eq_(attr_dict["height"].datatype, "real")
    eq_(attr_dict["state"].datatype, "text")


def test_create_copy_from_lines():
    """
    The format of the copy-from-file should be acceptable by PostgreSQL.
    """
    datapackage = create_simple_package()
    data_types = datapackage.deduce_data_types()

    lines = datapackage._create_copy_from_lines(data_types)

    eq_(lines[0], "123001\t2013-08-30 15:30:00+00:00\t405\t0.0\tenabled\n")

    datapackage = create_package_array()
    data_types = datapackage.deduce_data_types()

    lines = datapackage._create_copy_from_lines(data_types)

    eq_(lines[0], "123001\t2013-08-30 15:30:00+00:00\t{0,1,2,4,7,4,2,1,0}\n")
