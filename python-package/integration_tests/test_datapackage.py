# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import datetime

import pytz
from nose.tools import eq_

from minerva_storage_attribute.datapackage import DataPackage

TIMESTAMP = pytz.utc.localize(datetime.datetime(2013, 8, 30, 15, 30))


def create_simple_package():
    attribute_names = ["power", "height", "state"]
    rows = [
        (123001, (405, 0.0, "enabled")),
        (123002, (300, 10.5, "enabled")),
        (123003, (41033, 22.3, "enabled")),
        (123004, (880, 30.0, "enabled"))]

    return DataPackage(TIMESTAMP, attribute_names, rows)


def test_constructor():
    datapackage = create_simple_package()

    eq_(datapackage.timestamp, TIMESTAMP)
    eq_(len(datapackage.attribute_names), 3)
    eq_(len(datapackage.rows), 4)


def test_deduce_data_types():
    datapackage = create_simple_package()

    data_types = datapackage.deduce_data_types()

    eq_(data_types[0], "integer")
    eq_(data_types[1], "real")
    eq_(data_types[2], "text")

    attr_type_dict = dict(zip(datapackage.attribute_names, data_types))

    eq_(attr_type_dict["state"], "text")
    eq_(attr_type_dict["power"], "integer")
    eq_(attr_type_dict["height"], "real")


def test_deduce_attributes():
    datapackage = create_simple_package()

    attributes = datapackage.deduce_attributes()

    eq_(attributes[0].name, "power")
    eq_(attributes[0].datatype, "integer")

    eq_(attributes[1].name, "height")
    eq_(attributes[1].datatype, "real")

    eq_(attributes[2].name, "state")
    eq_(attributes[2].datatype, "text")
