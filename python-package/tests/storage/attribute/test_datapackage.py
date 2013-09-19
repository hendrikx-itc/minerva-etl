# -*- coding: utf-8 -*-
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


def test_deduce_datatypes():
    timestamp = pytz.utc.localize(datetime.utcnow())
    datapackage = DataPackage(
        timestamp=timestamp,
        attribute_names=('height', 'power', 'refs'),
        rows=[
            (10034, ['15.6', '68', ['r32', 'r44', 'r50']])
        ]
    )

    data_types = datapackage.deduce_data_types()

    assert data_types[0] == 'real'
    assert data_types[1] == 'smallint'
    assert data_types[2] == 'text'


def test_deduce_datatypes_empty_package():
    timestamp = pytz.utc.localize(datetime.utcnow())
    datapackage = DataPackage(
        timestamp=timestamp,
        attribute_names=('height', 'power', 'refs'),
        rows=[
        ]
    )

    data_types = datapackage.deduce_data_types()

    assert data_types[0] == 'smallint'
    assert data_types[1] == 'smallint'
    assert data_types[2] == 'smallint'


def test_to_dict():
    timestamp = pytz.utc.localize(datetime(2013, 9, 16, 14, 36))
    datapackage = DataPackage(
        timestamp=timestamp,
        attribute_names=('height', 'power'),
        rows=[
            (10034, ['15.6', '68'])
        ]
    )

    json_data = datapackage.to_dict()

    expected_json = (
        '{"timestamp": "2013-09-16T14:36:00+00:00", '
        '"attribute_names": ["height", "power"], '
        '"rows": ['
        '[10034, "15.6", "68"]'
        ']'
        '}')

    eq_(json.dumps(json_data), expected_json)


def test_from_dict():
    json_data = {
        "timestamp": "2013-09-16T16:55:00+00:00",
        "attribute_names": ["tilt", "azimuth"],
        "rows": [
            [13403, "4", "180"]
        ]
    }

    datapackage = DataPackage.from_dict(json_data)

    eq_(datapackage.attribute_names[1], "azimuth")
    eq_(datapackage.rows[0][0], 13403)
