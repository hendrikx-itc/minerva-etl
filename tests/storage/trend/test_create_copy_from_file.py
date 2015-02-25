# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import datetime

import pytz

from minerva.directory import DataSource
from minerva.storage import datatype
from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage.trend.trendstore import create_copy_from_file


DATA_SOURCE = DataSource(
    1, name="DummySource", description="Dummy data source"
)


def test_create_copy_from_file_empty():
    f = create_copy_from_file(None, None, [], [])

    assert f.read() == ""


def test_create_copy_from_file_simple():
    timestamp = pytz.utc.localize(
        datetime.datetime(2008, 12, 3, 0, 15, 0)
    )

    modified = pytz.utc.localize(
        datetime.datetime(2008, 12, 3, 0, 15, 0)
    )

    rows = [
        (1, ('a', '23'))
    ]

    value_descriptors = [
        ValueDescriptor('x', datatype.Text),
        ValueDescriptor('y', datatype.Text)
    ]

    f = create_copy_from_file(timestamp, modified, rows, value_descriptors)

    text = f.read()
    expected = (
        "1\t'2008-12-03T00:15:00+00:00'\t'2008-12-03T00:15:00+00:00'\t"
        "a\t23\n"
    )

    assert text == expected


def test_create_copy_from_file_int_array():
    timestamp = pytz.utc.localize(
        datetime.datetime(2008, 12, 3, 0, 15, 0)
    )

    modified = pytz.utc.localize(
        datetime.datetime(2008, 12, 3, 0, 15, 0)
    )

    rows = [
        (1, ('a', '23', [1, 2, 3]))
    ]

    value_descriptors = [
        ValueDescriptor('x', datatype.Text),
        ValueDescriptor('y', datatype.Text),
        ValueDescriptor('z', datatype.SmallInt)  # should be array
    ]

    f = create_copy_from_file(timestamp, modified, rows, value_descriptors)

    text = f.read()
    expected = (
        "1\t'2008-12-03T00:15:00+00:00'\t'2008-12-03T00:15:00+00:00'\t"
        "a\t23\t{\"1\",\"2\",\"3\"}\n"
    )

    assert text == expected, '\n{}\n!=\n{}'.format(text, expected)


def test_create_copy_from_file_text_array():
    timestamp = pytz.utc.localize(
        datetime.datetime(2008, 12, 3, 0, 15, 0)
    )
    modified = pytz.utc.localize(
        datetime.datetime(2008, 12, 3, 0, 15, 0)
    )

    rows = [
        (1, ('a', '23', ["a=b,c=d", "e=f,g=h", "i=j,k=l"]))
    ]

    value_descriptors = [
        ValueDescriptor('x', datatype.Text),
        ValueDescriptor('y', datatype.SmallInt),
        ValueDescriptor('z', datatype.Text)  # should be array
    ]

    f = create_copy_from_file(timestamp, modified, rows, value_descriptors)

    text = f.read()

    expected = (
        "1\t'2008-12-03T00:15:00+00:00'\t'2008-12-03T00:15:00+00:00'\t"
        "a\t23\t{\"a=b,c=d\",\"e=f,g=h\",\"i=j,k=l\"}\n"
    )

    assert text == expected, '\n{}\n!=\n{}'.format(text, expected)
