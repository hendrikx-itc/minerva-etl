# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import datetime

from nose.tools import eq_

from minerva.directory.basetypes import DataSource
from minerva.storage.trend.storage import create_copy_from_file


def create_datasource(timezone):
    return DataSource(1, name="DummySource", description="Dummy data source",
            timezone="Europe/Amsterdam")

DATASOURCE = create_datasource("Europe/Amsterdam")


def test_create_copy_from_file_empty():
    f = create_copy_from_file(None, None, [])

    assert f.read() == ""


def test_create_copy_from_file_simple():
    timestamp = DATASOURCE.tzinfo.localize(
            datetime.datetime(2008, 12, 3, 0, 15, 0))
    modified = DATASOURCE.tzinfo.localize(
            datetime.datetime(2008, 12, 3, 0, 15, 0))

    rows = [
        (1, ('a', '23'))]

    f = create_copy_from_file(timestamp, modified, rows)

    text = f.read()
    expected = (
        "1\t'2008-12-03T00:15:00+01:00'\t'2008-12-03T00:15:00+01:00'\t"
        "a\t23\n")

    eq_(text, expected)


def test_create_copy_from_file_int_array():
    timestamp = DATASOURCE.tzinfo.localize(
            datetime.datetime(2008, 12, 3, 0, 15, 0))
    modified = DATASOURCE.tzinfo.localize(
            datetime.datetime(2008, 12, 3, 0, 15, 0))

    rows = [
        (1, ('a', '23', [1, 2, 3]))]

    f = create_copy_from_file(timestamp, modified, rows)

    text = f.read()
    expected = (
        "1\t'2008-12-03T00:15:00+01:00'\t'2008-12-03T00:15:00+01:00'\t"
        "a\t23\t{\"1\",\"2\",\"3\"}\n")

    eq_(text, expected)


def test_create_copy_from_file_text_array():
    timestamp = DATASOURCE.tzinfo.localize(
            datetime.datetime(2008, 12, 3, 0, 15, 0))
    modified = DATASOURCE.tzinfo.localize(
            datetime.datetime(2008, 12, 3, 0, 15, 0))

    rows = [
        (1, ('a', '23', ["a=b,c=d", "e=f,g=h", "i=j,k=l"]))]

    f = create_copy_from_file(timestamp, modified, rows)

    text = f.read()

    expected = (
        "1\t'2008-12-03T00:15:00+01:00'\t'2008-12-03T00:15:00+01:00'\t"
        "a\t23\t{\"a=b,c=d\",\"e=f,g=h\",\"i=j,k=l\"}\n")

    eq_(text, expected)
