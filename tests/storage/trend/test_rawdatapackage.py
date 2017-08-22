# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from nose.tools import assert_true, eq_

from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.rawdatapackage import RawDataPackage

TIMEZONE = "Europe/Amsterdam"


def test_constructor():
    granularity = create_granularity("900")
    timestamp = "2013-05-28 12:00:00"
    trend_names = ["counter_a", "counter_b", "counter_c"]

    rows = [
        ("Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott1", (
            "34", "10.3", "334303")),
        ("Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott2", (
            "42", "8.5", "206441"))]

    raw_datapackage = RawDataPackage(granularity, timestamp, trend_names, rows)

    assert_true(raw_datapackage is not None)


def test_merge_packages():
    granularity = create_granularity("900")
    timestamp = "2013-05-28 12:00:00"

    trend_names = ["counter_a", "counter_b", "counter_c"]
    rows = [
        ("Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott1", (
            "34", "10.3", "334303")),
        ("Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott2", (
            "42", "8.5", "206441"))]

    raw_datapackage_1 = RawDataPackage(
        granularity, timestamp, trend_names, rows)

    trend_names = ["counter_d", "counter_e"]
    rows = [
        ("Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott1", ("2", "0.003")),
        ("Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott2", ("0", "0.090"))]

    raw_datapackage_2 = RawDataPackage(
        granularity, timestamp, trend_names, rows)

    packages = [raw_datapackage_1, raw_datapackage_2]

    merged_packages = RawDataPackage.merge_packages(packages)

    eq_(len(merged_packages), 1)
