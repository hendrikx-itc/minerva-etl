# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.datapackage import DataPackage, DefaultPackageType


def test_constructor():
    granularity = create_granularity("900")
    timestamp = "2013-05-28 12:00:00"
    trend_names = ["counter_a", "counter_b", "counter_c"]

    rows = [
        (
            "Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott1",
            ("34", "10.3", "334303")
        ),
        (
            "Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott2",
            ("42", "8.5", "206441")
        )
    ]

    data_package = DefaultPackageType(
        granularity, timestamp, trend_names, rows
    )

    assert data_package is not None


def test_merge_packages():
    granularity = create_granularity("900")
    timestamp = "2013-05-28 12:00:00"

    trend_names = ["counter_a", "counter_b", "counter_c"]
    rows = [
        (
            "Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott1",
            ("34", "10.3", "334303")
        ),
        (
            "Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott2",
            ("42", "8.5", "206441")
        )
    ]

    data_package_1 = DefaultPackageType(
        granularity, timestamp, trend_names, rows
    )

    trend_names = ["counter_d", "counter_e"]
    rows = [
        (
            "Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott1",
            ("2", "0.003")
        ),
        (
            "Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott2",
            ("0", "0.090")
        )
    ]

    data_package_2 = DefaultPackageType(
        granularity, timestamp, trend_names, rows
    )

    packages = [data_package_1, data_package_2]

    merged_packages = DataPackage.merge_packages(packages)

    assert len(merged_packages) == 1
