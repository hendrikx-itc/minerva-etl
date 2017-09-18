# -*- coding: utf-8 -*-
import unittest

from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.rawdatapackage import RawDataPackage

TIMEZONE = "Europe/Amsterdam"


class TestRawDataPackage(unittest.TestCase):
    def test_constructor(self):
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

        raw_datapackage = RawDataPackage(
            granularity, timestamp, trend_names, rows
        )

        self.assertTrue(raw_datapackage is not None)

    def test_merge_packages(self):
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

        raw_data_package_1 = RawDataPackage(
                granularity, timestamp, trend_names, rows)

        trend_names = ["counter_d", "counter_e"]
        rows = [
            ("Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott1", ("2", "0.003")),
            ("Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott2", ("0", "0.090"))
        ]

        raw_data_package_2 = RawDataPackage(
            granularity, timestamp, trend_names, rows
        )

        packages = [raw_data_package_1, raw_data_package_2]

        merged_packages = RawDataPackage.merge_packages(packages)

        self.assertEqual(len(merged_packages), 1)
