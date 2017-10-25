# -*- coding: utf-8 -*-
from datetime import datetime
from operator import contains
from functools import partial
import unittest

import pytz

from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.datapackage import DataPackage, DefaultPackage


class TestDataPackage(unittest.TestCase):
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

        data_package = DefaultPackage(
            granularity, timestamp, trend_names, rows
        )

        self.assertIsNotNone(data_package)

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

        data_package_1 = DefaultPackage(
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

        data_package_2 = DefaultPackage(
            granularity, timestamp, trend_names, rows
        )

        packages = [data_package_1, data_package_2]

        merged_packages = DataPackage.merge_packages(packages)

        self.assertEqual(len(merged_packages), 1)

    def test_filter_trends(self):
        package = DefaultPackage(
            create_granularity("900"),
            pytz.utc.localize(datetime(2015, 2, 25, 10, 0, 0)),
            ['x', 'y', 'z'],
            [
                ('Node=001', (11, 12, 13)),
                ('Node=002', (21, 22, 23)),
                ('Node=003', (31, 32, 33)),
                ('Node=004', (41, 42, 43))
            ]
        )

        filtered_package = package.filter_trends(partial(contains, {'x', 'z'}))

        self.assertEqual(len(filtered_package.trend_names), 2)

        self.assertEqual(filtered_package.trend_names, ('x', 'z'))

        self.assertEqual(filtered_package.rows[3], ('Node=004', (41, 43)))

    def test_split(self):
        package = DefaultPackage(
            create_granularity("900"),
            pytz.utc.localize(datetime(2015, 2, 25, 10, 0, 0)),
            ['a', 'b', 'c', 'd', 'e'],
            [
                ('Node=001', (11, 12, 13, 14, 15)),
                ('Node=002', (21, 22, 23, 24, 25)),
                ('Node=003', (31, 32, 33, 34, 35)),
                ('Node=004', (41, 42, 43, 44, 45))
            ]
        )

        group_dict = {
            'a': 'blue',
            'b': 'red',
            'c': 'green',
            'd': 'blue',
            'e': 'red'
        }

        def color_group(trend_name):
            return group_dict[trend_name]

        packages = list(package.split(color_group))

        self.assertEqual(len(packages), 3)

        for color, package in packages:
            if color == 'blue':
                self.assertEqual(len(package.trend_names), 2, 'blue package should have 2 trends')
            elif color == 'red':
                self.assertEqual(len(package.trend_names), 2, 'red package should have 2 trends')
            elif color == 'green':
                self.assertEqual(len(package.trend_names), 1, 'green package should have 1 trends')
