# -*- coding: utf-8 -*-
from datetime import datetime
from operator import contains
from functools import partial
import unittest

import pytz
from minerva.directory.entityref import EntityIdRef

from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.datapackage import DataPackage, DataPackageType
from minerva.storage.trend.trend import Trend
from minerva.util import k


def refined_package_type_for_entity_type(type_name: str) -> DataPackageType:
    def identifier():
        return None

    get_entity_type_name = k(type_name)

    return DataPackageType(identifier, EntityIdRef, get_entity_type_name)


class TestDataPackage(unittest.TestCase):
    def test_constructor(self):
        granularity = create_granularity("900s")
        timestamp = "2013-05-28 12:00:00"
        trends = [
            Trend.Descriptor('counter_a', 'integer', ''),
            Trend.Descriptor('counter_b', 'numeric', ''),
            Trend.Descriptor('counter_c', 'integer', ''),
        ]

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

        data_package_type = refined_package_type_for_entity_type('Node')

        data_package = DataPackage(
            data_package_type,
            granularity, trends, rows
        )

        self.assertIsNotNone(data_package)

    def test_merge_packages(self):
        granularity = create_granularity("900s")
        timestamp = "2013-05-28 12:00:00"

        trends = [
            Trend.Descriptor('counter_a', 'integer', ''),
            Trend.Descriptor('counter_b', 'numeric', ''),
            Trend.Descriptor('counter_c', 'integer', ''),
        ]
        rows = [
            (
                "Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott1",
                timestamp,
                ("34", "10.3", "334303")
            ),
            (
                "Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott2",
                timestamp,
                ("42", "8.5", "206441")
            )
        ]

        data_package_type = refined_package_type_for_entity_type('Node')

        data_package_1 = DataPackage(
            data_package_type,
            granularity, trends, rows
        )

        trends = [
            Trend.Descriptor('counter_d', 'integer', ''),
            Trend.Descriptor('counter_e', 'numeric', '')
        ]

        rows = [
            (
                "Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott1",
                timestamp,
                ("2", "0.003")
            ),
            (
                "Network=SouthPole,Rnc=SP1,Rbs=AdmundsenScott2",
                timestamp,
                ("0", "0.090")
            )
        ]

        data_package_2 = DataPackage(
            data_package_type,
            granularity, trends, rows
        )

        packages = [data_package_1, data_package_2]

        merged_packages = DataPackage.merge_packages(packages)

        self.assertEqual(len(merged_packages), 1)

    def test_filter_trends(self):
        data_package_type = refined_package_type_for_entity_type('Node')

        timestamp = pytz.utc.localize(datetime(2015, 2, 25, 10, 0, 0))

        trends = [
            Trend.Descriptor('x', 'integer', ''),
            Trend.Descriptor('y', 'integer', ''),
            Trend.Descriptor('z', 'integer', '')
        ]

        package = DataPackage(
            data_package_type,
            create_granularity("900s"),
            trends,
            [
                ('Node=001', timestamp, (11, 12, 13)),
                ('Node=002', timestamp, (21, 22, 23)),
                ('Node=003', timestamp, (31, 32, 33)),
                ('Node=004', timestamp, (41, 42, 43))
            ]
        )

        filtered_package = package.filter_trends(partial(contains, {'x', 'z'}))

        self.assertEqual(len(filtered_package.trend_descriptors), 2)

        self.assertEqual(tuple(td.name for td in filtered_package.trend_descriptors), ('x', 'z'))

        self.assertEqual(filtered_package.rows[3], ('Node=004', timestamp, (41, 43)))

    def test_split(self):
        data_package_type = refined_package_type_for_entity_type('Node')
        timestamp = pytz.utc.localize(datetime(2015, 2, 25, 10, 0, 0))
        trends = [
            Trend.Descriptor('a', 'integer', ''),
            Trend.Descriptor('b', 'integer', ''),
            Trend.Descriptor('c', 'integer', ''),
            Trend.Descriptor('d', 'integer', ''),
            Trend.Descriptor('e', 'integer', ''),
        ]

        package = DataPackage(
            data_package_type,
            create_granularity('900s'),
            trends,
            [
                ('Node=001', timestamp, (11, 12, 13, 14, 15)),
                ('Node=002', timestamp, (21, 22, 23, 24, 25)),
                ('Node=003', timestamp, (31, 32, 33, 34, 35)),
                ('Node=004', timestamp, (41, 42, 43, 44, 45))
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
                self.assertEqual(len(package.trend_descriptors), 2, 'blue package should have 2 trends')
            elif color == 'red':
                self.assertEqual(len(package.trend_descriptors), 2, 'red package should have 2 trends')
            elif color == 'green':
                self.assertEqual(len(package.trend_descriptors), 1, 'green package should have 1 trends')
