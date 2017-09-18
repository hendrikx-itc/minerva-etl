# -*- coding: utf-8 -*-
import unittest

from minerva.directory.basetypes import DataSource, EntityType
from minerva.storage.trend.types_v4 import TrendStore
from minerva.storage.trend.granularity import create_granularity


TIMEZONE = "Europe/Amsterdam"


class TypesV4(unittest.TestCase):
    def test_trend_store(self):
        data_source = DataSource(
            id=10, name="test-src",
            description="this is just a test data source", timezone=TIMEZONE
        )
        entity_type = EntityType(
            id=20, name="test_type",
            description="this is just a test entity type"
        )
        granularity = create_granularity("900")

        trend_store = TrendStore(
            10, 'test_trendstore', data_source, entity_type, granularity, []
        )

        self.assertIsNotNone(trend_store)