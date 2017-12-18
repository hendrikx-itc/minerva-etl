import unittest

from minerva.directory import DataSource, EntityType
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.trendstore import TrendStore


class TestTrendStore(unittest.TestCase):
    def test_constructor(self):
        trend_store = TrendStore(
            id_=42,
            data_source=DataSource(1, 'test-source', 'description'),
            entity_type=EntityType(11, 'TestType', 'description'),
            granularity=create_granularity('1 day'),
        )

        self.assertIsNotNone(trend_store)

# Removed - old version
#
#    def test_base_table_name(self):
#        trend_store = TrendStore(
#            id_=42,
#            name='test-trend-store',
#            data_source=DataSource(1, 'test-source', 'description'),
#            entity_type=EntityType(11, 'TestType', 'description'),
#            granularity=create_granularity('1 day'),
#            trends=[]
#        )
#
#        self.assertEqual(trend_store.table_name(), 'test-trend-store')
