from minerva.test import eq_
from minerva.directory import DataSource, EntityType
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.trendstore import TrendStore


def test_constructor():
    trend_store = TrendStore(
        id_=42,
        data_source=DataSource(1, 'test-source', 'description'),
        entity_type=EntityType(11, 'TestType', 'description'),
        granularity=create_granularity('1 day'),
        trends=[]
    )

    assert trend_store is not None


def test_base_table_name():
    trend_store = TrendStore(
        id_=42,
        data_source=DataSource(1, 'test-source', 'description'),
        entity_type=EntityType(11, 'TestType', 'description'),
        granularity=create_granularity('1 day'),
        trends=[]
    )

    eq_(trend_store.table_name(), 'test-source_TestType_day')
