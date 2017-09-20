from contextlib import closing
from datetime import datetime
import unittest

import pytz

from minerva.test import connect, clear_database
from minerva.directory import DataSource, EntityType
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.trend import TrendDescriptor
from minerva.storage import datatype
from minerva.storage.trend.tabletrendstore import TableTrendStore
from minerva.storage.trend.engine import TrendEngine
from minerva.storage.trend.datapackage import \
    refined_package_type_for_entity_type


class TestEngine(unittest.TestCase):
    def setUp(self):
        self.conn = clear_database(connect())

    def tearDown(self):
        self.conn.close()

    def test_store_matching(self):
        trend_descriptors = [
            TrendDescriptor('x', datatype.registry['integer'], ''),
            TrendDescriptor('y', datatype.registry['integer'], ''),
        ]
    
        trend_names = [t.name for t in trend_descriptors]
    
        data_rows = [
            (10023, (10023, 2105)),
            (10047, (10047, 4906)),
            (10048, (10048, 2448)),
            (10049, (10049, 5271)),
            (10050, (10050, 3693)),
            (10051, (10051, 3753)),
            (10052, (10052, 2168)),
            (10053, (10053, 2372)),
            (10085, (10085, 2282)),
            (10086, (10086, 1763)),
            (10087, (10087, 1453))
        ]
    
        timestamp = pytz.utc.localize(datetime(2013, 1, 2, 10, 45, 0))
        granularity = create_granularity("900")
    
        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.from_name("test-src009")(cursor)
            entity_type = EntityType.from_name("test-type001")(cursor)
    
            trend_store = TableTrendStore.create(TableTrendStore.Descriptor(
                'test-trend-store', data_source, entity_type, granularity,
                trend_descriptors, 86400
            ))(cursor)
    
            trend_store.partition(timestamp).create(cursor)
    
            self.conn.commit()
    
            store_cmd = TrendEngine.store_cmd(
                refined_package_type_for_entity_type('test-type001')(
                    granularity, timestamp, trend_names, data_rows
                )
            )
    
            store_cmd(data_source)(self.conn)
    
            cursor.execute(
                'SELECT x FROM trend."test-trend-store" '
                "WHERE timestamp = '2013-01-02T10:45:00+00'"
            )
    
            rows = cursor.fetchall()
    
            self.assertEqual(len(rows), 11)

    def test_store_ignore_extra(self):
        """
        Test if extra trends are ignored when configured to ignore
        """
        trend_descriptors = [
            TrendDescriptor('x', datatype.registry['integer'], ''),
        ]
    
        data_rows = [
            (10023, (10023, 2105)),
            (10047, (10047, 4906)),
            (10048, (10048, 2448)),
            (10049, (10049, 5271)),
            (10050, (10050, 3693)),
            (10051, (10051, 3753)),
            (10052, (10052, 2168)),
            (10053, (10053, 2372)),
            (10085, (10085, 2282)),
            (10086, (10086, 1763)),
            (10087, (10087, 1453))
        ]
    
        trend_names = ['x', 'y']
    
        timestamp = pytz.utc.localize(datetime(2013, 1, 2, 10, 45, 0))
        granularity = create_granularity("900")
    
        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.from_name("test-src009")(cursor)
            entity_type = EntityType.from_name("test-type001")(cursor)
    
            trend_store = TableTrendStore.create(TableTrendStore.Descriptor(
                'test-trend-store', data_source, entity_type, granularity,
                trend_descriptors, 86400
            ))(cursor)
    
            trend_store.partition(timestamp).create(cursor)
    
            self.conn.commit()
    
            store_cmd = TrendEngine.make_store_cmd(
                TrendEngine.filter_existing_trends
            )(
                refined_package_type_for_entity_type('test-type001')(
                    granularity, timestamp, trend_names, data_rows
                )
            )
    
            store_cmd(data_source)(self.conn)
    
            cursor.execute(
                'SELECT x FROM trend."test-trend-store" '
                "WHERE timestamp = '2013-01-02T10:45:00+00'"
            )
    
            rows = cursor.fetchall()
    
            self.assertEqual(len(rows), 11)
