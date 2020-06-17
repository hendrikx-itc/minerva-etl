from contextlib import closing
from datetime import datetime, timedelta
import unittest

import pytz
from minerva.storage.trend.datapackage import DataPackage

from minerva.test import connect, clear_database
from minerva.directory import DataSource, EntityType
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.trend import Trend
from minerva.storage import datatype
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.trendstorepart import TrendStorePart
from minerva.storage.trend.engine import TrendEngine
from minerva.test.trend import refined_package_type_for_entity_type


class TestEngine(unittest.TestCase):
    def setUp(self):
        self.conn = clear_database(connect())

    def tearDown(self):
        self.conn.close()

    def test_store_matching(self):
        trend_descriptors = [
            Trend.Descriptor('x', datatype.registry['integer'], ''),
            Trend.Descriptor('y', datatype.registry['integer'], ''),
        ]

        trend_store_part_descr = TrendStorePart.Descriptor(
            'test-trend-store', trend_descriptors
        )

        timestamp = pytz.utc.localize(datetime(2013, 1, 2, 10, 45, 0))

        data_rows = [
            (10023, timestamp, (10023, 2105)),
            (10047, timestamp, (10047, 4906)),
            (10048, timestamp, (10048, 2448)),
            (10049, timestamp, (10049, 5271)),
            (10050, timestamp, (10050, 3693)),
            (10051, timestamp, (10051, 3753)),
            (10052, timestamp, (10052, 2168)),
            (10053, timestamp, (10053, 2372)),
            (10085, timestamp, (10085, 2282)),
            (10086, timestamp, (10086, 1763)),
            (10087, timestamp, (10087, 1453))
        ]
    
        granularity = create_granularity("900s")
        partition_size = timedelta(seconds=86400)
        entity_type_name = "test-type001"

        with self.conn.cursor() as cursor:
            data_source = DataSource.from_name("test-src009")(cursor)
            entity_type = EntityType.from_name(entity_type_name)(cursor)

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                [trend_store_part_descr], partition_size
            ))(cursor)

            trend_store.create_partitions_for_timestamp(self.conn, timestamp)

            self.conn.commit()

            data_package_type = refined_package_type_for_entity_type(entity_type_name)
    
            store_cmd = TrendEngine.store_cmd(
                DataPackage(
                    data_package_type,
                    granularity, trend_descriptors, data_rows
                ),
                {'job': 'test-job'}
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
            Trend.Descriptor('x', datatype.registry['integer'], ''),
        ]

        timestamp = pytz.utc.localize(datetime(2013, 1, 2, 10, 45, 0))

        data_rows = [
            (10023, timestamp, (10023, 2105)),
            (10047, timestamp, (10047, 4906)),
            (10048, timestamp, (10048, 2448)),
            (10049, timestamp, (10049, 5271)),
            (10050, timestamp, (10050, 3693)),
            (10051, timestamp, (10051, 3753)),
            (10052, timestamp, (10052, 2168)),
            (10053, timestamp, (10053, 2372)),
            (10085, timestamp, (10085, 2282)),
            (10086, timestamp, (10086, 1763)),
            (10087, timestamp, (10087, 1453))
        ]

        granularity = create_granularity("900s")
        partition_size = timedelta(seconds=86400)
        data_package_type = refined_package_type_for_entity_type('test-type001')

        with closing(self.conn.cursor()) as cursor:
            data_source = DataSource.from_name("test-src009")(cursor)
            entity_type = EntityType.from_name("test-type001")(cursor)

            parts = [
                TrendStorePart.Descriptor(
                    'test-trend-store', trend_descriptors
                )
            ]

            trend_store = TrendStore.create(TrendStore.Descriptor(
                data_source, entity_type, granularity,
                parts, partition_size
            ))(cursor)
    
            trend_store.create_partitions_for_timestamp(self.conn, timestamp)
    
            self.conn.commit()

            data_package = DataPackage(
                data_package_type,
                granularity, trend_descriptors, data_rows
            )

            store_cmd = TrendEngine.make_store_cmd(
                TrendEngine.filter_existing_trends
            )(data_package, {'job': 'test-job'})
    
            store_cmd(data_source)(self.conn)
    
            cursor.execute(
                'SELECT x FROM trend."test-trend-store" '
                "WHERE timestamp = '2013-01-02T10:45:00+00'"
            )
    
            rows = cursor.fetchall()
    
            self.assertEqual(len(rows), 11)
