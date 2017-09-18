import datetime
from contextlib import closing
import unittest

import pytz

from minerva.test import connect, clear_database
from minerva.test.trend import TestSet1Small
from minerva.directory import DataSource
from minerva.storage.trend.viewtrendstore import \
        ViewTrendStore, ViewTrendStoreDescriptor


class TestViews(unittest.TestCase):
    def setUp(self):
        self.conn = clear_database(connect())

    def tearDown(self):
        self.conn.close()

    def test_create_view(self):
        test_set_small = TestSet1Small()

        with closing(self.conn.cursor()) as cursor:
            test_set_small.load(cursor)

            data_source = DataSource.from_name("view-test")(cursor)

            view_query = (
                "SELECT "
                "999 AS entity_id, "
                "'2013-08-26 13:00:00+02:00'::timestamptz AS timestamp, "
                '10 AS "CntrA"'
            )

            trend_store = ViewTrendStore.create(ViewTrendStoreDescriptor(
                'test-view-trend-store',
                data_source, test_set_small.entity_type,
                test_set_small.granularity, view_query
            ))(cursor)

            start = pytz.utc.localize(
                datetime.datetime(2013, 8, 26, 13, 0, 0)
            )
            end = start

            trend_store.retrieve(["CntrA"]).execute(cursor)

            result = cursor.fetchall()

        self.assertEqual(len(result), 1)
