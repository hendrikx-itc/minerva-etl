import datetime
from contextlib import closing

from nose.tools import eq_

from minerva.test import with_conn
from minerva.directory import DataSource
from minerva.storage.trend.engine import TrendEngine
from minerva.storage.trend.trendstore import TrendStore, TrendStoreDescriptor
from minerva.storage.trend.view import View

from minerva_db import clear_database
from data import TestSet1Small


@with_conn(clear_database)
def test_create_view(conn):
    test_set_small = TestSet1Small()

    with closing(conn.cursor()) as cursor:
        test_set_small.load(cursor)

        data_source = DataSource.from_name("view-test")(cursor)

        trend_store = TrendStore.get(
            cursor, data_source, test_set_small.entitytype,
            test_set_small.granularity
        )

        if not trend_store:
            trend_store = TrendStore.create(TrendStoreDescriptor(
                data_source, test_set_small.entitytype,
                test_set_small.granularity, [], partition_size=86400
            ))(cursor)

        view_query = (
            "SELECT "
            "999 AS entity_id, "
            "'2013-08-26 13:00:00+02:00'::timestamp with time zone AS timestamp, "
            '10 AS "CntrA"'
        )

        View(trend_store, view_query).define(cursor).create(cursor)

    conn.commit()

    engine = TrendEngine(conn)

    start = test_set_small.datasource.tzinfo.localize(
        datetime.datetime(2013, 8, 26, 13, 0, 0)
    )
    end = start

    result = engine.retrieve(trend_store, ["CntrA"], None, start, end)

    eq_(len(result), 1)

