import datetime
from contextlib import closing

from nose.tools import eq_

from minerva.util import head
from minerva.test import with_conn
from minerva.directory.helpers_v4 import name_to_datasource

from minerva.storage import get_plugin
from minerva_storage_trend.trendstore import TrendStore
from minerva_storage_trend.view import View

from minerva_db import clear_database
from data import TestSet1Small


@with_conn(clear_database)
def test_create_view(conn):
    testset_small = TestSet1Small()

    with closing(conn.cursor()) as cursor:
        testset_small.load(cursor)

        datasource = name_to_datasource(cursor, "view-test")

        trendstore = TrendStore.get(cursor, datasource, testset_small.entitytype,
                testset_small.granularity)

        if not trendstore:
            trendstore = TrendStore(datasource, testset_small.entitytype,
                    testset_small.granularity, partition_size=86400,
                    type="view").create(cursor)

        view_sql = (
            "SELECT "
            "999 AS entity_id, "
            "'2013-08-26 13:00:00+02:00'::timestamp with time zone AS timestamp, "
            '10 AS "CntrA"')

        view = View(trendstore, view_sql).define(cursor).create(cursor)

    conn.commit()

    plugin = get_plugin("trend")

    instance_v4 = plugin(conn, api_version=4)

    start = testset_small.datasource.tzinfo.localize(datetime.datetime(2013, 8, 26, 13, 0, 0))
    end = start

    result = instance_v4.retrieve(trendstore, ["CntrA"], None, start, end)

    eq_(len(result), 1)

