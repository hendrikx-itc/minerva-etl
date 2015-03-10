from contextlib import closing
from datetime import datetime

import pytz

from minerva.test import with_conn, clear_database, eq_
from minerva.directory import DataSource, EntityType
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.trend import TrendDescriptor
from minerva.storage import datatype
from minerva.storage.trend.tabletrendstore import TableTrendStore, \
    TableTrendStoreDescriptor
from minerva.storage.trend.engine import TrendEngine, filter_existing_trends
from minerva.storage.trend.datapackage import \
    refined_package_type_for_entity_type


@with_conn(clear_database)
def test_store_matching(conn):
    trend_descriptors = [
        TrendDescriptor('x', datatype.Integer, ''),
        TrendDescriptor('y', datatype.Integer, ''),
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

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name("test-type001")(cursor)

        TableTrendStore.create(TableTrendStoreDescriptor(
            data_source, entity_type, granularity, trend_descriptors, 86400
        ))(cursor)

        conn.commit()

        TrendEngine.store(refined_package_type_for_entity_type('test-type001')(
            granularity, timestamp, trend_names, data_rows
        ))(data_source)(conn)

        cursor.execute(
            'SELECT x FROM trend."test-src009_test-type001_qtr" '
            "WHERE timestamp = '2013-01-02T10:45:00+00'"
        )

        rows = cursor.fetchall()

        eq_(len(rows), 11)


@with_conn(clear_database)
def test_store_ignore_extra(conn):
    trend_descriptors = [
        TrendDescriptor('x', datatype.Integer, ''),
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

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name("test-type001")(cursor)

        TableTrendStore.create(TableTrendStoreDescriptor(
            data_source, entity_type, granularity, trend_descriptors, 86400
        ))(cursor)

        conn.commit()

        store_cmd = TrendEngine.store(
            refined_package_type_for_entity_type('test-type001')(
                granularity, timestamp, trend_names, data_rows
            ),
            filter_existing_trends
        )

        store_cmd(data_source)(conn)

        cursor.execute(
            'SELECT x FROM trend."test-src009_test-type001_qtr" '
            "WHERE timestamp = '2013-01-02T10:45:00+00'"
        )

        rows = cursor.fetchall()

        eq_(len(rows), 11)
