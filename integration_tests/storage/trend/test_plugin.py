from contextlib import closing
from datetime import datetime, timedelta

from nose.tools import eq_, raises, assert_not_equal, assert_equal, \
    assert_false, assert_true
import pytz

from minerva.directory import DataSource
from minerva.test import with_conn
from minerva.storage import get_plugin
from minerva.util import head
from minerva.storage.trend.plugin import TrendPlugin

from minerva_db import clear_database
from data import TestData


def test_load_plugin():
    plugin = get_plugin("trend")

    assert_not_equal(plugin, None)


@with_conn
def test_create_default_instance(conn):
    """
    By default, a V3 API should be returned.
    """
    plugin = get_plugin("trend")

    instance = plugin(conn)

    eq_(instance.api_version(), 3)


@with_conn
def test_create_v3_instance(conn):
    plugin = get_plugin("trend")

    v3_instance = plugin(conn, api_version=3)

    eq_(v3_instance.api_version(), 3)


@with_conn
def test_create_v4_instance(conn):
    plugin = get_plugin("trend")

    v3_instance = plugin(conn, api_version=4)

    eq_(v3_instance.api_version(), 4)


@raises(Exception)
@with_conn
def test_unsupported_api_version(conn):
    plugin = get_plugin("trend")

    plugin(conn, api_version=42)


@with_conn
def test_retrieve_from_v4_trendstore(conn):
    plugin = get_plugin("trend")
    data = TestData()

    plugin_obj = plugin(conn, api_version=3)

    with closing(conn.cursor()) as cursor:
        clear_database(cursor)
        data.load(cursor)

    start = data.timestamp_1
    end = data.timestamp_1
    entity = data.entities[1]
    entities = [entity.id]

    column_names = [
        "CellID",
        "CCR",
        "CCRatts",
        "Drops"]

    datasources = [data.datasource_a]
    entitytype = data.entitytype
    granularity = 900

    r = plugin_obj.retrieve(
        datasources, granularity, entitytype, column_names, entities, start,
        end
    )

    eq_(len(r), 1)

    first_result = head(r)

    entity_id, timestamp, c1, c2, c3, c4 = first_result

    eq_(entity_id, entity.id)
    eq_(c4, 18)


@with_conn
def test_store_raw1(conn):
    plugin = TrendPlugin(conn)

    with closing(conn.cursor()) as cursor:
        clear_database(cursor)

        datasource = DataSource.from_name(cursor, "test_source001")

    conn.commit()

    granularity = 3600

    timestamp = "2012-04-19T11:00:00"

    trend_names = ["COUNTER1", "COUNTER2", "COUNTER3"]

    rows = [
        ("Network=dummy,Subnetwork=test,Element=1", ("1", "2", "3"))
    ]

    plugin.store_raw(datasource, granularity, timestamp, trend_names,
            rows)


@with_conn
def test_store_raw_fractured_small(conn):
    plugin = TrendPlugin(conn)

    with closing(conn.cursor()) as cursor:
        clear_database(cursor)

        datasource = DataSource.from_name(cursor, "test_source002")

    conn.commit()

    granularity = 3600

    timestamp = "2012-04-19T11:00:00"

    trend_names_part_1 = ["PART1_COUNTER1", "PART1_COUNTER2", "PART1_COUNTER3"]

    raw_data_rows = [
        ("Network=dummy,Subnetwork=test,Element=1", ("1", "2", "3"))
    ]

    plugin.store_raw(
        datasource, granularity, timestamp, trend_names_part_1, raw_data_rows
    )

    trend_names_part_2 = ["PART2_COUNTER1", "PART2_COUNTER2", "PART2_COUNTER3"]

    raw_data_rows = [
        ("Network=dummy,Subnetwork=test,Element=1", ("4", "5", "6"))
    ]

    plugin.store_raw(
        datasource, granularity, timestamp, trend_names_part_2, raw_data_rows
    )


@with_conn
def test_store_raw_fractured_large(conn):
    plugin = TrendPlugin(conn)

    with closing(conn.cursor()) as cursor:
        clear_database(cursor)

        datasource = DataSource.from_name(cursor, "test_source003")

    conn.commit()

    granularity = 3600

    timestamp = "2012-04-19T11:00:00"

    trend_names_part_1 = ["PART1_COUNTER1", "PART1_COUNTER2", "PART1_COUNTER3"]

    dn_template = "Network=dummy,Subnetwork=test,Element={}"

    raw_data_rows_part_1 = [
        (dn_template.format(i), ("1", "2", "3"))
        for i in range(100)
    ]

    plugin.store_raw(
        datasource, granularity, timestamp, trend_names_part_1,
        raw_data_rows_part_1
    )

    trend_names_part_2 = ["PART2_COUNTER1", "PART2_COUNTER2", "PART2_COUNTER3"]

    raw_data_rows_part_2 = [
        (dn_template.format(i), ("4", "5", "6"))
        for i in range(100)
    ]

    plugin.store_raw(
        datasource, granularity, timestamp, trend_names_part_2,
        raw_data_rows_part_2
    )


def test_most_recent_timestamp():
    """
    Test `get_most_recent_timestamp` static method of trend plugin
    """
    plugin_v3 = get_plugin("trend")
    tz = pytz.timezone("Europe/Amsterdam")

    ts = tz.localize(datetime(2012, 10, 8, 2, 42, 0))
    most_recent_timestamp = tz.localize(datetime(2012, 10, 8, 2, 0, 0))
    granularity = 3600

    assert_equal(
        plugin_v3.get_most_recent_timestamp(ts, granularity),
        most_recent_timestamp
    )

    ts = tz.localize(datetime(2012, 10, 8, 2, 42, 0))
    most_recent_timestamp = tz.localize(datetime(2012, 10, 8, 2, 0, 0))
    granularity = 3600

    assert_equal(
        plugin_v3.get_most_recent_timestamp(ts, granularity),
        most_recent_timestamp
    )

    ts = tz.localize(datetime(2012, 10, 29, 0, 0, 0))
    most_recent_timestamp = tz.localize(datetime(2012, 10, 29, 0, 0, 0))
    granularity = 604800

    assert_equal(
        plugin_v3.get_most_recent_timestamp(ts, granularity),
        most_recent_timestamp
    )

    ts = tz.localize(datetime(2012, 10, 28, 23, 59, 59))
    most_recent_timestamp = tz.localize(datetime(2012, 10, 22, 0, 0, 0))
    granularity = 604800

    assert_equal(
        plugin_v3.get_most_recent_timestamp(ts, granularity),
        most_recent_timestamp
    )

    ts = tz.localize(datetime(2012, 10, 8, 0, 0, 0)) - timedelta(0, 1)
    most_recent_timestamp = tz.localize(datetime(2012, 10, 1, 0, 0, 0))
    granularity = 604800

    assert_equal(
        plugin_v3.get_most_recent_timestamp(ts, granularity),
        most_recent_timestamp
    )

    ts = tz.localize(datetime(2012, 10, 9, 2, 30, 0))
    most_recent_timestamp = tz.localize(datetime(2012, 10, 9, 0, 0, 0))
    granularity = 86400

    assert_equal(
        plugin_v3.get_most_recent_timestamp(ts, granularity),
        most_recent_timestamp
    )

    ts = tz.localize(datetime(2012, 10, 29, 0, 0, 0))
    most_recent_timestamp = tz.localize(datetime(2012, 10, 29, 0, 0, 0))

    for granularity in [900, 3600, 86400, 604800]:
        assert_equal(
            plugin_v3.get_most_recent_timestamp(ts, granularity),
            most_recent_timestamp
        )

    ts = pytz.utc.localize(datetime(2012, 10, 9, 0, 14, 0))
    loc_ts = ts.astimezone(tz)
    timestamp = tz.localize(datetime(2012, 10, 9, 2, 0, 0))
    granularity = 86400

    assert_false(
        timestamp <= plugin_v3.get_most_recent_timestamp(loc_ts, granularity)
    )

    ts = pytz.utc.localize(datetime(2012, 10, 9, 9, 14, 0))
    loc_ts = ts.astimezone(tz)
    timestamp = tz.localize(datetime(2012, 10, 9, 11, 0, 0))
    granularity = 3600

    assert_true(
        timestamp <= plugin_v3.get_most_recent_timestamp(loc_ts, granularity)
    )

    # DST switch on oct 28th
    ts = tz.localize(datetime(2012, 10, 28, 17, 42, 0))
    most_recent_timestamp = tz.localize(datetime(2012, 10, 28, 0, 0, 0))
    granularity = 86400

    assert_equal(
        plugin_v3.get_most_recent_timestamp(ts, granularity),
        most_recent_timestamp
    )

    ts_utc = pytz.utc.localize(datetime(2013, 2, 25, 23, 0, 0))
    most_recent_timestamp = tz.localize(datetime(2013, 2, 26, 0, 0, 0))
    granularity = 86400

    assert_equal(
        plugin_v3.get_most_recent_timestamp(ts_utc, granularity, minerva_tz=tz),
        most_recent_timestamp
    )

    # DST switch on oct 28th
    ts_utc = pytz.utc.localize(datetime(2012, 10, 28, 16, 42, 0))
    most_recent_timestamp = tz.localize(datetime(2012, 10, 28, 0, 0, 0))
    granularity = 86400

    assert_equal(
        plugin_v3.get_most_recent_timestamp(ts_utc, granularity, minerva_tz=tz),
        most_recent_timestamp
    )
