from contextlib import closing
from datetime import datetime

import pytz

from nose.tools import eq_, assert_not_equal

from minerva.directory.helpers_v4 import name_to_datasource, name_to_entitytype
from minerva.test import with_conn

from minerva.storage import get_plugin
from minerva.storage.trend.plugin_v4 import TrendPlugin
from minerva.storage.trend.datapackage import DataPackage
from minerva.storage.trend.granularity import create_granularity

from minerva_db import clear_database


def test_load_plugin():
    plugin = get_plugin("trend")

    assert_not_equal(plugin, None)


@with_conn
def test_create_v4_instance(conn):
    """
    A V4 API should be returned when specifying api_version 4.
    """
    plugin = get_plugin("trend")

    instance = plugin(conn, api_version=4)

    eq_(instance.api_version(), 4)


def test_create_granularity():
    plugin = get_plugin("trend")

    instance = plugin(None, api_version=4)

    granularity = instance.create_granularity(3600)

    eq_(granularity.name, "3600")


@with_conn
def test_get_trendstore(conn):
    plugin = get_plugin("trend")

    instance = plugin(conn, api_version=4)

    granularity = create_granularity("900")

    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, "test-src")
        entitytype = name_to_entitytype(cursor, "test-type")

        instance.TrendStore(
            datasource, entitytype,
            granularity, 86400, "table").create(cursor)

    trendstore = instance.get_trendstore(datasource, entitytype, granularity)

    assert_not_equal(trendstore, None)


def test_create_datapackage():
    plugin = get_plugin("trend")

    instance = plugin(None, api_version=4)

    granularity = instance.create_granularity(3600)
    timestamp = pytz.utc.localize(datetime.now())
    trend_names = ["a", "b", "c"]
    rows = [(123, [1, 2, 3])]

    datapackage = instance.DataPackage(
        granularity, timestamp, trend_names, rows)

    assert datapackage is not None


@with_conn
def test_store_raw1(conn):
    plugin = TrendPlugin(conn)

    with closing(conn.cursor()) as cursor:
        clear_database(cursor)

        datasource = name_to_datasource(cursor, "test_source004")

    conn.commit()

    granularity = 3600

    timestamp = "2012-04-19T11:00:00"

    trend_names = ["COUNTER1", "COUNTER2", "COUNTER3"]

    rows = [
        ("Network=dummy,Subnetwork=test,Element=1", ("1", "2", "3"))
    ]

    raw_datapackage = DataPackage(granularity, timestamp, trend_names, rows)

    plugin.store_raw(datasource, raw_datapackage)


@with_conn
def test_store_raw_fractured_small(conn):
    plugin = TrendPlugin(conn)

    with closing(conn.cursor()) as cursor:
        clear_database(cursor)

        datasource = name_to_datasource(cursor, "test-source005")

    conn.commit()

    granularity = 3600

    timestamp = "2012-04-19T11:00:00"

    trend_names_part_1 = ["PART1_COUNTER1", "PART1_COUNTER2", "PART1_COUNTER3"]

    raw_data_rows = [
        ("Network=dummy,Subnetwork=test,Element=1", ("1", "2", "3"))
    ]

    raw_datapackage_1 = DataPackage(
        granularity, timestamp, trend_names_part_1, raw_data_rows)

    plugin.store_raw(datasource, raw_datapackage_1)

    trend_names_part_2 = ["PART2_COUNTER1", "PART2_COUNTER2", "PART2_COUNTER3"]

    raw_data_rows = [
        ("Network=dummy,Subnetwork=test,Element=1", ("4", "5", "6"))
    ]

    raw_datapackage_2 = DataPackage(
        granularity, timestamp, trend_names_part_2, raw_data_rows)

    plugin.store_raw(datasource, raw_datapackage_2)


@with_conn
def test_store_raw_fractured_large(conn):
    plugin = TrendPlugin(conn)

    with closing(conn.cursor()) as cursor:
        clear_database(cursor)

        datasource = name_to_datasource(cursor, "test-source006")

    conn.commit()

    granularity = 3600

    timestamp = "2012-04-19T11:00:00"

    trend_names_part_1 = ["PART1_COUNTER1", "PART1_COUNTER2", "PART1_COUNTER3"]

    dn_template = "Network=dummy,Subnetwork=test,Element={}"

    raw_data_rows_part_1 = [
        (dn_template.format(i), ("1", "2", "3"))
        for i in range(100)]

    raw_datapackage_1 = DataPackage(
        granularity, timestamp, trend_names_part_1, raw_data_rows_part_1)

    plugin.store_raw(datasource, raw_datapackage_1)

    trend_names_part_2 = ["PART2_COUNTER1", "PART2_COUNTER2", "PART2_COUNTER3"]

    raw_data_rows_part_2 = [
        (dn_template.format(i), ("4", "5", "6"))
        for i in range(100)]

    raw_datapackage_2 = DataPackage(
        granularity, timestamp, trend_names_part_2, raw_data_rows_part_2)

    plugin.store_raw(datasource, raw_datapackage_2)


class EntitySelection(object):

    def __init__(self, entity_ids):
        self.entity_ids = entity_ids

    def fill_temp_table(self, cursor, name):
        query = (
            "INSERT INTO {}(entity_id) "
            "VALUES (%s)").format(name)

        for entity_id in self.entity_ids:
            cursor.execute(query, (entity_id,))

    def create_temp_table(self, cursor, name):
        """
        Create and fill temp table with entity Ids
        """
        tmp_table_query = (
            "CREATE TEMP TABLE {}(entity_id integer NOT NULL) "
            "ON COMMIT DROP").format(name)

        cursor.execute(tmp_table_query)

        self.fill_temp_table(cursor, name)
