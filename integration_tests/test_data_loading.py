# -*- coding: utf-8 -*-
"""
Comprehensive tests of loading data into a Minerva database.
"""
import datetime
from contextlib import closing

import pytz

from minerva.storage.inputdescriptor import InputDescriptor
from minerva.storage.trend.trendstorepart import TrendStorePart
from minerva.test import clear_database
from minerva.directory import EntityType, DataSource
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.trend import Trend
from minerva.storage.trend.datapackage import DataPackage
from minerva.storage import datatype
from minerva.test.trend import package_type_for_entity_type
from minerva.util import merge_dicts


def test_defaults(start_db_container):
    conn = clear_database(start_db_container)

    data_descr = [
        {
            'name': 'x',
            'data_type': 'integer',
            'description': 'some integer value'
        },
        {
            'name': 'y',
            'data_type': 'double precision',
            'description': 'some floating point value'
        }
    ]

    data = [
        ('n=001', [45, 4.332]),
        ('n=002', [10, 1.001]),
        ('n=003', [6089, 133.03]),
        ('n=004', [None, 234.33])
    ]

    input_descriptors = [
        InputDescriptor.load(d) for d in data_descr
    ]

    timestamp = pytz.utc.localize(datetime.datetime(2015, 2, 24, 20, 0))

    parsed_data = [
        (dn, timestamp, [
            descriptor.parse(raw_value)
            for raw_value, descriptor
            in zip(values, input_descriptors)
        ])
        for dn, values in data
    ]

    partition_size = datetime.timedelta(seconds=3600)
    granularity = create_granularity("900 second")

    trend_descriptors = [
        Trend.Descriptor(d['name'], datatype.registry[d['data_type']], d['description'])
        for d in data_descr
    ]

    table_trend_store_part_descr = TrendStorePart.Descriptor(
        'test-trend-store', trend_descriptors
    )

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create("test_type", '')(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity,
            [table_trend_store_part_descr], partition_size
        ))(cursor)

    conn.commit()

    data_package_type = package_type_for_entity_type('test_type')

    data_package = DataPackage(
        data_package_type,
        granularity,
        trend_descriptors,
        parsed_data
    )

    trend_store.create_partitions_for_timestamp(conn, timestamp)

    job_id = 10

    trend_store.store(data_package, job_id)(conn)

    conn.commit()

    # Check outcome

    query = (
        'SELECT x, y '
        'FROM trend."{}" '
        'ORDER BY entity_id'
    ).format(trend_store.part_by_name['test-trend-store'].base_table_name())

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)

        rows = cursor.fetchall()

    assert len(rows) == 4

    assert rows[3][0] is None


def test_nulls(start_db_container):
    conn = clear_database(start_db_container)

    data_descr = [
        {
            'name': 'x',
            'data_type': 'integer',
            'description': 'some integer value',
        },
        {
            'name': 'y',
            'data_type': 'double precision',
            'description': 'some floating point value',
        }
    ]

    input_descr = [
        {
            'name': 'y',
            'parser_config': {
                'null_value': 'NULL'
            }

        }
    ]

    input_descr_lookup = {d['name']: d for d in input_descr}

    data = [
        ('test_nulls=001', ['45', '4.332']),
        ('test_nulls=002', ['10', '1.001']),
        ('test_nulls=003', ['6089', '133.03']),
        ('test_nulls=004', ['', 'NULL'])
    ]

    input_descriptors = [
        InputDescriptor.load(
            merge_dicts(d, input_descr_lookup.get(d['name']))
        )
        for d in data_descr
    ]

    timestamp = pytz.utc.localize(datetime.datetime(2015, 2, 24, 20, 0))

    parsed_data = [
        (dn, timestamp, [
            descriptor.parse(raw_value)
            for raw_value, descriptor
            in zip(values, input_descriptors)
        ])
        for dn, values in data
    ]

    partition_size = datetime.timedelta(seconds=3600)
    granularity = create_granularity("900 second")

    trend_descriptors = [
        Trend.Descriptor(
            d['name'],
            datatype.registry[d['data_type']],
            d['description']
        )
        for d in data_descr
    ]

    table_trend_store_part = TrendStorePart.Descriptor(
        'test-trend-store', trend_descriptors
    )

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-source", '')(cursor)
        entity_type = EntityType.create("test_type", '')(cursor)

        trend_store = TrendStore.create(TrendStore.Descriptor(
            data_source, entity_type, granularity,
            [table_trend_store_part], partition_size
        ))(cursor)

    conn.commit()

    data_package_type = package_type_for_entity_type('test_type')

    data_package = DataPackage(
        data_package_type,
        granularity,
        trend_descriptors,
        parsed_data
    )

    trend_store.create_partitions_for_timestamp(conn, timestamp)

    job_id = 11

    trend_store.store(data_package, job_id)(conn)

    conn.commit()

    # Check outcome

    query = (
        'SELECT x, y '
        'FROM trend."{}" '
        'ORDER BY entity_id'
    ).format(trend_store.part_by_name['test-trend-store'].base_table_name())

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)

        rows = cursor.fetchall()

    assert len(rows) == 4

    assert rows[3][0] is None
