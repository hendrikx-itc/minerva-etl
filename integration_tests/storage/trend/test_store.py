import time
from datetime import timedelta, datetime
from contextlib import closing

import pytz

from minerva.util import first
from minerva.db.query import Table, Call, Column, Eq, And
from minerva.db.error import DataTypeMismatch
from minerva.test import with_conn, clear_database, assert_not_equal, raises, \
    with_dataset, eq_
from minerva.directory import DataSource, EntityType
from minerva.storage import datatype
from minerva.storage.trend.test import DataSet
from minerva.storage.trend.datapackage import refined_package_type_for_entity_type
from minerva.storage.trend.trendstore import TrendStore, TrendStoreDescriptor
from minerva.storage.trend.trend import TrendDescriptor
from minerva.storage.trend.partitioning import Partitioning
from minerva.storage.trend.granularity import create_granularity



modified_table = Table("trend_directory", "modified")


@with_conn(clear_database)
def test_store_copy_from_1(conn):
    trend_names = ['CellID', 'CCR', 'CCRatts', 'Drops']

    data_rows = [
        (10023, (10023, 0.9919, 2105, 17)),
        (10047, (10047, 0.9963, 4906, 18)),
        (10048, (10048, 0.9935, 2448, 16)),
        (10049, (10049, 0.9939, 5271, 32)),
        (10050, (10050, 0.9940, 3693, 22)),
        (10051, (10051, 0.9944, 3753, 21)),
        (10052, (10052, 0.9889, 2168, 24)),
        (10053, (10053, 0.9920, 2372, 19)),
        (10085, (10085, 0.9987, 2282, 3)),
        (10086, (10086, 0.9972, 1763, 5)),
        (10087, (10087, 0.9931, 1453, 10))
    ]

    data_types = datatype.deduce_data_types(data_rows)
    timestamp = pytz.utc.localize(datetime(2013, 1, 2, 10, 45, 0))
    granularity = create_granularity("900")
    modified = pytz.utc.localize(datetime.now())

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name("test-type001")(cursor)

        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_source, entity_type, granularity,
            [
                TrendDescriptor(trend_name, data_type, '')
                for trend_name, data_type in zip(trend_names, data_types)
            ],
            86400
        ))(cursor)

        partition = trend_store.partition(timestamp)
        partition.create(cursor)

        table = partition.table()

        trend_store.store_copy_from(
            refined_package_type_for_entity_type('Node')(
                granularity, timestamp, trend_names, data_rows
            ),
            modified
        )(cursor)

        eq_(row_count(cursor, table), 11)

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        eq_(max_modified, modified)


@raises(DataTypeMismatch)
@with_conn(clear_database)
def test_store_copy_from_2(conn):
    trend_descriptors = [
        TrendDescriptor('CCR', datatype.DataTypeInteger, ''),
        TrendDescriptor('CCRatts', datatype.DataTypeSmallInt, ''),
        TrendDescriptor('Drops', datatype.DataTypeSmallInt, '')
    ]

    trend_names = [t.name for t in trend_descriptors]

    data_rows = [
        (10023, (0.9919, 2105, 17))
    ]

    timestamp = pytz.utc.localize(datetime(2013, 1, 2, 10, 45, 0))
    modified = pytz.utc.localize(datetime.now())
    granularity = create_granularity("900")

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src010")(cursor)
        entity_type = EntityType.from_name("test-type002")(cursor)

        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_source, entity_type, granularity, trend_descriptors, 86400
        ))(cursor)
        partition = trend_store.partition(timestamp)
        partition.create(cursor)
        table = partition.table()

        trend_store.store_copy_from(
            refined_package_type_for_entity_type('Node')(
                granularity, timestamp, trend_names, data_rows
            ),
            modified
        )(cursor)

        eq_(row_count(cursor, table), 1)

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        eq_(max_modified, modified)


@with_conn(clear_database)
def test_store_using_tmp(conn):
    granularity = create_granularity(900)
    trend_names = ['CellID', 'CCR', 'RadioFail', 'RFOldHo', 'AbisFailCall']
    data_rows = [
        (10023, (10023, 0.9919, 10, 3, 3)),
        (10047, (10047, 0.9963, 11, 5, 0)),
        (10048, (10048, 0.9935, 12, 3, 0)),
        (10049, (10049, 0.9939, 20, 3, 4)),
        (10050, (10050, 0.9940, 18, 3, 0)),
        (10051, (10051, 0.9944, 17, 2, 2)),
        (10052, (10052, 0.9889, 18, 2, 0)),
        (10053, (10053, 0.9920, 15, 3, 1)),
        (10023, (10023, 0.9931, 9, 0, 1)),
        (10085, (10085, 0.9987, 3, 0, 0)),
        (10086, (10086, 0.9972, 3, 2, 0))
    ]

    data_types = datatype.deduce_data_types(data_rows)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name("test-type001")(cursor)

        trend_descriptors = [
            TrendDescriptor(trend_name, data_type, '')
            for trend_name, data_type in zip(trend_names, data_types)
        ]

        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_source, entity_type, granularity, trend_descriptors, 86400
        ))(cursor)

        timestamp = pytz.utc.localize(datetime(2013, 1, 2, 10, 45, 0))

        partition = trend_store.partition(timestamp)

        table = partition.table()

        table.drop().if_exists().execute(cursor)

        partition.create(cursor)

        modified = pytz.utc.localize(datetime.now())

        trend_store.store_update(
            refined_package_type_for_entity_type('Node')(
                granularity, timestamp, trend_names, data_rows
            ),
            modified
        )(cursor)

        conn.commit()

        eq_(row_count(cursor, table), 10)

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        eq_(max_modified, modified)


@with_conn(clear_database)
def test_store_insert_rows(conn):
    granularity = create_granularity("900")
    trend_names = ['CellID', 'CCR', 'Drops']
    data_rows = [
        (10023, (10023, 0.9919, 17)),
        (10047, (10047, 0.9963, 18))
    ]

    modified = pytz.utc.localize(datetime.now())
    time1 = pytz.utc.localize(datetime(2013, 1, 2, 10, 45, 0))
    time2 = time1 - timedelta(days=1)

    data_types = datatype.deduce_data_types(data_rows)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name("test-type001")(cursor)

        trend_descriptors = [
            TrendDescriptor(trend_name, data_type, '')
            for trend_name, data_type in zip(trend_names, data_types)
        ]

        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_source, entity_type, granularity, trend_descriptors, 86400 * 7
        ))(cursor)

        partition = trend_store.partition(time1)

        table = partition.table()

        table.drop().if_exists().execute(cursor)

        partition.create(cursor)

        trend_store.store_batch_insert(
            refined_package_type_for_entity_type('Node')(
                granularity, time1, trend_names, data_rows
            ), modified
        )(cursor)
        conn.commit()

        eq_(row_count(cursor, table), 2)

        trend_store.store_batch_insert(
            refined_package_type_for_entity_type('Node')(
                granularity, time2, trend_names, data_rows
            ), modified
        )(cursor)
        conn.commit()

        eq_(row_count(cursor, table), 4)

        conn.commit()

        eq_(row_count(cursor, table), 4)

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        eq_(max_modified, modified)


@with_conn(clear_database)
def test_update_modified_column(conn):
    trend_names = ['CellID', 'CCR', 'Drops']
    data_rows = [
        (10023, (10023, 0.9919, 17)),
        (10047, (10047, 0.9963, 18))
    ]
    data_types = datatype.deduce_data_types(data_rows)

    update_data_rows = [(10023, ('10023', '0.9919', '17'))]
    timestamp = pytz.utc.localize(datetime.now())
    granularity = create_granularity("900")

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name("test-type001")(cursor)

        trend_descriptors = [
            TrendDescriptor(trend_name, data_type, '')
            for trend_name, data_type in zip(trend_names, data_types)
        ]

        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_source, entity_type, granularity, trend_descriptors, 86400
        ))(cursor)

        partition = trend_store.partition(timestamp)

        table = partition.table()

        partition.create(cursor)

        trend_store.store(
            refined_package_type_for_entity_type('Node')(
                granularity, timestamp, trend_names, data_rows
            )
        ).run(conn)

        time.sleep(1)
        trend_store.store(
            refined_package_type_for_entity_type('Node')(
                granularity, timestamp, trend_names, update_data_rows
            )
        ).run(conn)
        conn.commit()

        query = table.select([Column("modified")])

        query.execute(cursor)
        modified_list = [modified for modified in cursor.fetchall()]
        assert_not_equal(modified_list[0], modified_list[1])

        table.select(Call("max", Column("modified"))).execute(cursor)

        max_modified = first(cursor.fetchone())

        modified_table.select(Column("end")).where_(
            Eq(Column("trendstore_id"), trend_store.id)
        ).execute(cursor)

        end = first(cursor.fetchone())

        eq_(end, max_modified)


@with_conn(clear_database)
def test_update(conn):
    trend_names = ["CellID", "CCR", "Drops"]
    data_rows = [
        (10023, (10023, 0.9919, 17)),
        (10047, (10047, 0.9963, 18))
    ]
    data_types = datatype.deduce_data_types(data_rows)
    update_data_rows = [(10023, (10023, 0.5555, 17))]
    timestamp = datetime.now()
    granularity = create_granularity("900")

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name("test-type001")(cursor)

        trend_descriptors = [
            TrendDescriptor(trend_name, data_type, '')
            for trend_name, data_type in zip(trend_names, data_types)
        ]

        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_source, entity_type, granularity, trend_descriptors, 86400
        ))(cursor)

        partition = trend_store.partition(timestamp)
        partition.create(cursor)

        table = partition.table()

    trend_store.store(
        refined_package_type_for_entity_type('Node')(
            granularity, timestamp, trend_names, data_rows
        )
    ).run(conn)

    trend_store.store(
        refined_package_type_for_entity_type('Node')(
            granularity, timestamp, trend_names, update_data_rows
        )
    ).run(conn)

    conn.commit()

    query = table.select([Column("modified"), Column("CCR")])

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)
        rows = cursor.fetchall()

    assert_not_equal(rows[0][0], rows[1][0])
    assert_not_equal(rows[0][1], rows[1][1])


@with_conn(clear_database)
def test_update_and_modify_columns_fractured(conn):
    granularity = create_granularity("900")
    timestamp = pytz.utc.localize(datetime(2013, 1, 2, 10, 45, 0))
    entity_ids = range(1023, 1023 + 100)

    trend_names_a = ["CellID", "CCR", "Drops"]
    data_rows_a = [
        (i, (10023, 0.9919, 17))
        for i in entity_ids
    ]
    data_types_a = datatype.deduce_data_types(data_rows_a)

    trend_names_b = ["CellID", "Drops"]
    data_rows_b = [
        (i, (10023, 19))
        for i in entity_ids
    ]
    data_types_b = datatype.deduce_data_types(data_rows_b)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-src009")(cursor)
        entity_type = EntityType.from_name("test-type001")(cursor)

        trend_descriptors = [
            TrendDescriptor(trend_name, data_type, '')
            for trend_name, data_type in zip(trend_names_a, data_types_a)
        ]

        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_source, entity_type, granularity, trend_descriptors, 86400
        ))(cursor)

        partition = trend_store.partition(timestamp)
        partition.create(cursor)

        table = partition.table()

        conn.commit()

    trend_store.store(
        refined_package_type_for_entity_type('Node')(
            granularity, timestamp, trend_names_a, data_rows_a
        )
    ).run(conn)
    time.sleep(0.2)

    check_columns = map(Column, ["modified", "Drops"])
    query = table.select(check_columns)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)
        row_before = cursor.fetchone()

    trend_store.store(
        refined_package_type_for_entity_type('Node')(
            granularity, timestamp, trend_names_b, data_rows_b
        )
    ).run(conn)

    query = table.select(check_columns)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)
        row_after = cursor.fetchone()

    assert_not_equal(row_before[0], row_after[0])
    assert_not_equal(row_before[1], row_after[1])


class TestData(DataSet):
    def __init__(self):
        self.granularity = create_granularity("900")
        self.data_source = None
        self.entity_type = None

    def load(self, cursor):
        self.data_source = DataSource.from_name("test-source")(cursor)
        self.entity_type = EntityType.from_name("test_type")(cursor)


@with_conn(clear_database)
@with_dataset(TestData)
def test_create_trend_store(conn, data_set):
    partition_size = 3600

    create_trend_store = TrendStore.create(TrendStoreDescriptor(
        data_set.data_source, data_set.entity_type, data_set.granularity, [],
        partition_size
    ))

    with closing(conn.cursor()) as cursor:
        trend_store = create_trend_store(cursor)

    assert isinstance(trend_store, TrendStore)

    assert trend_store.id is not None


@with_conn(clear_database)
@with_dataset(TestData)
def test_create_trendstore_with_children(conn, data_set):
    partition_size = 3600

    with closing(conn.cursor()) as cursor:
        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_set.data_source, data_set.entity_type, data_set.granularity, [],
            partition_size
        ))(cursor)

        assert trend_store.id is not None

        timestamp = pytz.utc.localize(
            datetime.datetime(2013, 5, 6, 14, 45)
        )

        partition = trend_store.partition(timestamp)

        partition.create(cursor)


@with_conn(clear_database)
@with_dataset(TestData)
def test_get_trendstore(conn, data_set):
    partition_size = 3600

    with closing(conn.cursor()) as cursor:
        TrendStore.create(TrendStoreDescriptor(
            data_set.data_source, data_set.entity_type, data_set.granularity,
            [], partition_size
        ))(cursor)

        trend_store = TrendStore.get(
            data_set.data_source, data_set.entity_type, data_set.granularity
        )(cursor)

        eq_(trend_store.data_source.id, data_set.data_source.id)
        eq_(trend_store.partition_size, partition_size)
        assert trend_store.id is not None, "trendstore.id is None"


@with_conn(clear_database)
@with_dataset(TestData)
def test_store_copy_from(conn, data_set):
    partition_size = 86400

    with closing(conn.cursor()) as cursor:
        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_set.data_source, data_set.entity_type, data_set.granularity,
            [], partition_size
        ))(cursor)

    conn.commit()

    timestamp = pytz.utc.localize(
        datetime.datetime(2013, 4, 25, 9, 45)
    )

    trends = ["a", "b", "c"]

    def make_row(index):
        return 1234 + index, [1, 2, 3 + index]

    rows = list(map(make_row, range(100)))

    data_package = refined_package_type_for_entity_type('Node')(
        data_set.granularity, timestamp, trends, rows
    )

    transaction = trend_store.store(data_package)
    transaction.run(conn)

    transaction = trend_store.store(data_package)
    transaction.run(conn)


@with_conn(clear_database)
@with_dataset(TestData)
def test_store_copy_from_missing_column(conn, data_set):
    partition_size = 86400

    with closing(conn.cursor()) as cursor:
        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_set.data_source, data_set.entity_type, data_set.granularity,
            [], partition_size
        ))(cursor)

    conn.commit()

    timestamp = pytz.utc.localize(
        datetime(2013, 4, 25, 9, 45)
    )

    trends = ["a", "b", "c"]

    rows = [
        (1234 + index, [1, 2, 3 + index])
        for index in range(100)
    ]

    data_package = refined_package_type_for_entity_type('Node')(
        data_set.granularity, timestamp, trends, rows
    )

    transaction = trend_store.store(data_package)
    transaction.run(conn)

    # Store second part with one column extra

    timestamp = pytz.utc.localize(
        datetime(2013, 4, 25, 10, 00)
    )

    trends = ["a", "b", "c", "d"]

    def make_row_y(index):
        return 1234 + index, [1, 2, 3, 4 + index]

    rows = map(make_row_y, range(100))

    data_package = refined_package_type_for_entity_type('Node')(
        data_set.granularity, timestamp, trends, rows
    )

    transaction = trend_store.store(data_package)
    transaction.run(conn)


@with_conn(clear_database)
@with_dataset(TestData)
def test_store(conn, data_set):
    partition_size = 86400

    with closing(conn.cursor()) as cursor:
        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_set.data_source, data_set.entity_type, data_set.granularity,
            [], partition_size
        ))(cursor)

    conn.commit()

    timestamp = pytz.utc.localize(
        datetime(2013, 4, 25, 9, 45)
    )

    trends = ["a", "b", "c"]

    rows = [
        (1234, [1, 2, 3]),
        (2345, [4, 5, 6])
    ]

    data_package = refined_package_type_for_entity_type('Node')(
        data_set.granularity, timestamp, trends, rows
    )

    transaction = trend_store.store(data_package)
    transaction.run(conn)

    table = trend_store.partition(timestamp).table()

    condition = And(
        Eq(Column("entity_id"), 2345),
        Eq(Column("timestamp"), timestamp))

    query = table.select(Column("c")).where_(condition)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        c, = cursor.fetchone()

    eq_(c, 6)

    rows = [
        (1234, [1, 2, 3]),
        (2345, [4, 5, 7])]

    data_package = refined_package_type_for_entity_type('Node')(
        data_set.granularity, timestamp, trends, rows
    )

    transaction = trend_store.store(data_package)
    transaction.run(conn)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        c, = cursor.fetchone()

    eq_(c, 7)


@with_conn(clear_database)
@with_dataset(TestData)
def test_generate_index(conn, data_set):
    partition_size = 86400

    start = pytz.utc.localize(
        datetime(2013, 4, 25, 9, 45)
    )
    end = pytz.utc.localize(
        datetime(2013, 4, 27, 9, 45)
    )

    partitioning = Partitioning(partition_size)

    for timestamp in data_set.granularity.range(start, end):
        partition_index = partitioning.index(timestamp)

        args = partition_size, timestamp

        with closing(conn.cursor()) as cursor:
            cursor.callproc("trend_directory.timestamp_to_index", args)

            postgresql_partition_index, = cursor.fetchone()

        eq_(postgresql_partition_index, partition_index)


@with_conn(clear_database)
@with_dataset(TestData)
def test_store_add_column(conn, data_set):
    partition_size = 86400

    with closing(conn.cursor()) as cursor:
        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_set.data_source, data_set.entity_type, data_set.granularity,
            [], partition_size
        ))(cursor)

    conn.commit()

    timestamp = pytz.utc.localize(
        datetime(2013, 4, 25, 10, 45)
    )

    trends = ["a", "b", "c"]

    rows = [
        (1234, [1, 2, 3]),
        (2345, [4, 5, 6])
    ]

    data_package = refined_package_type_for_entity_type('Node')(
        data_set.granularity, timestamp, trends, rows
    )

    transaction = trend_store.store(data_package)
    transaction.run(conn)

    table = trend_store.partition(timestamp).table()

    condition = And(
        Eq(Column("entity_id"), 2345),
        Eq(Column("timestamp"), timestamp))

    query = table.select(Column("c")).where_(condition)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        c, = cursor.fetchone()

    eq_(c, 6)

    trends = ["a", "b", "c", "d"]

    rows = [
        (2345, [4, 5, 7, "2013-04-25 11:00:00"])
    ]

    data_package = refined_package_type_for_entity_type('Node')(
        data_set.granularity, timestamp, trends, rows
    )

    transaction = trend_store.store(data_package)
    transaction.run(conn)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        c, = cursor.fetchone()

    eq_(c, 7)


@with_conn(clear_database)
@with_dataset(TestData)
def test_store_alter_column(conn, data_set):
    partition_size = 86400

    trend_descriptors = [
        TrendDescriptor('a', datatype.DataTypeSmallInt, ''),
        TrendDescriptor('b', datatype.DataTypeSmallInt, ''),
        TrendDescriptor('c', datatype.DataTypeSmallInt, ''),
    ]

    trend_names = [t.name for t in trend_descriptors]

    with closing(conn.cursor()) as cursor:
        trend_store = TrendStore.create(TrendStoreDescriptor(
            data_set.data_source, data_set.entity_type, data_set.granularity,
            trend_descriptors, partition_size
        ))(cursor)

    conn.commit()

    timestamp = pytz.utc.localize(
        datetime(2013, 4, 25, 11, 00)
    )

    rows = [
        (1234, [1, 2, 3]),
        (2345, [4, 5, 6])
    ]

    data_package = refined_package_type_for_entity_type('Node')(
        data_set.granularity, timestamp, trend_names, rows
    )

    transaction = trend_store.store(data_package)
    transaction.run(conn)

    table = trend_store.partition(timestamp).table()

    condition = And(
        Eq(Column("entity_id"), 2345),
        Eq(Column("timestamp"), timestamp)
    )

    query = table.select(Column("c")).where_(condition)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        c, = cursor.fetchone()

    eq_(c, 6)

    rows = [
        (2345, [4, 5, "2013-04-25 11:00:00"])
    ]

    data_package = refined_package_type_for_entity_type('Node')(
        data_set.granularity, timestamp, trend_names, rows
    )

    transaction = trend_store.store(data_package)
    transaction.run(conn)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        c, = cursor.fetchone()

    eq_(c, datetime(2013, 4, 25, 11, 0, 0))


def row_count(cursor, table):
    cursor.execute("SELECT count(*) FROM {}".format(table.render()))

    count, = cursor.fetchone()

    return count
