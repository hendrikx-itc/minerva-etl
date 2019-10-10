from contextlib import closing


def create_partitions_for_trend_store(conn, trend_store_id, ahead_interval):
    query = (
        "WITH partition_indexes AS ("
        "SELECT trend_directory.timestamp_to_index(partition_size, t) AS i, p.id AS part_id "
        "FROM trend_directory.trend_store "
        "JOIN trend_directory.trend_store_part p ON p.trend_store_id = trend_store.id "
        "JOIN generate_series(now() - trend_store.retention_period, now() + '{}'::interval, partition_size) t ON true "
        "WHERE trend_store.id = %s"
        ") "
        "SELECT partition_indexes.part_id, partition_indexes.i FROM partition_indexes "
        "LEFT JOIN trend_directory.partition ON partition.index = i AND partition.trend_store_part_id = partition_indexes.part_id "
        "WHERE partition.id IS NULL"
    ).format(ahead_interval)

    args = (trend_store_id,)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)

        rows = cursor.fetchall()

    for trend_store_part_id, partition_index in rows:
        create_partition_for_trend_store_part(conn, trend_store_part_id, partition_index)


def create_partition_for_trend_store_part(conn, trend_store_part_id, partition_index):
    query = (
        "SELECT p.name, trend_directory.create_partition(p, %s) "
        "FROM trend_directory.trend_store_part p "
        "WHERE p.id = %s"
    )
    args = (partition_index, trend_store_part_id)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)

        name, p = cursor.fetchone()

        print('{} - {}'.format(name, partition_index))

