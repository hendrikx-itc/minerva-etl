from time import sleep
from contextlib import closing
from typing import Optional, Generator, Tuple

import psycopg2.errors

from minerva.db.error import DuplicateTable, LockNotAvailable, DeadLockDetected


def create_specific_partitions_for_trend_store(conn, trend_store_id, timestamp):
    query = (
        "SELECT part.id, trend_directory.timestamp_to_index(partition_size, %s) "
        "FROM trend_directory.trend_store ts "
        "JOIN trend_directory.trend_store_part part ON part.trend_store_id = ts.id "
        "WHERE ts.id = %s"
    )

    with conn.cursor() as cursor:
        cursor.execute(query, (timestamp, trend_store_id))

        rows = cursor.fetchall()
 
    retry = True
    attempt = 0

    for i, (trend_store_part_id, partition_index) in enumerate(rows):
        while retry and attempt < 3:
            attempt += 1
            try:
                name = create_partition_for_trend_store_part(
                    conn, trend_store_part_id, partition_index
                )

                conn.commit()

                yield name, partition_index, i + 1, len(rows)
                retry = False
            except PartitionExistsError as e:
                conn.rollback()
                retry = False
            except LockNotAvailable:
                conn.rollback()
                print(e)
                sleep(1)
            except DeadLockDetected:
                pass


def create_partitions_for_trend_store(
        conn, trend_store_id: int, ahead_interval: str, partition_count: Optional[int] = None
) -> Generator[Tuple[str, int, int, int], None, None]:
    """
    :param conn: Connection to Minerva database
    :param trend_store_id: Id of trend store to create partitions for
    :param ahead_interval: Interval string defining how far ahead partitions need te be created
    :param partition_count: The number of partitions to create or None
    to create partitions for the full retention period.
    """
    if partition_count is None:
        query = (
            "WITH partition_indexes AS ("
            "SELECT trend_directory.timestamp_to_index(partition_size, t) AS i, p.id AS part_id "
            "FROM trend_directory.trend_store "
            "JOIN trend_directory.trend_store_part p ON p.trend_store_id = trend_store.id "
            f"JOIN generate_series(now() - partition_size - trend_store.retention_period, now() + partition_size + '{ahead_interval}'::interval, partition_size) t ON true "
            "WHERE trend_store.id = %s"
            ") "
            "SELECT partition_indexes.part_id, partition_indexes.i FROM partition_indexes "
            "LEFT JOIN trend_directory.partition ON partition.index = i AND partition.trend_store_part_id = partition_indexes.part_id "
            "WHERE partition.id IS NULL"
        )
    else:
        query = (
            "WITH partition_indexes AS ("
            "SELECT trend_directory.timestamp_to_index(partition_size, t) AS i, p.id AS part_id "
            "FROM trend_directory.trend_store "
            "JOIN trend_directory.trend_store_part p ON p.trend_store_id = trend_store.id "
            f"JOIN generate_series(now() - partition_size - (partition_size * {partition_count}), now() + partition_size + '{ahead_interval}'::interval, partition_size) t ON true "
            "WHERE trend_store.id = %s"
            ") "
            "SELECT partition_indexes.part_id, partition_indexes.i FROM partition_indexes "
            "LEFT JOIN trend_directory.partition ON partition.index = i AND partition.trend_store_part_id = partition_indexes.part_id "
            "WHERE partition.id IS NULL"
        )

    args = (trend_store_id,)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)

        rows = cursor.fetchall()

    retry = True
    attempt = 0
    
    while retry and attempt < 3:
        attempt += 1
        for i, (trend_store_part_id, partition_index) in enumerate(rows):
            try:
                name = create_partition_for_trend_store_part(
                    conn, trend_store_part_id, partition_index
                )
                conn.commit()

                yield name, partition_index, i, len(rows)

                retry = False
            except LockNotAvailable as partition_lock:
                conn.rollback()
                print(f"Could not create partition for part {trend_store_part_id} - {partition_index}: {partition_lock}\n")
            except DeadLockDetected as deadlock:
                conn.rollback()
                print(f"Could not create partition for part {trend_store_part_id} - {partition_index}: {deadlock}\n")


class PartitionExistsError(Exception):
    def __init__(self, trend_store_part_id, partition_index):
        self.trend_store_part_id = trend_store_part_id
        self.partition_index = partition_index


def create_partition_for_trend_store_part(
        conn, trend_store_part_id, partition_index):
    query = (
        "SELECT p.name, trend_directory.create_partition(p, %s) "
        "FROM trend_directory.trend_store_part p "
        "WHERE p.id = %s"
    )
    args = (partition_index, trend_store_part_id)

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query, args)
        except psycopg2.errors.DuplicateTable:
            raise PartitionExistsError(trend_store_part_id, partition_index)
        except psycopg2.errors.LockNotAvailable as e:
            raise LockNotAvailable(e)

        name, p = cursor.fetchone()

        return name
