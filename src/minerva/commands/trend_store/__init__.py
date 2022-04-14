"""Provides the trend-store sub-command."""
from contextlib import closing
import sys
import datetime
from typing import Optional, List
from pathlib import Path

import psycopg2.errors
from psycopg2 import sql

from minerva.db import connect
from minerva.db.error import LockNotAvailable
from minerva.commands.partition import (
    create_partitions_for_trend_store,
    create_specific_partitions_for_trend_store,
)
from minerva.instance import TrendStore, MinervaInstance
from minerva.commands.trend_store.create import setup_create_parser
from minerva.commands.trend_store.add_trends import setup_add_trends_parser
from minerva.commands.trend_store.add_parts import setup_add_parts_parser
from minerva.commands.trend_store.remove_trends import setup_remove_trends_parser
from minerva.commands.trend_store.deduce import setup_deduce_parser
from minerva.commands.trend_store.delete import setup_delete_parser
from minerva.commands.trend_store.show import setup_show_parser
from minerva.commands.trend_store.list import setup_list_parser
from minerva.commands.trend_store.check_config import setup_check_config_parser


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        "trend-store", help="command for administering trend stores"
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
    setup_add_trends_parser(cmd_subparsers)
    setup_add_parts_parser(cmd_subparsers)
    setup_remove_trends_parser(cmd_subparsers)
    setup_alter_trends_parser(cmd_subparsers)
    setup_change_trends_parser(cmd_subparsers)
    setup_deduce_parser(cmd_subparsers)
    setup_delete_parser(cmd_subparsers)
    setup_show_parser(cmd_subparsers)
    setup_list_parser(cmd_subparsers)
    setup_list_config_parser(cmd_subparsers)
    setup_check_config_parser(cmd_subparsers)
    setup_partition_parser(cmd_subparsers)
    setup_process_modified_log_parser(cmd_subparsers)
    setup_materialize_parser(cmd_subparsers)


def setup_alter_trends_parser(subparsers):
    cmd = subparsers.add_parser(
        "alter-trends",
        help="command for changing data types and aggregation types for trends from trend stores",  # pylint: disable=C0301
    )

    cmd.add_argument(
        "--force",
        action="store_true",
        help="change datatype even if the new datatype is less powerful than the old one",  # pylint: disable=C0301
    )

    cmd.add_argument(
        "definition", type=Path, help="file containing trend store definition"
    )

    cmd.set_defaults(cmd=alter_trends_cmd)


def alter_trends_cmd(args):
    instance = MinervaInstance.load()
    trend_store = instance.load_trend_store_from_file(args.definition)

    try:
        result = alter_tables_in_trend_store(trend_store, force=args.force)

        if result:
            sys.stdout.write("Changed columns: {}\n".format(", ".join(result)))
        else:
            sys.stdout.write("No columns were changed.\n")
    except Exception as exc:
        sys.stdout.write("Error:\n{}".format(str(exc)))
        raise exc


def setup_change_trends_parser(subparsers):
    cmd = subparsers.add_parser(
        "change",
        help="change the content of a trend store to a predefined type",
    )

    cmd.add_argument(
        "-v", "--verbose", action="store_true", help="verbose progress reporting"
    )

    cmd.add_argument(
        "--force",
        action="store_true",
        help="change datatype even if the new datatype is less powerful than the old one",  # pylint: disable=C0301
    )

    cmd.add_argument(
        "--statement-timeout", help="set the statement timeout on the database session"
    )

    cmd.add_argument(
        "definition", type=Path, help="file containing trend store definition"
    )

    cmd.set_defaults(cmd=change_trends_cmd)


def change_trends_cmd(args):
    instance = MinervaInstance.load()

    trend_store = instance.load_trend_store_from_file(args.definition)

    try:
        if args.verbose:
            print(f"applying changes to trend store {trend_store}")

        for _, (added, removed, changed) in change_trend_store(
            trend_store, force=args.force, statement_timeout=args.statement_timeout
        ):
            if added or removed or changed:
                print(f"added {added}")
                print(f"removed {removed}")
                print(f"changed {changed}")
            else:
                print("no changes were made")
    except Exception as exc:
        print(f"Error:\n{exc}")
        raise exc


def alter_tables_in_trend_store(trend_store: TrendStore, force=False):
    if force:
        change_function = sql.Identifier("trend_directory", "change_all_trend_data")
    else:
        change_function = sql.Identifier("trend_directory", "change_trend_data_upward")

    query = sql.SQL(
        "SELECT {}("
        "trend_directory.get_trend_store("
        "%s::text, %s::text, %s::interval"
        "), %s::trend_directory.trend_store_part_descr[]"
        ")"
    ).format(change_function)

    query_args = (
        trend_store.data_source,
        trend_store.entity_type,
        trend_store.granularity,
        trend_store.parts,
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)
            result = cursor.fetchone()

        conn.commit()

    if result and result[0]:
        return result[0]
    else:
        return None


def change_trend_store(
    trend_store: TrendStore, force=False, statement_timeout: Optional[str] = None
):
    with closing(connect()) as conn:
        for part in trend_store.parts:
            if statement_timeout is not None:
                set_statement_timeout(conn, statement_timeout)

            check_trend_store_part_exists(conn, trend_store, part.name)

            print(f"applying changes for part '{part.name}':")

            try:
                result = change_trend_store_part(conn, part, force)
            except psycopg2.errors.FeatureNotSupported as exc:
                conn.rollback()
                print(f"error changing trend store part '{part.name}': {exc}")
            else:
                conn.commit()

                yield part.name, result


def check_trend_store_part_exists(conn, trend_store, part_name):
    """Create a trend store part if it does not exists."""
    query = (
        "SELECT * FROM trend_directory.get_or_create_trend_store_part("
        "(trend_directory.get_trend_store(%s::text, %s::text, %s::interval)).id, %s"
        ")"
    )

    query_args = (
        trend_store.data_source,
        trend_store.entity_type,
        trend_store.granularity,
        part_name,
    )

    with conn.cursor() as cursor:
        cursor.execute(query, query_args)


def change_trend_store_part(conn, trend_store_part, force=False):
    if force:
        change_function = "change_trend_store_part_strong"
    else:
        change_function = "change_trend_store_part_weak"

    query = sql.SQL(
        "SELECT * FROM {}(%s::trend_directory.trend_store_part_descr)"
    ).format(sql.Identifier("trend_directory", change_function))

    query_args = (trend_store_part,)

    with conn.cursor() as cursor:
        cursor.execute(query, query_args)
        result = cursor.fetchone()

    return result


def set_statement_timeout(conn, statement_timeout: str):
    """Set the statement_timeout setting for this session."""
    query = "SET SESSION statement_timeout = %s"
    query_args = (statement_timeout,)

    with conn.cursor() as cursor:
        cursor.execute(query, query_args)


def setup_list_config_parser(subparsers):
    cmd = subparsers.add_parser(
        "list-config", help="list trend stores from configuration"
    )

    cmd.set_defaults(cmd=list_config_trend_stores_cmd)


def list_config_trend_stores_cmd(_args):
    instance = MinervaInstance.load()

    trend_stores = instance.list_trend_stores()

    for trend_store in trend_stores:
        print(trend_store)


def setup_partition_parser(subparsers):
    cmd = subparsers.add_parser("partition", help="manage trend store partitions")

    cmd_subparsers = cmd.add_subparsers()

    create_parser = cmd_subparsers.add_parser(
        "create", help="create partitions for trend store"
    )

    create_parser.add_argument(
        "--trend-store",
        type=int,
        help="Id of trend store for which to create partitions",
    )

    create_parser.add_argument(
        "--ahead-interval", help="period for which to create partitions"
    )

    create_parser.set_defaults(cmd=create_partition_cmd)

    create_for_timestamp_parser = cmd_subparsers.add_parser(
        "create-for-timestamp", help="create partitions for specific timestamp"
    )

    create_for_timestamp_parser.add_argument(
        "--trend-store",
        type=int,
        help="Id of trend store for which to create partitions",
    )

    create_for_timestamp_parser.add_argument(
        "timestamp", help="timestamp to create partitions for"
    )

    create_for_timestamp_parser.set_defaults(cmd=create_partition_for_timestamp_cmd)

    remove_old_parser = cmd_subparsers.add_parser(
        "remove-old", help="remove old partitions"
    )

    remove_old_parser.add_argument(
        "--pretend",
        action="store_true",
        default=False,
        help="do not actually delete partitions",
    )

    remove_old_parser.set_defaults(cmd=remove_old_partitions_cmd)


def remove_old_partitions_cmd(args):
    partition_count_query = "select count(*) from trend_directory.partition"

    old_partitions_query = (
        "select p.id, p.name, p.from, p.to "
        "from trend_directory.partition p "
        "join trend_directory.trend_store_part tsp on tsp.id = p.trend_store_part_id "
        "join trend_directory.trend_store ts on ts.id = tsp.trend_store_id "
        "where p.from < (now() - retention_period - partition_size - partition_size) "
        "order by p.name"
    )

    removed_partitions = 0

    with connect() as conn:
        set_lock_timeout(conn, "1s")
        conn.commit()

        with conn.cursor() as cursor:
            cursor.execute(partition_count_query)
            (total_partitions,) = cursor.fetchone()

            cursor.execute(old_partitions_query)

            rows = cursor.fetchall()

            print(f"Found {len(rows)} of {total_partitions} partitions to be removed")

            conn.commit()

            if len(rows) > 0:
                print()
                for partition_id, partition_name, data_from, data_to in rows:
                    if not args.pretend:
                        try:
                            cursor.execute(
                                sql.SQL("DROP TABLE {}").format(
                                    sql.Identifier("trend_partition", partition_name)
                                )
                            )
                            cursor.execute(
                                "DELETE FROM trend_directory.partition WHERE id = %s",
                                (partition_id,),
                            )
                            conn.commit()
                            removed_partitions += 1
                            print(
                                f"Removed partition {partition_name} ({data_from} - {data_to})"
                            )
                        except psycopg2.errors.LockNotAvailable as partition_lock:
                            conn.rollback()
                            print(
                                f"Could not remove partition {partition_name} ({data_from} - {data_to}): {partition_lock}"  # pylint: disable=C0301
                            )

                if args.pretend:
                    print(
                        f"\nWould have removed {removed_partitions} of {total_partitions} partitions"  # pylint: disable=C0301
                    )
                else:
                    print(
                        f"\nRemoved {removed_partitions} of {total_partitions} partitions"
                    )


def create_partition_cmd(args):
    ahead_interval = args.ahead_interval or "1 day"

    try:
        with closing(connect()) as conn:
            set_lock_timeout(conn, "1s")
            conn.commit()

            if args.trend_store is None:
                create_partitions_for_all_trend_stores(conn, ahead_interval)
            else:
                create_partitions_for_one_trend_store(
                    conn, args.trend_store, ahead_interval
                )
    except LockNotAvailable as partition_lock:
        print(f"Could not create partition: {partition_lock}")


def create_partitions_for_one_trend_store(conn, trend_store_id, ahead_interval):
    for name, partition_index, i, num in create_partitions_for_trend_store(
        conn, trend_store_id, ahead_interval
    ):
        print(f"{name} - {partition_index} ({i}/{num})")


def create_partitions_for_all_trend_stores(conn, ahead_interval):
    query = "SELECT id FROM trend_directory.trend_store"

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)

        rows = cursor.fetchall()

    for (trend_store_id,) in rows:
        create_partitions_for_one_trend_store(conn, trend_store_id, ahead_interval)


def create_partition_for_timestamp_cmd(args):
    print(f"Creating partitions for timestamp {args.timestamp}")

    query = "SELECT id FROM trend_directory.trend_store"

    with closing(connect()) as conn:
        if args.trend_store is None:
            with closing(conn.cursor()) as cursor:
                cursor.execute(query)

                rows = cursor.fetchall()

            for (trend_store_id,) in rows:
                for (
                    name,
                    partition_index,
                    i,
                    num,
                ) in create_specific_partitions_for_trend_store(
                    conn, trend_store_id, args.timestamp
                ):
                    print(f"{name} - {partition_index} ({i}/{num})")
        else:
            print("no")


def setup_process_modified_log_parser(subparsers):
    cmd = subparsers.add_parser(
        "process-modified-log", help="process modified log into modified state"
    )

    cmd.add_argument(
        "--reset",
        action="store_true",
        default=False,
        help="reset modified log processing state to Id 0",
    )

    cmd.set_defaults(cmd=process_modified_log_cmd)


def process_modified_log_cmd(args):
    process_modified_log(args.reset)


def process_modified_log(reset):
    reset_query = (
        "UPDATE trend_directory.modified_log_processing_state "
        "SET last_processed_id = %s "
        "WHERE name = 'current'"
    )

    get_position_query = (
        "SELECT last_processed_id "
        "FROM trend_directory.modified_log_processing_state "
        "WHERE name = 'current'"
    )

    query = "SELECT * FROM trend_directory.process_modified_log()"

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            if reset:
                cursor.execute(reset_query, (0,))

            cursor.execute(get_position_query)

            if cursor.rowcount == 1:
                (started_at_id,) = cursor.fetchone()
            else:
                started_at_id = 0

            cursor.execute(query)

            (last_processed_id,) = cursor.fetchone()

        conn.commit()

    timestamp_str = datetime.datetime.now()

    print(
        f"{timestamp_str} Processed modified log {started_at_id} - {last_processed_id}"
    )


def setup_materialize_parser(subparsers):
    cmd = subparsers.add_parser(
        "materialize", help="command for materializing trend data"
    )

    cmd.add_argument(
        "--reset",
        action="store_true",
        default=False,
        help="ignore materialization state",
    )

    cmd.add_argument("--max-num", help="maximum number of materializations to run")

    cmd.add_argument(
        "--newest-first",
        action="store_true",
        default=False,
        help="materialize newest data first",
    )

    cmd.add_argument("materialization", nargs="*", help="materialization Id or name")

    cmd.set_defaults(cmd=materialize_cmd)


def materialize_cmd(args):
    """Execute materializations specified by `args`."""
    try:
        if not args.materialization:
            materialize_all(args.reset, args.max_num, args.newest_first)
        else:
            materialize_selection(
                args.materialization, args.reset, args.max_num, args.newest_first
            )
    except Exception as exc:
        sys.stdout.write(f"Error:\n{exc}")
        raise exc


class MaterializationChunk:
    """Represents the materialization for one timestamp."""

    materialization_id: int
    name: str
    timestamp: datetime.datetime

    def __init__(
        self, materialization_id: int, name: str, timestamp: datetime.datetime
    ):
        self.materialization_id = materialization_id
        self.name = name
        self.timestamp = timestamp

    def __str__(self):
        return f"{self.name} - {self.timestamp}"

    def materialize(self, conn):
        try:
            materialize_query = (
                "SELECT (trend_directory.materialize(m, %s)).row_count "
                "FROM trend_directory.materialization m WHERE id = %s"
            )

            with conn.cursor() as cursor:
                cursor.execute(
                    materialize_query, (self.timestamp, self.materialization_id)
                )
                (row_count,) = cursor.fetchone()

            conn.commit()

            print(f"{self}: {row_count} records")
        except Exception as e:
            conn.rollback()
            print(f"Error materializing {self.name} ({self.materialization_id})")
            print(str(e))


def get_materialization_chunks_to_run(
    conn, materialization, reset: bool, max_num: Optional[int], newest_first: bool
) -> List[MaterializationChunk]:
    args = []

    try:
        materialization_id = int(materialization)
        materialization_selection_part = "m.id = %s"
        args.append(materialization_id)
    except ValueError:
        materialization_selection_part = "m::text = %s"
        args.append(materialization)

    query = (
        "SELECT m.id, m::text, ms.timestamp "
        "FROM trend_directory.materialization_state ms "
        "JOIN trend_directory.materialization m "
        "ON m.id = ms.materialization_id "
    )

    max_modified_supported = is_max_modified_supported(conn)

    if reset:
        where_clause = (
            "WHERE " + materialization_selection_part + " AND ms.timestamp < now() "
        )

        if max_modified_supported:
            where_clause += (
                "AND (ms.max_modified IS NULL "
                "OR ms.max_modified + m.processing_delay < now()) "
            )
    else:
        where_clause = (
            "WHERE " + materialization_selection_part + " AND ("
            "source_fingerprint != processed_fingerprint OR "
            "processed_fingerprint IS NULL"
            ") AND ms.timestamp < now() "
        )

        if max_modified_supported:
            where_clause += (
                "AND (ms.max_modified IS NULL "
                "OR ms.max_modified + m.processing_delay < now()) "
            )

    query += where_clause

    if newest_first:
        query += "ORDER BY ms.timestamp DESC "

    if max_num is not None:
        query += "LIMIT %s"
        args.append(max_num)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)

        rows = cursor.fetchall()

    conn.commit()

    return [MaterializationChunk(*row) for row in rows]


def materialize_selection(
    materializations, reset: bool, max_num: Optional[int], newest_first: bool
):
    with closing(connect()) as conn:
        for materialization in materializations:
            chunks = get_materialization_chunks_to_run(
                conn, materialization, reset, max_num, newest_first
            )

            for chunk in chunks:
                chunk.materialize(conn)
                conn.commit()


def is_max_modified_supported(conn) -> bool:
    """
    Returns true if the materialization_state.max_modified column is
    available in the database
    """
    query = (
        "select relname, attname "
        "from pg_class c "
        "join pg_namespace n on n.oid = relnamespace "
        "join pg_attribute a on a.attrelid = c.oid "
        "where nspname = 'trend_directory' "
        "and c.relname = 'materialization_state' "
        "and relkind = 'r' "
        "and attname = 'max_modified'"
    )

    with conn.cursor() as cursor:
        cursor.execute(query)

        return len(cursor.fetchall()) > 0


def materialize_all(reset: bool, max_num: Optional[int], newest_first: bool):
    query = (
        "SELECT m.id, m::text, ms.timestamp "
        "FROM trend_directory.materialization_state ms "
        "JOIN trend_directory.materialization m "
        "ON m.id = ms.materialization_id "
        "JOIN trend_directory.trend_store_part tsp "
        "ON tsp.id = m.dst_trend_store_part_id "
        "JOIN trend_directory.trend_store ts ON ts.id = tsp.trend_store_id "
        "WHERE now() - ts.retention_period < ms.timestamp "
    )
    args = []

    with closing(connect()) as conn:
        max_modified_supported = is_max_modified_supported(conn)

        if reset:
            where_clause = "AND m.enabled AND ms.timestamp < now() "

            if max_modified_supported:
                where_clause += (
                    "AND (ms.max_modified IS NULL "
                    "OR ms.max_modified + m.processing_delay < now()) "
                )
        else:
            where_clause = (
                "AND ("
                "source_fingerprint != processed_fingerprint OR "
                "processed_fingerprint IS NULL"
                ") AND m.enabled AND ms.timestamp < now() "
            )

            if max_modified_supported:
                where_clause += (
                    "AND (ms.max_modified IS NULL "
                    "OR ms.max_modified + m.processing_delay < now()) "
                )

        query += where_clause

        if newest_first:
            query += "ORDER BY ms.timestamp DESC "

        if max_num is not None:
            query += "LIMIT %s"
            args.append(max_num)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, args)

            chunks = [MaterializationChunk(*row) for row in cursor.fetchall()]

        conn.commit()

        for chunk in chunks:
            chunk.materialize(conn)

            conn.commit()


def set_lock_timeout(conn, duration: str):
    query = "SET lock_timeout = %s"
    args = (duration,)

    with conn.cursor() as cursor:
        cursor.execute(query, args)
