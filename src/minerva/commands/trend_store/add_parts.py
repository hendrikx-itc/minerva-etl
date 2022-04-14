"""Provides the 'add-parts' sub-command."""
from typing import Generator
import sys
from contextlib import closing
from pathlib import Path

from minerva.db import connect
from minerva.instance import MinervaInstance, TrendStore


def setup_add_parts_parser(subparsers):
    """Setup the argument parser for the 'add-parts' sub-command."""
    cmd = subparsers.add_parser(
        "add-parts", help="command for adding parts to trend stores"
    )

    cmd.add_argument(
        "definition", type=Path, help="file containing trend store definition"
    )

    cmd.set_defaults(cmd=add_parts_cmd)


def add_parts_cmd(args):
    instance = MinervaInstance.load()

    trend_store = instance.load_trend_store_from_file(args.definition)

    sys.stdout.write(
        "Adding trend store parts to trend store '{}' - '{}' - '{}' ... \n".format(  # pylint: disable=C0301
            trend_store.data_source, trend_store.entity_type, trend_store.granularity
        )
    )

    parts_added = 0

    try:
        for added_part_name in add_parts_to_trend_store(trend_store):
            sys.stdout.write(f" - added {added_part_name}\n")
            parts_added += 1
    except Exception as exc:
        sys.stdout.write(f"Error:\n{exc}")
        raise exc

    if parts_added:
        sys.stdout.write(f"Done: Added {parts_added} parts\n")
    else:
        sys.stdout.write("Done: Nothing to add\n")


def add_parts_to_trend_store(trend_store: TrendStore) -> Generator[str, None, None]:
    """Add any new trend store parts in the database."""
    query = (
        "select tsp.name "
        "from trend_directory.trend_store ts "
        "join directory.data_source ds on ds.id = ts.data_source_id "
        "join directory.entity_type et on et.id = ts.entity_type_id "
        "join trend_directory.trend_store_part tsp "
        "on tsp.trend_store_id = ts.id "
        "where ds.name = %s and et.name = %s and ts.granularity = %s::interval"
    )

    query_args = (
        trend_store.data_source,
        trend_store.entity_type,
        trend_store.granularity,
    )

    with closing(connect()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, query_args)

            trend_store_part_names = [name for name, in cursor.fetchall()]

            missing_parts = [
                part
                for part in trend_store.parts
                if part.name not in trend_store_part_names
            ]

            for missing_part in missing_parts:
                add_part_query = (
                    "select trend_directory.initialize_trend_store_part("
                    "trend_directory.define_trend_store_part("
                    "ts.id, %s, %s::trend_directory.trend_descr[],"
                    "%s::trend_directory.generated_trend_descr[]"
                    ")"
                    ") "
                    "from trend_directory.trend_store ts "
                    "join directory.data_source ds on ds.id = ts.data_source_id "
                    "join directory.entity_type et on et.id = ts.entity_type_id "
                    "where ds.name = %s and et.name = %s and ts.granularity = %s::interval"
                )

                add_part_query_args = (
                    missing_part.name,
                    missing_part.trends,
                    missing_part.generated_trends,
                    trend_store.data_source,
                    trend_store.entity_type,
                    trend_store.granularity,
                )

                cursor.execute(add_part_query, add_part_query_args)

                yield missing_part.name

        conn.commit()
