"""Provides the 'remove-trends' sub-command."""
import sys
from contextlib import closing
from pathlib import Path

from minerva.db import connect
from minerva.instance import MinervaInstance, TrendStore


def setup_remove_trends_parser(subparsers):
    cmd = subparsers.add_parser(
        "remove-trends", help="command for removing trends from trend stores"
    )

    cmd.add_argument(
        "definition", type=Path, help="file containing trend store definition"
    )

    cmd.set_defaults(cmd=remove_trends_cmd)


def remove_trends_cmd(args):
    instance = MinervaInstance.load()

    trend_store = instance.load_trend_store_from_file(args.definition)

    try:
        result = remove_trends_from_trend_store(trend_store)
        if result:
            sys.stdout.write(f"Removed trends: {result}\n")
        else:
            sys.stdout.write("No trends to be removed.\n")
    except Exception as exc:
        sys.stdout.write(f"Error:\n{exc}")
        raise exc


def remove_trends_from_trend_store(trend_store: TrendStore):
    query = (
        "SELECT trend_directory.remove_extra_trends("
        "trend_directory.get_trend_store("
        "%s::text, %s::text, %s::interval"
        "), %s::trend_directory.trend_store_part_descr[]"
        ")"
    )

    query_args = (
        trend_store.data_source,
        trend_store.entity_type,
        trend_store.granularity,
        trend_store.parts,
    )

    with closing(connect()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, query_args)
            result = cursor.fetchone()

        conn.commit()

    if result[0]:
        return ", ".join(str(r) for r in result[0])
    else:
        return None
