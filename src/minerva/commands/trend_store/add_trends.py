import sys
from contextlib import closing
from pathlib import Path

from minerva.instance import MinervaInstance, TrendStore
from minerva.db import connect


def setup_add_trends_parser(subparsers):
    """Setup the argument parser for the 'add-trends' sub-command."""
    cmd = subparsers.add_parser(
        "add-trends", help="command for adding trends to trend stores"
    )

    cmd.add_argument(
        "definition", type=Path, help="file containing trend store definition"
    )

    cmd.set_defaults(cmd=add_trends_cmd)


def add_trends_cmd(args):
    instance = MinervaInstance.load()

    trend_store = instance.load_trend_store_from_file(args.definition)

    try:
        result = add_trends_to_trend_store(trend_store)

        if result:
            sys.stdout.write(f"Added trends: {result}\n")
        else:
            sys.stdout.write("No trends to be added\n")
    except Exception as exc:
        sys.stdout.write(f"Error:\n{exc}")
        raise exc


def add_trends_to_trend_store(trend_store_definition: TrendStore):
    query = (
        "SELECT trend_directory.add_trends("
        "trend_directory.get_trend_store("
        "%s::text, %s::text, %s::interval"
        "), %s::trend_directory.trend_store_part_descr[]"
        ")"
    )

    query_args = (
        trend_store_definition.data_source,
        trend_store_definition.entity_type,
        trend_store_definition.granularity,
        trend_store_definition.parts,
    )

    with closing(connect()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, query_args)
            result = cursor.fetchone()

        conn.commit()

    return ", ".join(str(r) for r in result[0])
