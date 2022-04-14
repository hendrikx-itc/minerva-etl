"""Provides the 'create' sub-command."""
import sys
from contextlib import closing
from pathlib import Path

from minerva.commands.partition import create_partitions_for_trend_store
from minerva.instance import TrendStore, MinervaInstance
from minerva.db import connect
from minerva.db.error import UniqueViolation


class DuplicateTrendStore(Exception):
    """Indicates that a trend store with these properties already exists."""

    def __init__(self, data_source, entity_type, granularity):
        super().__init__()
        self.data_source = data_source
        self.entity_type = entity_type
        self.granularity = granularity

    def __str__(self):
        return "Duplicate trend store {}, {}, {}".format(
            self.data_source, self.entity_type, self.granularity
        )


def setup_create_parser(subparsers):
    """Setup the argument parser for the 'create' sub-command."""
    cmd = subparsers.add_parser("create", help="command for creating trend stores")

    cmd.add_argument(
        "--create-partitions",
        default=False,
        action="store_true",
        help="create partitions according to retention configuration",
    )

    cmd.add_argument(
        "definition", type=Path, help="file containing trend store definition"
    )

    cmd.set_defaults(cmd=create_trend_store_cmd)


def create_trend_store_cmd(args):
    trend_store = MinervaInstance.load_trend_store_from_file(args.definition)

    sys.stdout.write(
        f"Creating trend store '{trend_store.data_source}' - '{trend_store.entity_type}' - '{trend_store.granularity}' ... "  # pylint: disable=C0301
    )

    try:
        create_trend_store(trend_store, args.create_partitions)
        sys.stdout.write("OK\n")
    except DuplicateTrendStore as exc:
        print(f"Could not create trend store: {exc}")
    except Exception as exc:
        sys.stdout.write(f"Error:\n{exc}")
        raise exc


def create_trend_store(trend_store_definition: TrendStore, create_partitions: bool):
    query = (
        "SELECT id "
        "FROM trend_directory.create_trend_store("
        "%s::text, %s::text, %s::interval, %s::interval, "
        "%s::trend_directory.trend_store_part_descr[]"
        ")"
    )
    query_args = (
        trend_store_definition.data_source,
        trend_store_definition.entity_type,
        trend_store_definition.granularity,
        trend_store_definition.partition_size,
        trend_store_definition.parts,
    )

    with closing(connect()) as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(query, query_args)
            except UniqueViolation:
                raise DuplicateTrendStore(
                    trend_store_definition.data_source,
                    trend_store_definition.entity_type,
                    trend_store_definition.granularity,
                )

            (trend_store_id,) = cursor.fetchone()

            if create_partitions:
                print()
                ahead_interval = "3 days"

                for (
                    name,
                    partition_index,
                    index,
                    total,
                ) in create_partitions_for_trend_store(
                    conn, trend_store_id, ahead_interval
                ):
                    print(
                        f"created partition of part '{name}_{partition_index}' {index + 1}/{total}"
                    )

        conn.commit()
