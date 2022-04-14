"""Provides the 'check-config' sub-command."""
from pathlib import Path

from minerva.instance import MinervaInstance, TrendStore


def setup_check_config_parser(subparsers):
    """Setup the argument parser for the 'check-config' sub-command."""
    cmd = subparsers.add_parser(
        "check-config", help="check trend stores from configuration"
    )

    cmd.add_argument(
        "definition",
        type=Path,
        nargs="?",
        help="file containing trend store definition",
    )

    cmd.set_defaults(cmd=check_config_trend_stores_cmd)


def check_config_trend_stores_cmd(args):
    instance = MinervaInstance.load()

    if args.definition:
        trend_store = instance.load_trend_store_from_file(args.definition)

        check_trend_store(args.definition, trend_store)
    else:
        trend_stores_with_errors = 0

        for definition_path in instance.list_trend_stores():
            trend_store = instance.load_trend_store_from_file(definition_path)

            error_count = check_trend_store(definition_path, trend_store)

            if error_count:
                trend_stores_with_errors += 1

        if trend_stores_with_errors == 0:
            print("All trend stores Ok")
        else:
            print(f"{trend_stores_with_errors} trend stores with errors")


def check_trend_store(definition_path: Path, trend_store: TrendStore):
    error_count = 0

    # Check for duplicate part names
    for part in trend_store.parts:
        matching_parts = [p for p in trend_store.parts if p.name == part.name]
        name_occurrences = len(matching_parts)

        if name_occurrences > 1:
            print(
                f"There are {name_occurrences} parts named '{part.name}', but part names must be unique"  # pylint: disable=C0301
            )

            error_count += 1

    if error_count == 0:
        print(f"{definition_path} - Ok")
    else:
        print(f"{definition_path} - {error_count} errors")

    return error_count
