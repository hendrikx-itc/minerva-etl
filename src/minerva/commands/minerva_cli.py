import sys
import argparse

from minerva import __version__
from minerva.error import ConfigurationError
from minerva.commands import data_source, trend_store, entity_type, load_data, \
    structure, alias, attribute_store, initialize, relation, \
    trigger, load_sample_data, virtual_entity, notification_store, \
    aggregation, live_monitor, trend_materialization, quick_start, report, \
    generate_sample_data


def main():
    parser = argparse.ArgumentParser(
        description='Minerva administration tool set'
    )

    parser.add_argument(
        '--version', '-v', action="store_true", default=False,
        help="show Minerva version"
    )

    subparsers = parser.add_subparsers()

    aggregation.setup_command_parser(subparsers)
    alias.setup_command_parser(subparsers)
    attribute_store.setup_command_parser(subparsers)
    data_source.setup_command_parser(subparsers)
    entity_type.setup_command_parser(subparsers)
    initialize.setup_command_parser(subparsers)
    live_monitor.setup_command_parser(subparsers)
    load_data.setup_command_parser(subparsers)
    load_sample_data.setup_command_parser(subparsers)
    notification_store.setup_command_parser(subparsers)
    quick_start.setup_command_parser(subparsers)
    relation.setup_command_parser(subparsers)
    structure.setup_command_parser(subparsers)
    trend_materialization.setup_command_parser(subparsers)
    trend_store.setup_command_parser(subparsers)
    trigger.setup_command_parser(subparsers)
    virtual_entity.setup_command_parser(subparsers)
    report.setup_command_parser(subparsers)
    generate_sample_data.setup_command_parser(subparsers)

    args = parser.parse_args()

    if args.version:
        print("minerva {}".format(__version__))

        return 0

    if 'cmd' not in args:
        parser.print_help()

        return 0
    else:
        try:
            return args.cmd(args)
        except ConfigurationError as e:
            print(f'Configuration error: {e}')


if __name__ == '__main__':
    sys.exit(main())
