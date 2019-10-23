import sys
import argparse

from minerva.commands import data_source, trend_store, entity_type, load_data, \
    structure, alias, attribute_store, initialize, materialize, relation, \
    trigger, load_sample_data, virtual_entity


def main():
    parser = argparse.ArgumentParser(
        description='Minerva administration tool set'
    )

    subparsers = parser.add_subparsers()

    data_source.setup_command_parser(subparsers)
    trend_store.setup_command_parser(subparsers)
    attribute_store.setup_command_parser(subparsers)
    entity_type.setup_command_parser(subparsers)
    load_data.setup_command_parser(subparsers)
    structure.setup_command_parser(subparsers)
    alias.setup_command_parser(subparsers)
    initialize.setup_command_parser(subparsers)
    materialize.setup_command_parser(subparsers)
    relation.setup_command_parser(subparsers)
    trigger.setup_command_parser(subparsers)
    load_sample_data.setup_command_parser(subparsers)
    virtual_entity.setup_command_parser(subparsers)

    args = parser.parse_args()

    if 'cmd' not in args:
        parser.print_help()

        return 0
    else:
        return args.cmd(args)


if __name__ == '__main__':
    sys.exit(main())
