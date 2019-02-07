#!/usr/bin/env python3
import argparse

from minerva.commands import data_source, trend_store, entity_type, load_data, \
    structure, alias


def main():
    parser = argparse.ArgumentParser(
        description='Minerva administration tool set'
    )

    subparsers = parser.add_subparsers()

    data_source.setup_command_parser(subparsers)
    trend_store.setup_command_parser(subparsers)
    entity_type.setup_command_parser(subparsers)
    load_data.setup_command_parser(subparsers)
    structure.setup_command_parser(subparsers)
    alias.setup_command_parser(subparsers)

    args = parser.parse_args()

    if 'cmd' not in args:
        parser.print_help()

        return 0
    else:
        return args.cmd(args)
