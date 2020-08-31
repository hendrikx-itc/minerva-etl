from contextlib import closing
import argparse
import json

import yaml
from minerva.commands import show_rows_from_cursor

from minerva.db import connect
from minerva.storage.trend.materialization import from_config, Materialization


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'trend-materialization', help='command for administering trend materializations'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
    setup_update_parser(cmd_subparsers)
    setup_drop_parser(cmd_subparsers)
    setup_list_parser(cmd_subparsers)


def setup_create_parser(subparsers):
    cmd = subparsers.add_parser(
        'create', help='create a materialization'
    )

    cmd.add_argument(
        '--format', choices=['yaml', 'json'], default='yaml',
        help='format of definition'
    )

    cmd.add_argument(
        'definition', type=argparse.FileType('r'),
        help='file containing materialization definition'
    )

    cmd.set_defaults(cmd=create_materialization)


def setup_update_parser(subparsers):
    cmd = subparsers.add_parser(
        'update', help='update a materialization'
    )

    cmd.add_argument(
        '--format', choices=['yaml', 'json'], default='yaml',
        help='format of definition'
    )

    cmd.add_argument(
        'definition', type=argparse.FileType('r'),
        help='file containing materialization definition'
    )

    cmd.set_defaults(cmd=update_materialization)


def setup_drop_parser(subparsers):
    cmd = subparsers.add_parser(
        'drop', help='drop a materialization'
    )

    cmd.add_argument(
        'name', help='name of materialization (target trend store part)'
    )

    cmd.set_defaults(cmd=drop_materialization)


def setup_list_parser(subparsers):
    cmd = subparsers.add_parser(
        'list', help='list materializations'
    )

    cmd.set_defaults(cmd=list_materializations)


def create_materialization(args):
    if args.format == 'json':
        definition = json.load(args.definition)
    elif args.format == 'yaml':
        definition = yaml.load(args.definition, Loader=yaml.SafeLoader)

    define_materialization(definition)


def define_materialization(definition):
    materialization = from_config(definition)

    with closing(connect()) as conn:
        materialization.create(conn)

        conn.commit()

    print("Created materialization '{}'".format(definition['target_trend_store_part']))


def update_materialization(args):
    if args.format == 'json':
        definition = json.load(args.definition)
    elif args.format == 'yaml':
        definition = yaml.load(args.definition, Loader=yaml.SafeLoader)

    materialization = from_config(definition)

    with closing(connect()) as conn:
        materialization.update(conn)

        conn.commit()

    print("Updated materialization '{}'".format(definition['target_trend_store_part']))


def drop_materialization(args):
    materialization = Materialization(args.name)

    with closing(connect()) as conn:
        count = materialization.drop(conn)

        conn.commit()

    if count == 0:
        print("No materialization matched name '{}'".format(args.name))


def list_materializations(args):
    with closing(connect()) as conn:
        query = (
            "SELECT id, m::text AS name FROM trend_directory.materialization m ORDER BY m::text"
        )

        with conn.cursor() as cursor:
            cursor.execute(query)

            show_rows_from_cursor(cursor)

        conn.commit()
