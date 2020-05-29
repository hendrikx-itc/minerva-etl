import json
from contextlib import closing
import argparse
import sys

import psycopg2
import yaml

from minerva.db import connect


class DuplicateNotificationStore(Exception):
    def __init__(self, data_source):
        self.data_source = data_source

    def __str__(self):
        return 'Duplicate notification store {}'.format(
            self.data_source
        )


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'notification-store',
        help='command for administering notification stores'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
    setup_delete_parser(cmd_subparsers)


def setup_create_parser(subparsers):
    cmd = subparsers.add_parser(
        'create', help='command for creating notification stores'
    )

    cmd.add_argument(
        '--data-source',
        help='name of the data source of the new attribute store'
    )

    cmd.add_argument(
        '--from-json', type=argparse.FileType('r'),
        help='use json description for attribute store'
    )

    cmd.add_argument(
        '--from-yaml', type=argparse.FileType('r'),
        help='use yaml description for attribute store'
    )

    cmd.set_defaults(cmd=create_notification_store_cmd)


def create_notification_store_cmd(args):
    if args.from_json:
        notification_store_config = json.load(args.from_json)
    elif args.from_yaml:
        notification_store_config = yaml.load(
            args.from_yaml, Loader=yaml.SafeLoader
        )
    else:
        notification_store_config = {
            'data_source': 'example_source',
            'attributes': []
        }

    if args.data_source:
        notification_store_config['data_source'] = args.data_source

    notification_store_name = notification_store_config['data_source']

    sys.stdout.write(
        "Creating notification store '{}'... ".format(notification_store_name)
    )

    try:
        create_notification_store_from_json(notification_store_config)
        sys.stdout.write("OK\n")
    except DuplicateNotificationStore as exc:
        sys.stdout.write(exc)


def create_notification_store_from_json(data):
    query = (
        'SELECT notification_directory.create_notification_store('
        '%s::text, {}'
        ')'
    ).format(
        'ARRAY[{}]::notification_directory.attr_def[]'.format(','.join([
            "('{}', '{}', '{}')".format(
                attribute['name'],
                attribute['data_type'],
                attribute.get('description', '')
            )
            for attribute in data['attributes']
        ]))
    )

    query_args = (
        data['data_source'],
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            try:
                cursor.execute(query, query_args)
            except psycopg2.errors.UniqueViolation as exc:
                raise DuplicateNotificationStore(data['data_source']) from exc

        conn.commit()


def setup_delete_parser(subparsers):
    cmd = subparsers.add_parser(
        'delete', help='command for deleting notification stores'
    )

    cmd.add_argument('name', help='name of notification store')

    cmd.set_defaults(cmd=delete_notification_store_cmd)


def delete_notification_store_cmd(args):
    query = (
        'SELECT notification_directory.delete_notification_store(%s::name)'
    )

    query_args = (
        args.name,
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

        conn.commit()
