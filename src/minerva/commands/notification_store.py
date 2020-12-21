from contextlib import closing
import argparse
import sys

import psycopg2

from minerva.db import connect
from minerva.db.error import UniqueViolation
from minerva.instance import MinervaInstance, NotificationStore, load_yaml


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
        'definition', type=argparse.FileType('r'),
        help='definition of attribute store'
    )

    cmd.set_defaults(cmd=create_notification_store_cmd)


def create_notification_store_cmd(args):
    # definition = load_yaml(args.definition)
    # notification_store_name = definition['data_source']

    notification_store = MinervaInstance.load_notification_store_from_file(args.definition)
    notification_store_name = notification_store.data_source

    sys.stdout.write(
        "Creating notification store '{}'... ".format(notification_store_name)
    )

    try:
        create_notification_store_from_definition(notification_store)
        sys.stdout.write("OK\n")
    except DuplicateNotificationStore as exc:
        sys.stdout.write(str(exc))


def create_notification_store_from_definition(notification_store: NotificationStore):
    query = (
        'SELECT notification_directory.create_notification_store('
        '%s::text, {}'
        ')'
    ).format(
        'ARRAY[{}]::notification_directory.attr_def[]'.format(','.join([
            "('{}', '{}', '{}')".format(
                attribute.name,
                attribute.data_type,
                ''
            )
            for attribute in notification_store.attributes
        ]))
    )

    query_args = (
        notification_store.data_source,
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            try:
                cursor.execute(query, query_args)
            except UniqueViolation as exc:
                raise DuplicateNotificationStore(notification_store.data_source) from exc

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
