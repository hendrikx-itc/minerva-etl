from contextlib import closing
import argparse
import sys

import yaml
import dateutil.parser
import dateutil.tz

from minerva.db import connect
from minerva.trigger.trigger import Trigger
from minerva.util.tabulate import render_table


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'trigger', help='command for administering triggers'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
    setup_delete_parser(cmd_subparsers)
    setup_list_parser(cmd_subparsers)
    setup_create_notifications_parser(cmd_subparsers)


def setup_create_parser(subparsers):
    cmd = subparsers.add_parser(
        'create', help='command for creating triggers'
    )

    cmd.add_argument(
        '--from-yaml', type=argparse.FileType('r'),
        help='use yaml description for trend store'
    )

    cmd.add_argument(
        '--output-sql', help='output generated sql without executing it'
    )

    cmd.set_defaults(cmd=create_trigger_cmd)


def create_trigger_cmd(args):
    if args.from_yaml:
        trigger_config = yaml.load(args.from_yaml, Loader=yaml.SafeLoader)
    else:
        sys.stdout.write("No configuration provided\n")
        return

    sys.stdout.write(
        "Creating trigger '{}' ...\n".format(trigger_config['name'])
    )

    try:
        create_trigger_from_config(trigger_config)
    except Exception as exc:
        sys.stdout.write("Error:\n{}".format(str(exc)))
        raise exc

    sys.stdout.write("Done\n")


def create_trigger_from_config(config):
    trigger = Trigger.from_config(config)

    with closing(connect()) as conn:
        conn.autocommit = True

        for message in trigger.create(conn):
            print(message)


def setup_delete_parser(subparsers):
    cmd = subparsers.add_parser(
        'delete', help='command for deleting triggers'
    )

    cmd.add_argument('name', help='name of trigger')

    cmd.set_defaults(cmd=delete_trigger_cmd)


def delete_trigger_cmd(args):
    with closing(connect()) as conn:
        conn.autocommit = True

        Trigger(args.name).delete(conn)


def setup_list_parser(subparsers):
    cmd = subparsers.add_parser(
        'list', help='command for listing triggers'
    )

    cmd.set_defaults(cmd=list_cmd)


def list_cmd(args):
    query = 'SELECT id, name, enabled FROM trigger.rule'

    with closing(connect()) as conn:
        conn.autocommit = True

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()

    show_rows(['id', 'name', 'enabled'], rows)


def show_rows(column_names, rows, show_cmd=print):
    column_align = "<" * len(column_names)
    column_sizes = ["max"] * len(column_names)

    for line in render_table(column_names, column_align, column_sizes, rows):
        show_cmd(line)


def setup_create_notifications_parser(subparsers):
    cmd = subparsers.add_parser(
        'create-notifications',
        help='command for executing triggers and creating notifications'
    )

    cmd.add_argument('--trigger', help="name of trigger")

    cmd.add_argument(
        '--timestamp', help="timestamp for which to execute trigger"
    )

    cmd.set_defaults(cmd=execute_trigger_cmd)


def execute_trigger_cmd(args):
    if args.timestamp:
        timestamp = dateutil.parser.parse(args.timestamp)
    else:
        timestamp = None

    trigger = Trigger(args.trigger)

    with closing(connect()) as conn:
        conn.autocommit = True

        notification_count = trigger.execute(conn, timestamp)

    print("Notifications generated: {}".format(notification_count))
