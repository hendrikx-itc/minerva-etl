from contextlib import closing
import argparse
import sys

import yaml
import psycopg2
from psycopg2 import sql
import dateutil.parser
import dateutil.tz

from minerva.db import connect


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'trigger', help='command for administering triggers'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
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
    with closing(connect()) as conn:
        conn.autocommit = True

        print(" - creating KPI type")

        try:
            create_kpi_type(conn, config)
        except psycopg2.errors.DuplicateObject as exc:
            # Type already exists
            sys.stdout.write('Type exists already\n')

        print(" - creating KPI function")

        try:
            create_kpi_function(conn, config)
        except psycopg2.errors.DuplicateFunction as exc:
            # Function already exists
            sys.stdout.write('Function exists already\n')

        #set_fingerprint(conn, config)

        print(" - creating rule")

        create_rule(conn, config)

        print(" - setting thresholds")

        set_thresholds(conn, config)

        print(" - defining notification")

        define_notification(conn, config)


def create_kpi_type(conn, config):
    type_name = '{}_kpi'.format(config['name'])

    column_specs = [
        ('entity_id', 'integer'),
        ('timestamp', 'timestamp with time zone')
    ]

    column_specs.extend(
        (kpi_column['name'], kpi_column['data_type'])
        for kpi_column in config['kpi_data']
    )

    columns = [
        sql.SQL('{{}} {}'.format(data_type)).format(sql.Identifier(name))
        for name, data_type in column_specs
    ]

    columns_part = sql.SQL(', ').join(columns)

    query_parts = [
        sql.SQL(
            "CREATE TYPE trigger_rule.{} AS ("
        ).format(sql.Identifier(type_name)),
        columns_part,
        sql.SQL(')')
    ]

    query = sql.SQL('').join(query_parts)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)


def create_kpi_function(conn, config):
    function_name = '{}_kpi'.format(config['name'])
    type_name = '{}_kpi'.format(config['name'])

    query_parts = [
        sql.SQL(
            'CREATE FUNCTION trigger_rule.{}(timestamp with time zone)\n'
            'RETURNS SETOF trigger_rule.{}\n'
            'AS $trigger$'
        ).format(sql.Identifier(function_name), sql.Identifier(type_name)),
        sql.SQL(config['kpi_function']),
        sql.SQL(
            '$trigger$ LANGUAGE plpgsql STABLE;'
        )
    ]

    query = sql.SQL('').join(query_parts)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)


def define_notification(conn, config):
    query = 'SELECT trigger.define_notification(%s, %s)'

    query_args = (
        config['name'],
        config['notification']
    )

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, query_args)


def set_fingerprint(conn, config):
    query = sql.SQL('SELECT trigger.set_fingerprint({}, {});').format(
        sql.Literal(config['name']),
        sql.Literal(config['fingerprint'])
    )

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)


def create_rule(conn, config):
    create_query = (
        "SELECT * "
        "FROM trigger.create_rule('{}', array[{}]::trigger.threshold_def[]);"
    ).format(
        config['name'],
        ','.join(
            "('{}', '{}')".format(threshold['name'], threshold['data_type'])
            for threshold in config['thresholds']
        )
    )

    set_notification_store_query = (
        "UPDATE trigger.rule "
        "SET notification_store_id = notification_store.id "
        "FROM notification_directory.notification_store "
        "JOIN directory.data_source "
        "ON data_source.id = notification_store.data_source_id "
        "WHERE rule.id = %s AND data_source.name = %s"
    )

    with closing(conn.cursor()) as cursor:
        cursor.execute(create_query)

        row = cursor.fetchone()

        rule_id, _, _, _, _, _ = row

        cursor.execute(
            set_notification_store_query,
            (rule_id, config['notification_store'])
        )


def set_thresholds(conn, config):
    function_name = '{}_set_thresholds'.format(config['name'])

    query = 'SELECT trigger_rule."{}"({})'.format(
        function_name,
        ','.join(len(config['thresholds']) * ['%s'])
    )

    query_args = tuple(threshold['value'] for threshold in config['thresholds'])

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, query_args)


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

    query = "SELECT * FROM trigger.create_notifications(%s, %s)"
    query_args = (args.trigger, timestamp)

    with closing(connect()) as conn:
        conn.autocommit = True

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            row = cursor.fetchone()

            print(row)
