import json
from contextlib import closing
import argparse
import datetime

from minerva.commands import LoadHarvestPlugin, ListPlugins, load_json
from minerva.db import connect
from minerva.harvest.trend_config_deducer import deduce_config
from minerva.util.tabulate import render_table
from minerva.storage.trend.tabletrendstore import TableTrendStore


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'trend-store', help='command for administering trend stores'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
    setup_deduce_parser(cmd_subparsers)
    setup_delete_parser(cmd_subparsers)
    setup_add_trend_parser(cmd_subparsers)
    setup_show_parser(cmd_subparsers)
    setup_create_partition_parser(cmd_subparsers)


def setup_create_parser(subparsers):
    cmd = subparsers.add_parser(
        'create', help='command for creating trend stores'
    )

    cmd.add_argument(
        '--data-source',
        help='name of the data source of the new trend store'
    )

    cmd.add_argument(
        '--entity-type',
        help='name of the entity type of the new trend store'
    )

    cmd.add_argument(
        '--granularity',
        help='granularity of the new trend store'
    )

    cmd.add_argument(
        '--partition-size',
        help='partition size of the new trend store'
    )

    cmd.add_argument(
        '--from-json', type=argparse.FileType('r'),
        help='use json description for trend store'
    )

    cmd.set_defaults(cmd=create_trend_store_cmd)


def create_trend_store_cmd(args):
    if args.from_json:
        data = json.load(args.from_json)
    else:
        data = {
            'parts': []
        }

    if args.data_source:
        data['data_source'] = args.data_source

    if args.entity_type:
        data['entity_type'] = args.entity_type

    if args.granularity:
        data['granularity'] = args.granularity

    if args.partition_size:
        data['partition_size'] = args.partition_size

    if 'alias_type' not in data and 'entity_type' in data:
        data['alias_type'] = data['entity_type']

    create_trend_store_from_json(data)


def setup_deduce_parser(subparsers):
    cmd = subparsers.add_parser(
        'deduce', help='command for deducing trend stores from data'
    )

    cmd.add_argument(
        "file_path", nargs="?",
        help="path of file that will be processed"
    )

    cmd.add_argument(
        "-p", "--plugin", action=LoadHarvestPlugin,
        help="harvester plug-in to use for processing file(s)"
    )

    cmd.add_argument(
        "-l", "--list-plugins", action=ListPlugins,
        help="list installed Harvester plug-ins")

    cmd.add_argument(
        "--parser-config", type=load_json,
        help="parser specific configuration"
    )

    cmd.add_argument(
        '--data-source', default='default',
        help='name of the data source of the trend store'
    )

    cmd.add_argument(
        '--granularity', default='1 day',
        help='granularity of the new trend store'
    )

    cmd.add_argument(
        '--partition-size', default=86400,
        help='partition size of the trend store'
    )

    cmd.set_defaults(cmd=deduce_trend_store_cmd(cmd))


def deduce_trend_store_cmd(cmd_parser):
    def cmd(args):
        if 'plugin' not in args:
            cmd_parser.print_help()
            return

        parser = args.plugin.create_parser(args.parser_config)

        config = deduce_config(args.file_path, parser)

        print(json.dumps(config, sort_keys=True, indent=4))

    return cmd


def create_trend_store_from_json(data):
    query = (
        'SELECT trend_directory.create_table_trend_store('
        '%s::text, %s::text, %s::interval, %s::integer, {}'
        ')'
    ).format(
        "ARRAY[{}]::trend_directory.table_trend_store_part_descr[]".format(','.join([
            "('{}', {})".format(
                part['name'],
                'ARRAY[{}]::trend_directory.trend_descr[]'.format(','.join([
                    "('{}', '{}', '')".format(
                        trend['name'],
                        trend['data_type'],
                        ''
                    )
                    for trend in part['trends']
                ]))
            )
            for part in data['parts']
        ]))
    )

    query_args = (
        data['data_source'], data['entity_type'],
        data['granularity'], data['partition_size']
    )

    alias_query = "SELECT alias_directory.get_or_create_alias_type('{}')".format(
        data['alias_type']
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)
            cursor.execute(alias_query)

        conn.commit()


def setup_delete_parser(subparsers):
    cmd = subparsers.add_parser(
        'delete', help='command for deleting trend stores'
    )

    cmd.add_argument('name', help='name of trend store')

    cmd.set_defaults(cmd=delete_trend_store_cmd)


def delete_trend_store_cmd(args):
    query = (
        'SELECT trend_directory.delete_table_trend_store(%s::name)'
    )

    query_args = (
        args.name,
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

        conn.commit()


def setup_add_trend_parser(subparsers):
    cmd = subparsers.add_parser(
        'add-trend', help='add a trend to a trend store'
    )

    cmd.add_argument('--data-type')

    cmd.add_argument('--trend-name')

    cmd.add_argument('--part-name')

    cmd.add_argument(
        'trend_store',
        help='name of the trend store where the trend will be added'
    )

    cmd.set_defaults(cmd=add_trend_to_trend_store_cmd)


def add_trend_to_trend_store_cmd(args):
    query = (
        'SELECT trend_directory.add_trend_to_trend_store('
        'table_trend_store_part, %s::name, %s::text, %s::text'
        ') '
        'FROM trend_directory.table_trend_store_part WHERE name = %s'
    )

    query_args = (
        args.trend_name, args.data_type, '', args.trend_store
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            print(cursor.fetchone())

        conn.commit()


def setup_show_parser(subparsers):
    cmd = subparsers.add_parser(
        'show', help='show information on trend stores'
    )

    cmd.add_argument(
        '--id', help='id of trend store', type=int
    )

    cmd.set_defaults(cmd=show_trend_store_cmd)


def show_rows(column_names, rows):
    column_align = "<" * len(column_names)
    column_sizes = ["max"] * len(column_names)

    for line in render_table(column_names, column_align, column_sizes, rows):
        print(line)


def show_rows_from_cursor(cursor):
    show_rows([c.name for c in cursor.description], cursor.fetchall())


def show_trend_store_cmd(args):
    query = (
        'SELECT '
        'table_trend_store.id, '
        'entity_type.name AS entity_type, '
        'data_source.name AS data_source, '
        'table_trend_store.granularity,'
        'table_trend_store.partition_size, '
        'table_trend_store.retention_period '
        'FROM trend_directory.table_trend_store '
        'JOIN directory.entity_type ON entity_type.id = entity_type_id '
        'JOIN directory.data_source ON data_source.id = data_source_id'
    )

    query_args = []

    if args.id:
        query += ' WHERE table_trend_store.id = %s'
        query_args.append(
            args.id
        )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            show_rows_from_cursor(cursor)


def setup_create_partition_parser(subparsers):
    cmd = subparsers.add_parser(
        'create-partition', help='create partition for trend store'
    )

    cmd.add_argument(
        '--id', help='id of trend store', type=int
    )

    cmd.add_argument(
        '--part-name', help='name of trend store part'
    )

    cmd.add_argument(
        '--timestamp', default=datetime.datetime.now(),
        help='timestamp for which to create partition'
    )

    cmd.set_defaults(cmd=create_partition_cmd)


def create_partition_cmd(args):
    with closing(connect()) as conn:
        table_trend_store = TableTrendStore.get_by_id(args.id)(conn)
        try:
            timestamp = int(args.timestamp)
        except:
            # args.timestamp was not a number, assume it's a datetime already
            timestamp = args.timestamp
        else:
            # args.timestamp was a number, assume it's a unix timestamp
            timestamp = datetime.datetime.fromtimestamp(timestamp)


        if args.part_name:
            table_trend_store.partition(
                args.part_name, timestamp
            ).create(conn)
        else:
            table_trend_store.create_partitions(timestamp)(conn)

        conn.commit()
