import json
from contextlib import closing
import argparse
import sys

from minerva.commands import LoadHarvestPlugin, ListPlugins, load_json
from minerva.db import connect
from minerva.harvest.trend_config_deducer import deduce_config
from minerva.util.tabulate import render_table


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'trend-store', help='command for administering trend stores'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
    setup_deduce_parser(cmd_subparsers)
    setup_delete_parser(cmd_subparsers)
    setup_add_trend_parser(cmd_subparsers)
    setup_alter_trend_parser(cmd_subparsers)
    setup_show_parser(cmd_subparsers)
    setup_list_parser(cmd_subparsers)


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

    sys.stdout.write(
        "Creating trend store '{}' - '{}'... ".format(
            data['data_source'], data['entity_type']
        )
    )

    try:
        create_trend_store_from_json(data)
        sys.stdout.write("OK\n")
    except Exception as exc:
        sys.stdout.write("Error:\n{}".format(exc))


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
                    "('{}', '{}', '{}')".format(
                        trend['name'],
                        trend['data_type'],
                        trend.get('description', '')
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

    alias_query = (
        "SELECT alias_directory.get_or_create_alias_type('{}')".format(
            data['alias_type']
        )
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

    cmd.add_argument('id', help='id of trend store')

    cmd.set_defaults(cmd=delete_trend_store_cmd)


def delete_trend_store_cmd(args):
    query = (
        'SELECT trend_directory.delete_table_trend_store(%s)'
    )

    query_args = (
        args.id,
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
        'WITH ts AS (SELECT trend_directory.add_trend_to_trend_store('
        'table_trend_store_part, %s::name, %s::text, %s::text'
        ') as created '
        'FROM trend_directory.table_trend_store_part WHERE name = %s) '
        'SELECT (ts.created).* FROM ts'
    )

    query_args = (
        args.trend_name, args.data_type, '', args.trend_store
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            result = cursor.fetchone()

        conn.commit()

    (
        trend_id, table_trend_store_part_id, name, data_type, extra_data,
        description
    )= result

    print("Created trend '{}'".format(name))


def setup_alter_trend_parser(subparsers):
    cmd = subparsers.add_parser(
        'alter-trend', help='alter a trend of a trend store'
    )

    cmd.add_argument('--data-type', help='New data type')

    cmd.add_argument('--trend-name', help='New trend name')

    cmd.add_argument(
        'trend',
        help='Identifier of the trend to modify <trend_store_part>.<trend_name>'
    )

    cmd.set_defaults(cmd=alter_trend_cmd)


def alter_trend_cmd(_args):
    query = "SELECT 10"
    query_args = tuple()

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            print(cursor.fetchone())

        conn.commit()


def setup_show_parser(subparsers):
    cmd = subparsers.add_parser(
        'show', help='show information on a trend store'
    )

    cmd.add_argument(
        'trend_store_id', help='id of trend store', type=int
    )

    cmd.set_defaults(cmd=show_trend_store_cmd)


def show_rows(column_names, rows, show_cmd=print):
    column_align = "<" * len(column_names)
    column_sizes = ["max"] * len(column_names)

    for line in render_table(column_names, column_align, column_sizes, rows):
        show_cmd(line)


def show_rows_from_cursor(cursor, show_cmd=print):
    show_rows(
        [c.name for c in cursor.description],
        cursor.fetchall(),
        show_cmd
    )


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
        'JOIN directory.data_source ON data_source.id = data_source_id '
        'WHERE table_trend_store.id = %s'
    )

    query_args = (args.trend_store_id,)

    parts_query = (
        'SELECT '
        'tsp.id, '
        'tsp.name '
        'FROM trend_directory.table_trend_store_part tsp '
        'WHERE tsp.trend_store_id = %s'
    )

    parts_query_args = (args.trend_store_id,)

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            (
                id_, entity_type, data_source, granularity, partition_size,
                retention_period
            ) = cursor.fetchone()

            print("Table Trend Store")
            print("")
            print("id:               {}".format(id_))
            print("entity_type:      {}".format(entity_type))
            print("data_source:      {}".format(data_source))
            print("granularity:      {}".format(granularity))
            print("partition_size:   {}".format(partition_size))
            print("retention_period: {}".format(retention_period))
            print("parts:")

            cursor.execute(parts_query, parts_query_args)

            rows = cursor.fetchall()

            for part_id, part_name in rows:
                header = '{} ({})'.format(part_name, part_id)
                print("                  {}".format(header))
                print("                  {}".format('='*len(header)))

                part_query = (
                    'SELECT id, name, data_type '
                    'FROM trend_directory.table_trend '
                    'WHERE table_trend_store_part_id = %s'
                )
                part_args = (part_id, )

                cursor.execute(part_query, part_args)

                def show_cmd(line):
                    print("                  {}".format(line))

                show_rows_from_cursor(cursor, show_cmd)


def setup_list_parser(subparsers):
    cmd = subparsers.add_parser(
        'list', help='list trend stores'
    )

    cmd.set_defaults(cmd=list_trend_stores_cmd)


def list_trend_stores_cmd(_args):
    query = (
        'SELECT '
        'ts.id as id, '
        'data_source.name as data_source, '
        'entity_type.name as entity_type, '
        'ts.granularity '
        'FROM trend_directory.table_trend_store ts '
        'JOIN directory.data_source ON data_source.id = ts.data_source_id '
        'JOIN directory.entity_type ON entity_type.id = ts.entity_type_id'
    )

    query_args = []

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            show_rows_from_cursor(cursor)
