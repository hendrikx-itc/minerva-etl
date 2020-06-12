import json
from contextlib import closing
import argparse
import sys
import datetime
from typing import BinaryIO, Generator

import yaml
import psycopg2
from psycopg2.extensions import adapt

from minerva.commands import LoadHarvestPlugin, ListPlugins, load_json, ConfigurationError
from minerva.db import connect
from minerva.harvest.trend_config_deducer import deduce_config
from minerva.util.tabulate import render_table
from minerva.commands.partition import create_partitions_for_trend_store, \
    create_specific_partitions_for_trend_store
from minerva.instance import TrendStorePart, TrendStore, MinervaInstance


class DuplicateTrendStore(Exception):
    def __init__(self, data_source, entity_type, granularity):
        self.data_source = data_source
        self.entity_type = entity_type
        self.granularity = granularity

    def __str__(self):
        return 'Duplicate trend store {}, {}, {}'.format(
            self.data_source,
            self.entity_type,
            self.granularity
        )


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'trend-store', help='command for administering trend stores'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
    setup_add_trends_parser(cmd_subparsers)
    setup_add_parts_parser(cmd_subparsers)
    setup_remove_parser(cmd_subparsers)
    setup_alter_trends_parser(cmd_subparsers)
    setup_change_trends_parser(cmd_subparsers)
    setup_deduce_parser(cmd_subparsers)
    setup_delete_parser(cmd_subparsers)
    setup_show_parser(cmd_subparsers)
    setup_list_parser(cmd_subparsers)
    setup_list_config_parser(cmd_subparsers)
    setup_partition_parser(cmd_subparsers)
    setup_process_modified_log_parser(cmd_subparsers)
    setup_materialize_parser(cmd_subparsers)


def setup_create_parser(subparsers):
    cmd = subparsers.add_parser(
        'create', help='command for creating trend stores'
    )

    cmd.add_argument(
        '--format', choices=['yaml', 'json'], default='yaml',
        help='format of definition'
    )

    cmd.add_argument(
        'definition', type=argparse.FileType('r'),
        help='file containing trend store definition'
    )

    cmd.set_defaults(cmd=create_trend_store_cmd)


def load_definition(in_file: BinaryIO, file_format: str) -> dict:
    """
    Load definition data in one of the supported formats (YAML, JSON)
    :param in_file: Path of the file to load
    :param file_format: Format of the data
    :return: dict
    """
    if file_format == 'json':
        return json.load(in_file)
    elif file_format == 'yaml':
        return yaml.load(in_file, Loader=yaml.SafeLoader)
    else:
        raise ConfigurationError('Unsupported format: {format}')


def create_trend_store_cmd(args):
    trend_store_config = load_definition(args.definition, args.format)

    sys.stdout.write(
        "Creating trend store '{}' - '{}' - '{}' ... ".format(
            trend_store_config['data_source'],
            trend_store_config['entity_type'],
            trend_store_config['granularity']
        )
    )

    try:
        create_trend_store(trend_store_config)
        sys.stdout.write("OK\n")
    except Exception as exc:
        sys.stdout.write("Error:\n{}".format(str(exc)))
        raise exc


def setup_add_trends_parser(subparsers):
    cmd = subparsers.add_parser(
        'add-trends', help='command for adding trends to trend stores'
    )

    cmd.add_argument(
        '--format', choices=['yaml', 'json'], default='yaml',
        help='format of definition'
    )

    cmd.add_argument(
        'definition', type=argparse.FileType('r'),
        help='file containing trend store definition'
    )

    cmd.set_defaults(cmd=add_trends_cmd)


def add_trends_cmd(args):
    trend_store = load_trend_store(args.definition, args.format)

    try:
        result = add_trends_to_trend_store(trend_store)

        if result:
            sys.stdout.write(f"Added trends: {result}\n")
        else:
            sys.stdout.write("No trends to be added\n")
    except Exception as exc:
        sys.stdout.write("Error:\n{}".format(str(exc)))
        raise exc


def setup_add_parts_parser(subparsers):
    cmd = subparsers.add_parser(
        'add-parts', help='command for adding parts to trend stores'
    )

    cmd.add_argument(
        '--format', choices=['yaml', 'json'], default='yaml',
        help='format of definition (default is yaml)'
    )

    cmd.add_argument(
        'definition', type=argparse.FileType('r'),
        help='file containing trend store definition'
    )

    cmd.set_defaults(cmd=add_parts_cmd)


def load_trend_store(in_file: BinaryIO, file_format: str) -> TrendStore:
    trend_store_def = load_definition(in_file, file_format)

    return TrendStore.from_dict(trend_store_def)


def setup_remove_parser(subparsers):
    cmd = subparsers.add_parser(
        'remove-trends', help='command for removing trends from trend stores'
    )

    cmd.add_argument(
        '--format', choices=['yaml', 'json'], default='yaml',
        help='format of definition'
    )

    cmd.add_argument(
        'definition', type=argparse.FileType('r'),
        help='file containing trend store definition'
    )

    cmd.set_defaults(cmd=remove_trends_cmd)


def remove_trends_cmd(args):
    trend_store_definition = load_trend_store(args.definition, args.format)

    try:
        result = remove_trends_from_trend_store(trend_store_definition)
        if result:
            sys.stdout.write(f"Removed trends: {result}\n")
        else:
            sys.stdout.write("No trends to be removed.\n")
    except Exception as exc:

        sys.stdout.write("Error:\n{}".format(str(exc)))
        raise exc


def setup_alter_trends_parser(subparsers):
    cmd = subparsers.add_parser(
        'alter-trends',
        help='command for changing data types and aggregation types for trends from trend stores'  # noqa: disable=E501
    )

    cmd.add_argument(
        '--force', action='store_true',
        help='change datatype even if the new datatype is less powerful than the old one'  # noqa: disable=E501

    )

    cmd.add_argument(
        '--format', choices=['yaml', 'json'], default='yaml',
        help='format of definition'
    )

    cmd.add_argument(
        'definition', type=argparse.FileType('r'),
        help='file containing trend store definition'
    )

    cmd.set_defaults(cmd=alter_trends_cmd)


def alter_trends_cmd(args):
    trend_store_config = load_definition(args.definition, args.format)

    try:
        result = alter_tables_in_trend_store_from_json(
            trend_store_config, force=args.force
        )

        if result:
            sys.stdout.write("Changed columns: {}\n".format(", ".join(result)))
        else:
            sys.stdout.write("No columns were changed.\n")
    except Exception as exc:
        sys.stdout.write("Error:\n{}".format(str(exc)))
        raise exc


def setup_change_trends_parser(subparsers):
    cmd = subparsers.add_parser(
        'change', help='change the content of a trendstore to a predefined type'  # noqa: disable=E501
    )

    cmd.add_argument(
        '--force', action='store_true',
        help='change datatype even if the new datatype is less powerful than the old one'  # noqa: disable=E501
    )

    cmd.add_argument(
        '--format', choices=['yaml', 'json'], default='yaml',
        help='format of definition'
    )

    cmd.add_argument(
        'definition', type=argparse.FileType('r'),
        help='file containing trend store definition'
    )

    cmd.set_defaults(cmd=change_trends_cmd)


def change_trends_cmd(args):
    trend_store = load_trend_store(args.definition, args.format)

    try:
        result = change_trend_store(trend_store, force=args.force)

        if result:
            text = result[0]
            for part in result[1:]:
                if text.endswith(':'):
                    text = text + ' ' + part
                elif part.endswith(':'):
                    sys.stdout.write(text + '\n')
                    text = part
                else:
                    text = text + ', ' + part
            sys.stdout.write(text + '\n')
        else:
            sys.stdout.write('No changes were made.')
    except Exception as exc:
        sys.stdout.write("Error:\n{}".format(str(exc)))
        raise exc


def add_parts_cmd(args):
    trend_store = load_trend_store(args.definition, args.format)

    sys.stdout.write(
        "Adding trend store parts to trend store '{}' - '{}' - '{}' ... \n".format(  # noqa: disable=E501
            trend_store.data_source,
            trend_store.entity_type,
            trend_store.granularity
        )
    )

    parts_added = 0

    try:
        for added_part_name in add_parts_to_trend_store(trend_store):
            sys.stdout.write(f' - added {added_part_name}\n')
            parts_added += 1
    except Exception as exc:
        sys.stdout.write("Error:\n{}".format(str(exc)))
        raise exc

    if parts_added:
        sys.stdout.write(f"Done: Added {parts_added} parts\n")
    else:
        sys.stdout.write("Done: Nothing to add\n")


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


def create_trend_store(trend_store_definition: TrendStore):
    query = (
        'SELECT trend_directory.create_trend_store('
        '%s::text, %s::text, %s::interval, %s::interval, '
        '%s::trend_directory.trend_store_part_descr[]'
        ')'
    )
    query_args = (
        trend_store_definition.data_source, trend_store_definition.entity_type,
        trend_store_definition.granularity, trend_store_definition.partition_size,
        trend_store_definition.parts
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            try:
                cursor.execute(query, query_args)
            except psycopg2.errors.UniqueViolation:
                raise DuplicateTrendStore(
                    trend_store_definition.data_source, trend_store_definition.entity_type,
                    trend_store_definition.granularity
                )

        conn.commit()


def add_trends_to_trend_store(trend_store_definition: TrendStore):
    query = (
        'SELECT trend_directory.add_trends('
        'trend_directory.get_trend_store('
        '%s::text, %s::text, %s::interval'
        '), %s::trend_directory.trend_store_part_descr[]'
        ')'
    )

    query_args = (
        trend_store_definition.data_source,
        trend_store_definition.entity_type,
        trend_store_definition.granularity,
        trend_store_definition.parts
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)
            result = cursor.fetchone()

        conn.commit()

    return ', '.join(str(r) for r in result[0])


def remove_trends_from_trend_store(trend_store_definition: TrendStore):
    query = (
        'SELECT trend_directory.remove_extra_trends('
        'trend_directory.get_trend_store('
        '%s::text, %s::text, %s::interval'
        '), %s::trend_directory.trend_store_part_descr[]'
        ')'
    )

    query_args = (
        trend_store_definition.data_source,
        trend_store_definition.entity_type,
        trend_store_definition.granularity,
        trend_store_definition.parts
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)
            result = cursor.fetchone()

        conn.commit()

    if result[0]:
        return ', '.join(str(r) for r in result[0])
    else:
        return None


def alter_tables_in_trend_store_from_json(trend_store_definition: TrendStore, force=False):
    query = (
        'SELECT trend_directory.{}('
        'trend_directory.get_trend_store('
        '%s::text, %s::text, %s::interval'
        '), %s::trend_directory.trend_store_part_descr[]'
        ')'.format(
            'change_all_trend_data'
            if force else 'change_trend_data_upward'
         )
    )

    query_args = (
        trend_store_definition.data_source,
        trend_store_definition.entity_type,
        trend_store_definition.granularity,
        trend_store_definition.parts
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)
            result = cursor.fetchone()

        conn.commit()

    if result and result[0]:
        return result[0]
    else:
        return None


def change_trend_store(trend_store: TrendStore, force=False):
    query = (
        'SELECT trend_directory.{}('
        'trend_directory.get_trend_store('
        '%s::text, %s::text, %s::interval'
        '), %s::trend_directory.trend_store_part_descr[]'
        ')'.format(
            'change_trendstore_strong'
            if force else 'change_trendstore_weak'
        )
    )

    query_args = (
        trend_store.data_source, trend_store.entity_type,
        trend_store.granularity, trend_store.parts
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)
            result = cursor.fetchone()

        conn.commit()

    if result and result[0]:
        return result[0]
    else:
        return None


def add_parts_to_trend_store(
        trend_store: TrendStore) -> Generator[str, None, None]:
    query = (
        'select tsp.name '
        'from trend_directory.trend_store ts '
        'join directory.data_source ds on ds.id = ts.data_source_id '
        'join directory.entity_type et on et.id = ts.entity_type_id '
        'join trend_directory.trend_store_part tsp '
        'on tsp.trend_store_id = ts.id '
        'where ds.name = %s and et.name = %s and ts.granularity = %s::interval'
    )

    query_args = (
        trend_store.data_source, trend_store.entity_type,
        trend_store.granularity
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            trend_store_part_names = [name for name, in cursor.fetchall()]

            missing_parts = [
                part
                for part in trend_store.parts
                if part.name not in trend_store_part_names
            ]

            for missing_part in missing_parts:
                add_part_query = (
                    'select trend_directory.initialize_trend_store_part('
                    'trend_directory.define_trend_store_part('
                    'ts.id, %s, %s::trend_directory.trend_descr[],'
                    '%s::trend_directory.generated_trend_descr[]'
                    ')'
                    ') '
                    'from trend_directory.trend_store ts '
                    'join directory.data_source ds on ds.id = ts.data_source_id '
                    'join directory.entity_type et on et.id = ts.entity_type_id '
                    'where ds.name = %s and et.name = %s and ts.granularity = %s::interval'
                )

                add_part_query_args = (
                    missing_part.name, missing_part.trends,
                    missing_part.generated_trends, trend_store.data_source,
                    trend_store.entity_type, trend_store.granularity
                )

                cursor.execute(add_part_query, add_part_query_args)

                yield missing_part.name

        conn.commit()


def setup_delete_parser(subparsers):
    cmd = subparsers.add_parser(
        'delete', help='command for deleting trend stores'
    )

    cmd.add_argument('id', help='id of trend store')

    cmd.set_defaults(cmd=delete_trend_store_cmd)


def delete_trend_store_cmd(args):
    query = (
        'SELECT trend_directory.delete_trend_store(%s)'
    )

    query_args = (
        args.id,
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

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
        'trend_store.id, '
        'entity_type.name AS entity_type, '
        'data_source.name AS data_source, '
        'trend_store.granularity,'
        'trend_store.partition_size, '
        'trend_store.retention_period '
        'FROM trend_directory.trend_store '
        'JOIN directory.entity_type ON entity_type.id = entity_type_id '
        'JOIN directory.data_source ON data_source.id = data_source_id '
        'WHERE trend_store.id = %s'
    )

    query_args = (args.trend_store_id,)

    parts_query = (
        'SELECT '
        'tsp.id, '
        'tsp.name '
        'FROM trend_directory.trend_store_part tsp '
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
                    'WHERE trend_store_part_id = %s'
                )
                part_args = (part_id, )

                cursor.execute(part_query, part_args)

                def show_cmd(line):
                    print("                  {}".format(line))

                show_rows_from_cursor(cursor, show_cmd)


def setup_list_parser(subparsers):
    cmd = subparsers.add_parser(
        'list', help='list trend stores from database'
    )

    cmd.set_defaults(cmd=list_trend_stores_cmd)


def list_trend_stores_cmd(_args):
    query = (
        'SELECT '
        'ts.id as id, '
        'data_source.name as data_source, '
        'entity_type.name as entity_type, '
        'ts.granularity '
        'FROM trend_directory.trend_store ts '
        'JOIN directory.data_source ON data_source.id = ts.data_source_id '
        'JOIN directory.entity_type ON entity_type.id = ts.entity_type_id'
    )

    query_args = []

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            show_rows_from_cursor(cursor)


def setup_list_config_parser(subparsers):
    cmd = subparsers.add_parser(
        'list-config', help='list trend stores from configuration'
    )

    cmd.set_defaults(cmd=list_config_trend_stores_cmd)


def list_config_trend_stores_cmd(args):
    instance = MinervaInstance.load()

    trend_stores = instance.list_trend_stores()

    for trend_store in trend_stores:
        print(trend_store)


def setup_partition_parser(subparsers):
    cmd = subparsers.add_parser(
        'partition', help='manage trend store partitions'
    )

    cmd_subparsers = cmd.add_subparsers()

    create_parser = cmd_subparsers.add_parser(
        'create', help='create partitions for trend store'
    )

    create_parser.add_argument(
        '--trend-store', type=int,
        help='Id of trend store for which to create partitions'
    )

    create_parser.add_argument(
        '--ahead-interval',
        help='period for which to create partitions'
    )

    create_parser.set_defaults(cmd=create_partition_cmd)

    create_for_timestamp_parser = cmd_subparsers.add_parser(
        'create-for-timestamp', help='create partitions for specific timestamp'
    )

    create_for_timestamp_parser.add_argument(
        '--trend-store', type=int,
        help='Id of trend store for which to create partitions'
    )

    create_for_timestamp_parser.add_argument(
        'timestamp', help='timestamp to create partitions for'
    )

    create_for_timestamp_parser.set_defaults(cmd=create_partition_for_timestamp_cmd)

    remove_old_parser = cmd_subparsers.add_parser(
        'remove-old', help='remove old partitions'
    )

    remove_old_parser.add_argument(
        '--pretend', action='store_true',
        default=False, help='do not actually delete partitions'
    )

    remove_old_parser.set_defaults(cmd=remove_old_partitions_cmd)


def remove_old_partitions_cmd(args):
    partition_count_query = (
        'select count(*) from trend_directory.partition'
    )

    old_partitions_query = (
        'select p.id, p.name, p.from, p.to '
        'from trend_directory.partition p '
        'join trend_directory.trend_store_part tsp on tsp.id = p.trend_store_part_id '
        'join trend_directory.trend_store ts on ts.id = tsp.trend_store_id '
        'where p.to < now() - retention_period '
        'order by p.name'
    )

    removed_partitions = 0

    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(partition_count_query)
            total_partitions, = cursor.fetchone()

            cursor.execute(old_partitions_query)

            rows = cursor.fetchall()

            print(f'Found {len(rows)} of {total_partitions} partitions to be removed')

            conn.commit()

            if len(rows) > 0:
                print()

                for partition_id, partition_name, data_from, data_to in rows:
                    if not args.pretend:
                        cursor.execute(f'drop table trend_partition."{partition_name}"')
                        cursor.execute('delete from trend_directory.partition where id = %s', (partition_id,))
                        conn.commit()
                    removed_partitions += 1
                    print(f' - {partition_name} ({data_from} - {data_to})')

                if args.pretend:
                    print(f'\nWould have removed {removed_partitions} of {total_partitions} partitions')
                else:
                    print(f'\nRemoved {removed_partitions} of {total_partitions} partitions')


def create_partition_cmd(args):
    ahead_interval = args.ahead_interval or '1 day'

    with closing(connect()) as conn:
        if args.trend_store is None:
            create_partitions_for_all_trend_stores(conn, ahead_interval)
        else:
            create_partitions_for_one_trend_store(
                conn, args.trend_store, ahead_interval
            )


def create_partitions_for_one_trend_store(conn, trend_store_id, ahead_interval):
    for name, partition_index, i, num in create_partitions_for_trend_store(
        conn, trend_store_id, ahead_interval
    ):
        print(
            '{} - {} ({}/{})'.format(name, partition_index, i, num)
        )

    conn.commit()


def create_partitions_for_all_trend_stores(conn, ahead_interval):
    query = 'SELECT id FROM trend_directory.trend_store'

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)

        rows = cursor.fetchall()

    for trend_store_id, in rows:
        create_partitions_for_one_trend_store(
            conn, trend_store_id, ahead_interval
        )


def create_partition_for_timestamp_cmd(args):
    print(f'Creating partitions for timestamp {args.timestamp}')

    query = 'SELECT id FROM trend_directory.trend_store'

    with closing(connect()) as conn:
        if args.trend_store is None:
            with closing(conn.cursor()) as cursor:
                cursor.execute(query)

                rows = cursor.fetchall()

            for trend_store_id, in rows:
                for name, partition_index, i, num in create_specific_partitions_for_trend_store(conn, trend_store_id, args.timestamp):
                    print(
                        '{} - {} ({}/{})'.format(name, partition_index, i, num)
                    )
        else:
            print('no')


def setup_process_modified_log_parser(subparsers):
    cmd = subparsers.add_parser(
        'process-modified-log',
        help='process modified log into modified state'
    )

    cmd.add_argument(
        '--reset', action='store_true', default=False,
        help='reset modified log processing state to Id 0'
    )

    cmd.set_defaults(cmd=process_modified_log_cmd)


def process_modified_log_cmd(args):
    process_modified_log(args.reset)


def process_modified_log(reset):
    reset_query = (
        "UPDATE trend_directory.modified_log_processing_state "
        "SET last_processed_id = %s "
        "WHERE name = 'current'"
    )

    get_position_query = (
        "SELECT last_processed_id "
        "FROM trend_directory.modified_log_processing_state "
        "WHERE name = 'current'"
    )

    query = "SELECT * FROM trend_directory.process_modified_log()"

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            if reset:
                cursor.execute(reset_query, (0,))

            cursor.execute(get_position_query)

            if cursor.rowcount == 1:
                started_at_id, = cursor.fetchone()
            else:
                started_at_id = 0

            cursor.execute(query)

            last_processed_id, = cursor.fetchone()

        conn.commit()

    timestamp_str = datetime.datetime.now()

    print(
        f"{timestamp_str} Processed modified log {started_at_id} - {last_processed_id}"
    )


def setup_materialize_parser(subparsers):
    cmd = subparsers.add_parser(
        'materialize', help='command for materializing trend data'
    )

    cmd.add_argument(
        '--reset', action='store_true', default=False,
        help='ignore materialization state'
    )

    cmd.set_defaults(cmd=materialize_cmd)


def materialize_cmd(args):
    try:
        materialize_all(args.reset)
    except Exception as exc:
        sys.stdout.write("Error:\n{}".format(str(exc)))
        raise exc


def materialize_all(reset):
    query = (
        "SELECT m.id, m::text, ms.timestamp "
        "FROM trend_directory.materialization_state ms "
        "JOIN trend_directory.materialization m "
        "ON m.id = ms.materialization_id "
    )

    if reset:
        where_clause = (
            "WHERE m.enabled AND ms.timestamp < now()"
        )
    else:
        where_clause = (
            "WHERE ("
            "source_fingerprint != processed_fingerprint OR "
            "processed_fingerprint IS NULL"
            ") AND m.enabled AND ms.timestamp < now()"
        )

    query += where_clause

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()

        conn.commit()

        for materialization_id, name, timestamp in rows:
            try:
                row_count = materialize(conn, materialization_id, timestamp)

                conn.commit()

                print("{} - {}: {} records".format(name, timestamp, row_count))
            except Exception as e:
                conn.rollback()
                print("Error materializing {} ({})".format(
                    name, materialization_id
                ))
                print(str(e))


def materialize(conn, materialization_id, timestamp):
    materialize_query = (
        "SELECT (trend_directory.materialize(m, %s)).row_count "
        "FROM trend_directory.materialization m WHERE id = %s"
    )

    with closing(conn.cursor()) as cursor:
        cursor.execute(materialize_query, (timestamp, materialization_id))
        row_count, = cursor.fetchone()

    return row_count
