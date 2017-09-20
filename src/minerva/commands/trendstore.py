from contextlib import closing

from psycopg2 import connect


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'trendstore', help='command for administering trend stores'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
    setup_delete_parser(cmd_subparsers)
    setup_add_trend_parser(cmd_subparsers)


def setup_create_parser(subparsers):
    cmd = subparsers.add_parser(
        'create', help='command for creating trend stores'
    )

    cmd.add_argument('--datasource', default='default', help='name of the data source of the new trend store')

    cmd.add_argument('--entitytype', default='unknown', help='name of the entity type of the new trend store')

    cmd.add_argument('--granularity', default='1 day', help='granularity of the new trend store')

    cmd.add_argument('--partition-size', default=86400, help='partition size of the new trend store')

    cmd.add_argument('name', help='name of the new trend store')

    cmd.set_defaults(cmd=create_trendstore_cmd)


def create_trendstore_cmd(args):
    query = (
        'SELECT trend_directory.create_table_trend_store(%s::name, %s::text, %s::text, %s::interval, %s::integer, '
        '%s::trend_directory.trend_descr[])'
    )

    trend_descriptors = []

    query_args = (
        args.name, args.datasource, args.entitytype, args.granularity, args.partition_size, trend_descriptors
    )

    with closing(connect('')) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

        conn.commit()


def setup_delete_parser(subparsers):
    cmd = subparsers.add_parser(
        'delete', help='command for deleting trend stores'
    )

    cmd.add_argument('name', help='name of the new trend store')

    cmd.set_defaults(cmd=delete_trendstore_cmd)


def delete_trendstore_cmd(args):
    query = (
        'SELECT trend_directory.delete_table_trend_store(%s::name)'
    )

    query_args = (
        args.name,
    )

    with closing(connect('')) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

        conn.commit()


def setup_add_trend_parser(subparsers):
    cmd = subparsers.add_parser(
        'add-trend', help='add a trend to a trend store'
    )

    cmd.add_argument('--data-type')

    cmd.add_argument('--trend-name')

    cmd.add_argument('trendstore', help='name of the trend store where the trend will be added')

    cmd.set_defaults(cmd=add_trend_to_trendstore_cmd)


def add_trend_to_trendstore_cmd(args):
    query = (
        'SELECT trend_directory.add_trend_to_trend_store(table_trend_store, %s::name, %s::text, %s::text) '
        'FROM trend_directory.table_trend_store WHERE name = %s'
    )

    query_args = (
        args.trend_name, args.data_type, '', args.trendstore
    )

    with closing(connect('')) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            print(cursor.fetchone())

        conn.commit()
