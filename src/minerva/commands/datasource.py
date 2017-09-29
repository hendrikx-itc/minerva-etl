from contextlib import closing

from psycopg2 import connect


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'datasource', help='command for administering data sources'
    )

    group = cmd.add_mutually_exclusive_group(required=False)

    group.add_argument(
        '--create', '-c', action='store_true', help='create new data source'
    )

    group.add_argument(
        '--delete', '-d', action='store_true', help='delete data source'
    )

    cmd.add_argument('name', help='name of the data source')

    cmd.set_defaults(cmd=datasource_cmd)


def datasource_cmd(args):
    if args.create:
        create_data_source(args.name)
    elif args.delete:
        delete_data_source(args.name)


def create_data_source(name):
    query_args = (name,)

    with closing(connect('')) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute('SELECT directory.create_data_source(%s)', query_args)

        conn.commit()


def delete_data_source(name):
    query_args = (name,)

    with closing(connect('')) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute('SELECT directory.delete_data_source(%s)', query_args)

            rowcount = cursor.rowcount

        conn.commit()

    if rowcount == 1:
        print('successfully deleted data source {}'.format(name))
