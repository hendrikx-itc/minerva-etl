from contextlib import closing

from minerva.util.tabulate import render_table
from minerva.db import connect


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'data-source', help='command for administering data sources'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
    setup_delete_parser(cmd_subparsers)
    setup_list_parser(cmd_subparsers)


def setup_create_parser(subparsers):
    cmd = subparsers.add_parser(
        'create', help='command for creating data sources'
    )

    cmd.add_argument('name', help='name of the new data source')

    cmd.set_defaults(cmd=create_data_source_cmd)


def setup_delete_parser(subparsers):
    cmd = subparsers.add_parser(
        'delete', help='command for deleting data sources'
    )

    cmd.add_argument('name', help='name of the data source to delete')

    cmd.set_defaults(cmd=delete_data_source_cmd)


def setup_list_parser(subparsers):
    cmd = subparsers.add_parser(
        'list', help='command for listing data sources'
    )

    cmd.set_defaults(cmd=list_data_source_cmd)


def create_data_source_cmd(args):
    create_data_source(args.name)


def delete_data_source_cmd(args):
    delete_data_source(args.name)


def list_data_source_cmd(args):
    list_data_sources()


def create_data_source(name):
    query_args = (name,)

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(
                'SELECT directory.create_data_source(%s)',
                query_args
            )

        conn.commit()


def delete_data_source(name):
    query_args = (name,)

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(
                'SELECT directory.delete_data_source(%s)',
                query_args
            )

            rowcount = cursor.rowcount

        conn.commit()

    if rowcount == 1:
        print('deleted data source {}'.format(name))


def list_data_sources():
    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute('SELECT id, name FROM directory.data_source')

            rows = cursor.fetchall()

    column_names = ["id", "name"]
    column_align = "<" * len(column_names)
    column_sizes = ["max"] * len(column_names)

    for line in render_table(column_names, column_align, column_sizes, rows):
        print(line)
