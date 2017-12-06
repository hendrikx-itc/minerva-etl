from contextlib import closing

from psycopg2 import connect

from minerva.util.tabulate import render_table


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'entity-type', help='command for administering entity types'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
    setup_delete_parser(cmd_subparsers)
    setup_list_parser(cmd_subparsers)


def setup_create_parser(subparsers):
    cmd = subparsers.add_parser(
        'create', help='command for creating entity types'
    )

    cmd.add_argument('name', help='name of the new entity type')

    cmd.set_defaults(cmd=create_entity_type_cmd)


def setup_delete_parser(subparsers):
    cmd = subparsers.add_parser(
        'delete', help='command for deleting entity types'
    )

    cmd.add_argument('name', help='name of the entity type to delete')

    cmd.set_defaults(cmd=delete_entity_type_cmd)


def setup_list_parser(subparsers):
    cmd = subparsers.add_parser(
        'list', help='command for listing entity types'
    )

    cmd.set_defaults(cmd=list_entity_type_cmd)


def create_entity_type_cmd(args):
    create_entity_type(args.name)


def delete_entity_type_cmd(args):
    delete_entity_type(args.name)


def list_entity_type_cmd(args):
    list_entity_types()


def create_entity_type(name):
    query_args = (name,)

    with closing(connect('')) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(
                'SELECT directory.create_entity_type(%s)',
                query_args
            )

        conn.commit()


def delete_entity_type(name):
    query_args = (name,)

    with closing(connect('')) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(
                'SELECT directory.delete_entity_type(%s)',
                query_args
            )

            rowcount = cursor.rowcount

        conn.commit()

    if rowcount == 1:
        print('successfully deleted entity type {}'.format(name))


def list_entity_types():
    with closing(connect('')) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute('SELECT id, name FROM directory.entity_type')

            rows = cursor.fetchall()

    column_names = ["id", "name"]
    column_align = "<" * len(column_names)
    column_sizes = ["max"] * len(column_names)

    for line in render_table(column_names, column_align, column_sizes, rows):
        print(line)
