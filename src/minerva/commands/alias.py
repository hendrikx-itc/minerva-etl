from contextlib import closing

from minerva.db import connect
from minerva.db.error import translate_postgresql_exceptions, UniqueViolation
from minerva.commands import show_rows_from_cursor


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'alias', help='command for administering aliases'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
    setup_delete_parser(cmd_subparsers)
    setup_list_parser(cmd_subparsers)


def setup_create_parser(subparsers):
    cmd = subparsers.add_parser(
        'create', help='command for creating trend stores'
    )

    cmd.add_argument(
        'name', help='name of the new alias'
    )

    cmd.set_defaults(cmd=create_alias_cmd)


def create_alias_cmd(args):
    try:
        translate_postgresql_exceptions(create_alias)(args.name)
    except UniqueViolation:
        print("an alias with the name '{}' already exists".format(args.name))
        return 1


def create_alias(name):
    query = (
        'SELECT * FROM alias_directory.create_alias_type(%s::name)'
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, (name,))

            conn.commit()

            show_rows_from_cursor(cursor)


def setup_delete_parser(subparsers):
    cmd = subparsers.add_parser(
        'delete', help='command for deleting aliases'
    )

    cmd.add_argument('name', help='name of alias')

    cmd.set_defaults(cmd=delete_alias_cmd)


def delete_alias_cmd(args):
    query = (
        'SELECT (alias_directory.delete_alias_type(alias_type)).* '
        'FROM alias_directory.alias_type '
        'WHERE name = %s'
    )

    query_args = (
        args.name,
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            conn.commit()

            if cursor.rowcount == 0:
                print("no aliases found matching name '{}'".format(args.name))
            else:
                show_rows_from_cursor(cursor)


def setup_list_parser(subparsers):
    cmd = subparsers.add_parser(
        'list', help='list aliases'
    )

    cmd.set_defaults(cmd=list_aliases_cmd)


def list_aliases_cmd(_args):
    query = (
        'SELECT id, name '
        'FROM alias_directory.alias_type'
    )

    query_args = []

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            show_rows_from_cursor(cursor)
