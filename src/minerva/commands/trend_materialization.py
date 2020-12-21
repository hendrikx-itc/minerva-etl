from contextlib import closing
import argparse

from minerva.commands import show_rows_from_cursor
from minerva.db import connect
from minerva.db.error import DuplicateTable
from minerva.instance import load_yaml
from minerva.storage.trend.materialization import from_config, Materialization
import psycopg2


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'trend-materialization', help='command for administering trend materializations'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
    setup_update_parser(cmd_subparsers)
    setup_drop_parser(cmd_subparsers)
    setup_list_parser(cmd_subparsers)


def setup_create_parser(subparsers):
    cmd = subparsers.add_parser(
        'create', help='create a materialization'
    )

    cmd.add_argument(
        'definition', type=argparse.FileType('r'),
        help='file containing materialization definition'
    )

    cmd.set_defaults(cmd=create_materialization)


def setup_update_parser(subparsers):
    cmd = subparsers.add_parser(
        'update', help='update a materialization'
    )

    cmd.add_argument(
        'definition', type=argparse.FileType('r'),
        help='file containing materialization definition'
    )

    cmd.set_defaults(cmd=update_materialization)


def setup_drop_parser(subparsers):
    cmd = subparsers.add_parser(
        'drop', help='drop a materialization'
    )

    cmd.add_argument(
        'name', help='name of materialization (target trend store part)'
    )

    cmd.set_defaults(cmd=drop_materialization)


def setup_list_parser(subparsers):
    cmd = subparsers.add_parser(
        'list', help='list materializations'
    )

    cmd.set_defaults(cmd=list_materializations)


def create_materialization(args):
    definition = load_yaml(args.definition)

    define_materialization(definition)


def define_materialization(definition):
    materialization = from_config(definition)

    with closing(connect()) as conn:
        try:
            materialization.create(conn)
            conn.commit()

            print(f"Created materialization '{definition['target_trend_store_part']}'")
        except DuplicateTable as e:
            print(f"Error creating materialization: {e}")

def update_materialization(args):
    definition = load_yaml(args.definition)

    materialization = from_config(definition)

    with closing(connect()) as conn:
        materialization.update(conn)

        conn.commit()

    print(f"Updated materialization '{definition['target_trend_store_part']}'")


def drop_materialization(args):
    materialization = Materialization(args.name)

    with closing(connect()) as conn:
        count = materialization.drop(conn)

        conn.commit()

    if count > 0:
        print(f"Drop materialization '{args.name}'")
    else:    
        print(f"No materialization matched name '{args.name}'")


def list_materializations(args):
    with closing(connect()) as conn:
        query = (
            "SELECT id, m::text AS name FROM trend_directory.materialization m ORDER BY m::text"
        )

        with conn.cursor() as cursor:
            cursor.execute(query)

            show_rows_from_cursor(cursor)

        conn.commit()
