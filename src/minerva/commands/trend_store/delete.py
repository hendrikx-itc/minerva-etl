"""Provides the 'delete' sub-command."""
from contextlib import closing

from minerva.db import connect


def setup_delete_parser(subparsers):
    """Setup the argument parser for the 'delete' sub-command."""
    cmd = subparsers.add_parser("delete", help="command for deleting trend stores")

    cmd.add_argument("id", help="id of trend store")

    cmd.set_defaults(cmd=delete_trend_store_cmd)


def delete_trend_store_cmd(args):
    query = "SELECT trend_directory.delete_trend_store(%s)"

    query_args = (args.id,)

    with closing(connect()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, query_args)

        conn.commit()
