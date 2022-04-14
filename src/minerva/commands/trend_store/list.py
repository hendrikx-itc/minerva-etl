"""Provides the 'list' sub-command."""
from contextlib import closing

from minerva.commands import show_rows_from_cursor
from minerva.db import connect


def setup_list_parser(subparsers):
    """Setup the argument parser for the 'list' sub-command."""
    cmd = subparsers.add_parser("list", help="list trend stores from database")

    cmd.set_defaults(cmd=list_trend_stores_cmd)


def list_trend_stores_cmd(_args):
    query = (
        "SELECT "
        "ts.id as id, "
        "data_source.name as data_source, "
        "entity_type.name as entity_type, "
        "ts.granularity "
        "FROM trend_directory.trend_store ts "
        "JOIN directory.data_source ON data_source.id = ts.data_source_id "
        "JOIN directory.entity_type ON entity_type.id = ts.entity_type_id"
    )

    query_args = []

    with closing(connect()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, query_args)

            show_rows_from_cursor(cursor)
