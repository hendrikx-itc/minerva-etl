"""
Provides the 'report' sub-command for reporting on metrics of trend stores,
attribute stores, etc.
"""
from typing import Generator

from psycopg2 import sql

from minerva.db import connect
from minerva.util.tabulate import render_table


def setup_command_parser(subparsers):
    """
    Setup the 'report' sub-command in the subparsers of the parent command.
    """
    cmd = subparsers.add_parser(
        'report',
        help='command for generating Minerva instance report with metrics'
    )

    cmd.set_defaults(cmd=report_cmd)


def report_cmd(_args):
    print('Minerva Instance Report')
    print()

    with connect() as conn:
        conn.autocommit = True
        lines = generate_report(conn)

    for line in lines:
        print(line)


def generate_report(conn) -> Generator[str, None, None]:
    yield from generate_trend_report(conn)
    yield from generate_attribute_report(conn)


def generate_trend_report(conn) -> Generator[str, None, None]:
    query = (
        'SELECT '
        'ds.name AS data_source_name, '
        'et.name AS entity_type_name, '
        'tsp.name '
        'FROM trend_directory.trend_store ts '
        'JOIN trend_directory.trend_store_part tsp '
        'ON ts.id = tsp.trend_store_id '
        'JOIN directory.data_source ds ON ds.id = ts.data_source_id '
        'JOIn directory.entity_type et ON et.id = ts.entity_type_id '
        'ORDER BY ts.id, tsp.name'
    )

    with conn.cursor() as cursor:
        cursor.execute(query)

        rows = cursor.fetchall()

        table_rows = [
            (
                data_source,
                entity_type,
                name,
                get_trend_store_part_statistics(conn, name)
            )
            for data_source, entity_type, name in rows
        ]

        column_names = [
            'Data Source', 'Entity Type', 'Part Name', 'Record Count'
        ]

        column_align = ['<', '<', '<', '>']
        column_sizes = ["max"] * len(column_names)

        yield from render_table(
            column_names, column_align, column_sizes, table_rows
        )


def get_trend_store_part_statistics(conn, trend_store_part_name: str):
    query = sql.SQL(
        'SELECT count(*) FROM {}'
    ).format(
        sql.Identifier('trend', trend_store_part_name)
    )

    with conn.cursor() as cursor:
        cursor.execute(query)

        row_count, = cursor.fetchone()

        return row_count


def generate_attribute_report(conn) -> Generator[str, None, None]:
    query = (
        'SELECT attribute_store::text AS name '
        'FROM attribute_directory.attribute_store ORDER BY name'
    )

    with conn.cursor() as cursor:
        cursor.execute(query)

        rows = cursor.fetchall()

        table_rows = [
            (
                name,
                get_attribute_store_part_statistics(conn, name)
            )
            for name, in rows
        ]

        column_names = ['Name', 'Record Count']

        column_align = ['<', '>']
        column_sizes = ["max"] * len(column_names)

        yield from render_table(
            column_names, column_align, column_sizes, table_rows
        )


def get_attribute_store_part_statistics(conn, attribute_store_part_name: str):
    query = sql.SQL(
        'SELECT count(*) FROM {}'
    ).format(
        sql.Identifier('attribute_history', attribute_store_part_name)
    )

    with conn.cursor() as cursor:
        cursor.execute(query)

        row_count, = cursor.fetchone()

        return row_count
