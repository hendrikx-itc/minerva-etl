"""
Provides the 'report' sub-command for reporting on metrics of trend stores,
attribute stores, etc.
"""
from typing import Generator, List, Tuple

from psycopg2 import sql

from minerva.db import connect
from minerva.util.tabulate import render_table, render_rst_table


class Formatter:
    def format_header(self, title, level: int):
        raise NotImplementedError()

    def format_table(self, column_names, column_align, column_sizes, table_rows):
        raise NotImplementedError()


class PlainFormatter(Formatter):
    def format_header(self, title, level: int):
        yield title

    def format_table(self, column_names, column_align, column_sizes, table_rows):
        yield from render_table(
            column_names, column_align, column_sizes, table_rows
        )


class RstFormatter(Formatter):
    header_chars = ['=', '-', '~', '.']

    def format_header(self, title, level: int):
        yield title
        yield len(title) * RstFormatter.header_chars[level - 1]

    def format_table(self, column_names, column_align, column_sizes, table_rows):
        yield from render_rst_table(
            column_names, column_align, column_sizes, table_rows
        )


formatters = {
    'plain': PlainFormatter(),
    'rst': RstFormatter()
}


def setup_command_parser(subparsers):
    """
    Setup the 'report' sub-command in the subparsers of the parent command.
    """
    cmd = subparsers.add_parser(
        'report',
        help='command for generating Minerva instance report with metrics'
    )

    format_choices = list(formatters.keys())
    format_default = 'plain'

    cmd.add_argument(
        '--format', choices=format_choices, default=format_default,
        help=f'Set output format (default: {format_default})'
    )

    cmd.set_defaults(cmd=report_cmd)


def report_cmd(args):
    formatter = formatters[args.format]

    for line in formatter.format_header('Minerva Instance Report', 1):
        print(line)

    print()

    with connect() as conn:
        conn.autocommit = True
        lines = generate_report(conn, formatter)

    for line in lines:
        print(line)


def generate_report(conn, formatter: Formatter) -> Generator[str, None, None]:
    for line in formatter.format_header('Trend Store Metrics', 2):
        print(line)

    print()

    yield from generate_trend_report(conn, formatter)

    print()

    for line in formatter.format_header('Attribute Store Metrics', 2):
        print(line)

    print()

    yield from generate_attribute_report(conn, formatter)

    print()


def generate_trend_report(conn, formatter: Formatter) -> Generator[str, None, None]:
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

        yield from formatter.format_table(
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


def generate_attribute_report(conn, formatter: Formatter) -> Generator[str, None, None]:
    table_rows = [
        get_attribute_store_part_statistics(conn, name)
        for name in get_attribute_store_names(conn)
    ]

    column_names = ['Name', 'Record Count', 'Unique Entity Count', 'Max Timestamp']

    column_align = ['<', '>', '>', '<']
    column_sizes = ["max"] * len(column_names)

    yield from formatter.format_table(
        column_names, column_align, column_sizes, table_rows
    )


def get_attribute_store_names(conn) -> List[str]:
    query = (
        'SELECT attribute_store::text AS name '
        'FROM attribute_directory.attribute_store ORDER BY name'
    )

    with conn.cursor() as cursor:
        cursor.execute(query)

        return [name for name, in cursor.fetchall()]


def get_attribute_store_part_statistics(conn, attribute_store_name: str) -> Tuple[str, int, int, str]:
    attribute_history_identifier = sql.Identifier(
        'attribute_history', attribute_store_name
    )

    record_count_query = sql.SQL(
        'SELECT count(*) FROM {}'
    ).format(attribute_history_identifier)

    unique_entity_count_query = sql.SQL(
        'SELECT count(distinct entity_id) FROM {}'
    ).format(attribute_history_identifier)

    most_recent_timestamp_query = sql.SQL(
        'SELECT max(timestamp) FROM {}'
    ).format(attribute_history_identifier)

    with conn.cursor() as cursor:
        cursor.execute(record_count_query)

        record_count, = cursor.fetchone()

        cursor.execute(unique_entity_count_query)

        unique_entity_count, = cursor.fetchone()

        cursor.execute(most_recent_timestamp_query)

        most_recent_timestamp, = cursor.fetchone()

        if most_recent_timestamp is None:
            most_recent_timestamp_str = ''
        else:
            most_recent_timestamp_str = str(most_recent_timestamp)

    return attribute_store_name, record_count, unique_entity_count, most_recent_timestamp_str
