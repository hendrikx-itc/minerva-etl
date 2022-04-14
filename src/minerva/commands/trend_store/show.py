"""Provides the 'show' sub-command."""
from contextlib import closing

from minerva.commands import show_rows_from_cursor
from minerva.db import connect


def setup_show_parser(subparsers):
    cmd = subparsers.add_parser("show", help="show information on a trend store")

    cmd.add_argument("trend_store_id", help="id of trend store", type=int)

    cmd.set_defaults(cmd=show_trend_store_cmd)


def show_trend_store_cmd(args):
    query = (
        "SELECT "
        "trend_store.id, "
        "entity_type.name AS entity_type, "
        "data_source.name AS data_source, "
        "trend_store.granularity,"
        "trend_store.partition_size, "
        "trend_store.retention_period "
        "FROM trend_directory.trend_store "
        "JOIN directory.entity_type ON entity_type.id = entity_type_id "
        "JOIN directory.data_source ON data_source.id = data_source_id "
        "WHERE trend_store.id = %s"
    )

    query_args = (args.trend_store_id,)

    parts_query = (
        "SELECT "
        "tsp.id, "
        "tsp.name "
        "FROM trend_directory.trend_store_part tsp "
        "WHERE tsp.trend_store_id = %s"
    )

    parts_query_args = (args.trend_store_id,)

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            (
                id_,
                entity_type,
                data_source,
                granularity,
                partition_size,
                retention_period,
            ) = cursor.fetchone()

            print("Table Trend Store")
            print("")
            print(f"id:               {id_}")
            print(f"entity_type:      {entity_type}")
            print(f"data_source:      {data_source}")
            print(f"granularity:      {granularity}")
            print(f"partition_size:   {partition_size}")
            print(f"retention_period: {retention_period}")
            print("parts:")

            cursor.execute(parts_query, parts_query_args)

            rows = cursor.fetchall()

            for part_id, part_name in rows:
                header = f"{part_name} ({part_id})"
                print(f"                  {header}")
                print("                  {}".format("=" * len(header)))

                part_query = (
                    "SELECT id, name, data_type "
                    "FROM trend_directory.table_trend "
                    "WHERE trend_store_part_id = %s"
                )
                part_args = (part_id,)

                cursor.execute(part_query, part_args)

                def show_cmd(line):
                    print(f"                  {line}")

                show_rows_from_cursor(cursor, show_cmd)
