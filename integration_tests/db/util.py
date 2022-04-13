from contextlib import closing

from psycopg2 import sql

from minerva.db.util import create_temp_table_from
from minerva.db.query import Table


def test_create_temp_table_from(start_db_container):
    conn = start_db_container

    table = Table("trend_partition", "storage_tmp_test_table")

    query = sql.SQL(
        "CREATE TABLE {}("
        "   entity_id integer,"
        "   timestamp timestamp with time zone,"
        "   modified timestamp with time zone,"
        "   x integer,"
        "   y double precision"
        ")"
    ).format(table.identifier())

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)

        tmp_table = create_temp_table_from(cursor, table)

    assert tmp_table.name == f"tmp_{table.name}"
