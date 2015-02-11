from contextlib import closing

from minerva.db.util import create_temp_table_from
from minerva.db.query import Table
from minerva.test import with_conn


@with_conn()
def test_create_temp_table_from(conn):
    table = Table("trend_partition", "storage_tmp_test_table")

    with closing(conn.cursor()) as cursor:
        cursor.execute(
            'CREATE TABLE {}('
            '   entity_id integer,'
            '   timestamp timestamp with time zone,'
            '   modified timestamp with time zone,'
            '   x integer,'
            '   y double precision'
            ')'.format(table.render())
        )

        tmp_table = create_temp_table_from(cursor, table)

    assert tmp_table.name == 'tmp_{}'.format(table.name)