import sys
import argparse
from contextlib import closing

from minerva.db import connect


def main():
    parser = argparse.ArgumentParser(
        description='Process modified_log entries'
    )

    args = parser.parse_args()

    with closing(connect()) as conn:
        process_loop(conn, 1)

    return 0


def process_loop(conn, start_id):
    query = (
        "SELECT max(id), trend_directory.update_modified(table_trend_store_part_id, timestamp, max(modified)) "
        "FROM trend_directory.modified_log "
        "WHERE id > %s "
        "GROUP BY table_trend_store_part_id, timestamp"
    )

    query_args = (start_id,)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, query_args)

        max_id, _ = cursor.fetchone()

        print(max_id)

    conn.commit()


if __name__ == '__main__':
    sys.exit(main())
