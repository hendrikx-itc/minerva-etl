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
        process_loop(conn, 0)

    return 0


def process_loop(conn, start_id):
    query = "SELECT trend_directory.process_modified_log(%s)"

    query_args = (start_id,)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, query_args)

        max_id, = cursor.fetchone()

        print(max_id)

    conn.commit()


if __name__ == '__main__':
    sys.exit(main())
