from contextlib import closing
import sys

from minerva.db import connect


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'materialize', help='command for materializing trend data'
    )

    cmd.set_defaults(cmd=materialize_cmd)


def materialize_cmd(args):
    try:
        materialize_all()
    except Exception as exc:
        sys.stdout.write("Error:\n{}".format(str(exc)))
        raise exc


def materialize_all():
    query = (
        "SELECT m.id, m::text, ms.timestamp "
        "FROM trend_directory.materialization_state ms "
        "JOIN trend_directory.materialization m "
        "ON m.id = ms.materialization_id "
        "WHERE ("
        "source_fingerprint != processed_fingerprint OR "
        "processed_fingerprint IS NULL"
        ") AND m.enabled AND ms.timestamp < now()"
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()

        conn.commit()

        for materialization_id, name, timestamp in rows:
            try:
                row_count = materialize(conn, materialization_id, timestamp)

                conn.commit()

                print("{} - {}: {} records".format(name, timestamp, row_count))
            except Exception as e:
                conn.rollback()
                print(str(e))


def materialize(conn, materialization_id, timestamp):
    materialize_query = (
        "SELECT (trend_directory.materialize(m, %s)).row_count "
        "FROM trend_directory.materialization m WHERE id = %s"
    )

    with closing(conn.cursor()) as cursor:
        cursor.execute(materialize_query, (timestamp, materialization_id))
        row_count, = cursor.fetchone()

    return row_count
