from contextlib import closing

from psycopg2 import sql

from minerva.db import connect


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'relation', help='command for administering relations'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_materialize_parser(cmd_subparsers)


def setup_materialize_parser(subparsers):
    cmd = subparsers.add_parser(
        'materialize', help='materialize relations'
    )

    cmd.set_defaults(cmd=materialize_relations)


def materialize_relations(args):
    query = (
        'SELECT name FROM relation_directory.type'
    )

    with closing(connect()) as conn:
        conn.autocommit = True

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

            names = [name for name, in cursor.fetchall()]

            for name in names:
                materialize_relation_query = sql.SQL(
                    "REFRESH MATERIALIZED VIEW relation.{}"
                ).format(sql.Identifier(name))

                cursor.execute(materialize_relation_query)

                print("Materialized relation '{}'".format(name))
