from contextlib import closing

from psycopg2 import sql

from minerva.db import connect


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'virtual-entity', help='command for administering virtual entities'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_materialize_parser(cmd_subparsers)


def setup_materialize_parser(subparsers):
    cmd = subparsers.add_parser(
        'materialize', help='command for materializing virtual entities'
    )

    cmd.set_defaults(cmd=create_materialize_cmd)


def create_materialize_cmd(args):
    materialize_virtual_entities()


def materialize_virtual_entities():
    get_view_names_query = (
        "SELECT relname "
        "FROM pg_class "
        "JOIN pg_roles r ON r.oid = pg_class.relowner "
        "JOIN pg_namespace ns ON ns.oid = pg_class.relnamespace "
        "WHERE nspname='virtual_entity' AND relkind = 'v';"
    )

    with closing(connect()) as conn:
        conn.autocommit = True

        with closing(conn.cursor()) as cursor:
            cursor.execute(get_view_names_query)

            names = [name for name, in cursor.fetchall()]

            for name in names:
                materialize_virtual_entity_query = sql.SQL(
                    "INSERT INTO entity.{} (name) "
                    "SELECT name FROM virtual_entity.{} ON CONFLICT DO NOTHING"
                ).format(sql.Identifier(name), sql.Identifier(name))

                cursor.execute(materialize_virtual_entity_query)

                print(
                    "Materialized virtual entity type '{}'".format(name)
                )
