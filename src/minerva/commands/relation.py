from contextlib import closing
import argparse

from psycopg2 import sql

from minerva.instance import load_yaml
from minerva.db import connect


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'relation', help='command for administering relations'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
    setup_materialize_parser(cmd_subparsers)
    setup_remove_parser(cmd_subparsers)


def setup_create_parser(subparsers):
    cmd = subparsers.add_parser(
        'create', help='create a relation'
    )

    cmd.add_argument(
        'definition', type=argparse.FileType('r'),
        help='file containing relation definition'
    )

    cmd.set_defaults(cmd=create_relation)


class DuplicateRelation(Exception):
    def __str__(self):
        return "Duplicate relation"


def create_relation(args):
    definition = load_yaml(args.definition)
    print(definition)

    try:
        define_relation(definition)
    except DuplicateRelation as exc:
        print(exc)


def define_relation(definition):
    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            try:
                cursor.execute(create_materialized_table_query(definition))
                cursor.execute(distribute_materialized_table_query(definition))
                cursor.execute(create_materialization_function_query(definition))
                cursor.execute(register_type_query(definition))
            except Exception as exc:
                print(exc)
            finally:
                conn.commit()

def create_materialized_table_query(relation):
    return 'CREATE TABLE relation."{}" ("source_id" integer, "target_id" integer)'.format(
        relation['name']
    )

def distribute_materialized_table_query(relation):
    return 'SELECT create_reference_table(\'relation."{}"\')'.format(
        relation['name']
    )

def create_materialization_function_query(relation):
    return ('CREATE FUNCTION relation_directory."materialize_{}"() ' +\
        'RETURNS VOID AS $$ '+\
        'TRUNCATE relation."{}"; ' +\
        'INSERT INTO relation."{}" ({}); ' +\
        '$$ LANGUAGE sql volatile').format(
            relation['name'],
            relation['name'],
            relation['name'],
            relation['query']
            )

#def create_materialized_view_query(relation):
#    return 'CREATE MATERIALIZED VIEW relation."{}" AS\n{}'.format(
#        relation['name'],
#        relation['query']
#    )


def register_type_query(relation):
    return "SELECT relation_directory.register_type('{}');".format(
        relation['name']
    )


def setup_materialize_parser(subparsers):
    cmd = subparsers.add_parser(
        'materialize', help='materialize relations'
    )

    cmd.set_defaults(cmd=materialize_relations_cmd)


def materialize_relations_cmd(args):
    materialize_relations()


def materialize_relations():
    query = (
        'SELECT name FROM relation_directory.type'
    )

    with closing(connect()) as conn:
        conn.autocommit = True

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

            names = [name for name, in cursor.fetchall()]

            for name in names:
                materialize_relation_query = \
                    'SELECT relation_directory."materialize_{}"()'.format(name)

                cursor.execute(materialize_relation_query)

                print("Materialized relation '{}'".format(name))


def setup_remove_parser(subparsers):
    cmd = subparsers.add_parser(
        'remove', help='remove a relation'
    )

    cmd.add_argument(
        'name', help='name of the relation to be removed'
    )

    cmd.set_defaults(cmd=remove_relation)


def remove_relation(args):
    name = args.name
    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(
                "SELECT relation_directory.remove('{}');".format(name)
            )
            result = cursor.fetchone()[0]
            if result:
                print("Removed relation '{}'".format(result))
            else:
                print("No relation to remove")

        conn.commit()
