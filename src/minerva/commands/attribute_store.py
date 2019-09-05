import json
from contextlib import closing
import argparse

import yaml

from minerva.db import connect
from minerva.util.tabulate import render_table


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'attribute-store', help='command for administering attribute stores'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_create_parser(cmd_subparsers)
    setup_delete_parser(cmd_subparsers)
    setup_add_attribute_parser(cmd_subparsers)
    setup_remove_attribute_parser(cmd_subparsers)
    setup_show_parser(cmd_subparsers)
    setup_list_parser(cmd_subparsers)


def setup_create_parser(subparsers):
    cmd = subparsers.add_parser(
        'create', help='command for creating attribute stores'
    )

    cmd.add_argument(
        '--data-source',
        help='name of the data source of the new attribute store'
    )

    cmd.add_argument(
        '--entity-type',
        help='name of the entity type of the new attribute store'
    )

    cmd.add_argument(
        '--from-json', type=argparse.FileType('r'),
        help='use json description for attribute store'
    )

    cmd.add_argument(
        '--from-yaml', type=argparse.FileType('r'),
        help='use yaml description for attribute store'
    )

    cmd.set_defaults(cmd=create_attribute_store_cmd)


def create_attribute_store_cmd(args):
    if args.from_json:
        attribute_store_config = json.load(args.from_json)
    elif args.from_yaml:
        attribute_store_config = yaml.load(args.from_yaml)
    else:
        attribute_store_config = {
            'data_source': 'example_source',
            'entity_type': 'example_type',
            'attributes': []
        }

    if args.data_source:
        attribute_store_config['data_source'] = args.data_source

    if args.entity_type:
        attribute_store_config['entity_type'] = args.entity_type

    create_attribute_store_from_json(attribute_store_config)


def create_attribute_store_from_json(data):
    query = (
        'SELECT attribute_directory.create_attribute_store('
        '%s::text, %s::text, {}'
        ')'
    ).format(
        'ARRAY[{}]::attribute_directory.attribute_descr[]'.format(','.join([
            "('{}', '{}', '{}')".format(
                attribute['name'],
                attribute['data_type'],
                attribute.get('description', '')
            )
            for attribute in data['attributes']
        ]))
    )

    query_args = (
        data['data_source'], data['entity_type']
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

        conn.commit()


def setup_delete_parser(subparsers):
    cmd = subparsers.add_parser(
        'delete', help='command for deleting attribute stores'
    )

    cmd.add_argument('name', help='name of attribute store')

    cmd.set_defaults(cmd=delete_attribute_store_cmd)


def delete_attribute_store_cmd(args):
    query = (
        'SELECT attribute_directory.delete_attribute_store(%s::name)'
    )

    query_args = (
        args.name,
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

        conn.commit()


def setup_add_attribute_parser(subparsers):
    cmd = subparsers.add_parser(
        'add-attribute', help='add an attribute to an attribute store'
    )

    cmd.add_argument('-t', '--data-type')

    cmd.add_argument('-n', '--attribute-name')

    cmd.add_argument('-d', '--description')

    cmd.add_argument(
        'attribute_store',
        help='name of the attribute store where the attribute will be added'
    )

    cmd.set_defaults(cmd=add_attribute_to_attribute_store_cmd)


def add_attribute_to_attribute_store_cmd(args):
    query = (
        'SELECT attribute_directory.create_attribute('
        'attribute_store, %s::name, %s::text, %s::text'
        ') '
        'FROM attribute_directory.attribute_store WHERE attribute_store::text = %s'
    )

    query_args = (
        args.attribute_name, args.data_type, args.description, args.attribute_store
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            print(cursor.fetchone())

        conn.commit()


def setup_remove_attribute_parser(subparsers):
    cmd = subparsers.add_parser(
        'remove-attribute', help='add an attribute to an attribute store'
    )

    cmd.add_argument(
        'attribute_store',
        help='attribute store from where the attribute should be removed'
    )

    cmd.add_argument(
        'attribute_name',
        help='name of the attribute that should be removed'
    )

    cmd.set_defaults(cmd=remove_attribute_from_attribute_store_cmd)


def remove_attribute_from_attribute_store_cmd(args):
    query = (
        'SELECT attribute_directory.drop_attribute('
        'attribute_store, %s::name'
        ') '
        'FROM attribute_directory.attribute_store '
        'WHERE attribute_store::text = %s'
    )

    query_args = (
        args.attribute_name, args.attribute_store
    )

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            print("Attribute removed")

        conn.commit()


def setup_show_parser(subparsers):
    cmd = subparsers.add_parser(
        'show', help='show information on attribute stores'
    )

    cmd.add_argument('attribute_store', help='Attribute store to show')

    cmd.add_argument(
        '--id', help='id of trend store', type=int
    )

    cmd.set_defaults(cmd=show_attribute_store_cmd)


def show_rows(column_names, rows):
    column_align = "<" * len(column_names)
    column_sizes = ["max"] * len(column_names)

    for line in render_table(column_names, column_align, column_sizes, rows):
        print(line)


def show_rows_from_cursor(cursor):
    show_rows([c.name for c in cursor.description], cursor.fetchall())


def show_attribute_store_cmd(args):
    query = (
        'SELECT '
        'atts.id, '
        'entity_type.name AS entity_type, '
        'data_source.name AS data_source '
        'FROM attribute_directory.attribute_store atts '
        'JOIN directory.entity_type ON entity_type.id = entity_type_id '
        'JOIN directory.data_source ON data_source.id = data_source_id'
    )

    query_args = []

    if args.id:
        query += ' WHERE atts.id = %s'
        query_args.append(args.id)
    else:
        query += ' WHERE atts::text = %s'
        query_args.append(args.attribute_store)

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            show_rows_from_cursor(cursor)


def setup_list_parser(subparsers):
    cmd = subparsers.add_parser(
        'list', help='list attribute stores'
    )

    cmd.set_defaults(cmd=list_attribute_stores_cmd)


def list_attribute_stores_cmd(args):
    query = (
        'SELECT '
        'atts::text as attribute_store, '
        'data_source.name as data_source, '
        'entity_type.name as entity_type '
        'FROM attribute_directory.attribute_store atts '
        'JOIN directory.data_source ON data_source.id = atts.data_source_id '
        'JOIN directory.entity_type ON entity_type.id = atts.entity_type_id'
    )

    query_args = []

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            show_rows_from_cursor(cursor)
