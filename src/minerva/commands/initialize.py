import os
from contextlib import closing
import sys
import glob

import yaml

from minerva.db import connect

from minerva.commands.attribute_store import create_attribute_store_from_json
from minerva.commands.trend_store import create_trend_store_from_json


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'initialize', help='command for complete initialization of Minerva instance'
    )

    cmd.add_argument(
        '-i', '--instance-root',
        help='root directory of the instance definition'
    )

    cmd.set_defaults(cmd=initialize_cmd)


def initialize_cmd(args):
    instance_root = args.instance_root or os.environ.get('INSTANCE_ROOT')

    if instance_root is None:
        raise Exception("INSTANCE_ROOT environment variable not set and --instance-root option not specified")

    sys.stdout.write(
        "Initializing Minerva instance from '{}'\n".format(
            instance_root
        )
    )

    try:
        initialize_instance(instance_root)
    except Exception as exc:
        sys.stdout.write("Error:\n{}".format(str(exc)))
        raise exc


def initialize_instance(instance_root):
    initialize_attribute_stores(instance_root)
    initialize_trend_stores(instance_root)
    define_virtual_entities(instance_root)
    define_relations(instance_root)
    define_materializations(instance_root)

    create_partitions()


def initialize_attribute_stores(instance_root):
    definition_files = glob.glob(os.path.join(instance_root, 'attribute/*.yaml'))

    for definition_file_path in definition_files:
        print(definition_file_path)

        with open(definition_file_path) as definition_file:
            definition = yaml.load(definition_file)

        create_attribute_store_from_json(definition)


def initialize_trend_stores(instance_root):
    definition_files = glob.glob(os.path.join(instance_root, 'trend/*.json'))

    for definition_file_path in definition_files:
        print(definition_file_path)

        with open(definition_file_path) as definition_file:
            definition = yaml.load(definition_file)

        create_trend_store_from_json(definition)


def define_virtual_entities(instance_root):
    definition_files = glob.glob(os.path.join(instance_root, 'virtual-entity/*.sql'))

    for definition_file_path in definition_files:
        print(definition_file_path)

        with open(definition_file_path) as definition_file:
            sql = definition_file.read()

        with closing(connect()) as conn:
            conn.autocommit = True

            with closing(conn.cursor()) as cursor:
                cursor.execute(sql)


def define_relations(instance_root):
    definition_files = glob.glob(os.path.join(instance_root, 'relation/*.yaml'))

    for definition_file_path in definition_files:
        print(definition_file_path)

        with open(definition_file_path) as definition_file:
            definition = yaml.load(definition_file)

        define_relation(definition)


def define_relation(definition):
    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(create_materialized_view_query(definition))
            cursor.execute(register_type_query(definition))

        conn.commit()


def create_materialized_view_query(relation):
    return 'CREATE MATERIALIZED VIEW relation."{}" AS\n{}'.format(
        relation['name'],
        relation['query']
    )


def register_type_query(relation):
    return "SELECT relation_directory.register_type('{}');".format(
        relation['name']
    )


def define_materializations(instance_root):
    definition_files = glob.glob(os.path.join(instance_root, 'materialization/*.sql'))

    for definition_file_path in definition_files:
        print(definition_file_path)

        with open(definition_file_path) as definition_file:
            sql = definition_file.read()

        with closing(connect()) as conn:
            conn.autocommit = True

            with closing(conn.cursor()) as cursor:
                cursor.execute(sql)


def create_partitions():
    query = "SELECT id FROM trend_directory.trend_store"

    with closing(connect()) as conn:
        conn.autocommit = True

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()

        for trend_store_id, in rows:
            create_partitions_for_trend_store(conn, trend_store_id)


def create_partitions_for_trend_store(conn, trend_store_id):
    query = (
        "SELECT trend_directory.timestamp_to_index(partition_size, generate_series(now() - retention_period, now(), partition_size)) "
        "FROM trend_directory.trend_store WHERE id = %s"
    )
    args = (trend_store_id,)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)

        rows = cursor.fetchall()

    for partition_index, in rows:
        create_partition_for_trend_store(conn, trend_store_id, partition_index)


def create_partition_for_trend_store(conn, trend_store_id, partition_index):
    query = (
        "SELECT p.name, trend_directory.create_partition(p, %s) "
        "FROM trend_directory.trend_store_part p "
        "WHERE p.trend_store_id = %s"
    )
    args = (partition_index, trend_store_id)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)

        name, p = cursor.fetchone()

        print('{} - {}'.format(name, partition_index))

