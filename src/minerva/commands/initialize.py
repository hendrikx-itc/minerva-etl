import os
from contextlib import closing
import sys
import glob

import yaml
import psycopg2.errors

from minerva.db import connect

from minerva.commands.attribute_store import create_attribute_store_from_json, \
    DuplicateAttributeStore
from minerva.commands.trend_store import create_trend_store_from_json, \
    DuplicateTrendStore
from minerva.commands.notification_store import \
    create_notification_store_from_json, DuplicateNotificationStore
from minerva.commands.partition import create_partitions_for_trend_store
from minerva.commands.trigger import create_trigger_from_config
from minerva.commands.load_sample_data import load_sample_data


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'initialize',
        help='command for complete initialization of Minerva instance'
    )

    cmd.add_argument(
        '-i', '--instance-root',
        help='root directory of the instance definition'
    )

    cmd.add_argument(
        '--load-sample-data', action='store_true', default=False,
        help='generate and load sample data as specified in instance'
    )

    cmd.set_defaults(cmd=initialize_cmd)


def initialize_cmd(args):
    instance_root = (
        args.instance_root or os.environ.get('INSTANCE_ROOT') or os.getcwd()
    )

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

    if args.load_sample_data:
        header('Loading sample data')
        load_sample_data(instance_root)


def header(title):
    width = (len(title) + 4)

    print('')
    print('#' * width)
    print('# {} #'.format(title))
    print('#' * width)
    print('')


def initialize_instance(instance_root):
    header("Initializing attribute stores")
    initialize_attribute_stores(instance_root)

    header("Initializing trend stores")
    initialize_trend_stores(instance_root)

    header("Initializing notification stores")
    initialize_notification_stores(instance_root)

    header("Initializing virtual entities")
    define_virtual_entities(instance_root)

    header("Defining relations")
    define_relations(instance_root)

    header('Initializing materializations')
    define_materializations(instance_root)

    header('Initializing triggers')
    define_triggers(instance_root)

    header('Creating partitions')
    create_partitions()


def initialize_attribute_stores(instance_root):
    definition_files = glob.glob(
        os.path.join(instance_root, 'attribute/*.yaml')
    )

    for definition_file_path in definition_files:
        print(definition_file_path)

        with open(definition_file_path) as definition_file:
            definition = yaml.load(definition_file, Loader=yaml.SafeLoader)

        try:
            create_attribute_store_from_json(definition)
        except DuplicateAttributeStore as exc:
            print(exc)


def initialize_trend_stores(instance_root):
    definition_files = glob.glob(os.path.join(instance_root, 'trend/*.yaml'))

    for definition_file_path in definition_files:
        print(definition_file_path)

        with open(definition_file_path) as definition_file:
            definition = yaml.load(definition_file, Loader=yaml.SafeLoader)

        try:
            create_trend_store_from_json(definition)
        except DuplicateTrendStore as exc:
            print(exc)


def initialize_notification_stores(instance_root):
    definition_files = glob.glob(os.path.join(instance_root, 'notification/*.yaml'))

    for definition_file_path in definition_files:
        print(definition_file_path)

        with open(definition_file_path) as definition_file:
            definition = yaml.load(definition_file, Loader=yaml.SafeLoader)

        try:
            create_notification_store_from_json(definition)
        except DuplicateNotificationStore as exc:
            print(exc)


def define_virtual_entities(instance_root):
    definition_files = glob.glob(
        os.path.join(instance_root, 'virtual-entity/*.sql')
    )

    for definition_file_path in definition_files:
        print(definition_file_path)

        execute_sql_file(definition_file_path)


def define_relations(instance_root):
    definition_files = glob.glob(os.path.join(instance_root, 'relation/*.yaml'))

    for definition_file_path in definition_files:
        print(definition_file_path)

        with open(definition_file_path) as definition_file:
            definition = yaml.load(definition_file, Loader=yaml.SafeLoader)

        try:
            define_relation(definition)
        except DuplicateRelation as exc:
            print(exc)


class DuplicateRelation(Exception):
    def __str__(self):
        return "Duplicate relation"


def define_relation(definition):
    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            try:
                cursor.execute(create_materialized_view_query(definition))
            except psycopg2.errors.DuplicateTable:
                raise DuplicateRelation(definition)

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


def execute_sql_file(file_path):
    with open(file_path) as definition_file:
        sql = definition_file.read()

    with closing(connect()) as conn:
        conn.autocommit = True

        with closing(conn.cursor()) as cursor:
            cursor.execute(sql)


def define_materializations(instance_root):
    definition_files = glob.glob(
        os.path.join(instance_root, 'materialization/*.sql')
    )

    for definition_file_path in definition_files:
        print(definition_file_path)

        execute_sql_file(definition_file_path)


def define_triggers(instance_root):
    definition_files = glob.glob(os.path.join(instance_root, 'trigger/*.yaml'))

    for definition_file_path in definition_files:
        print(definition_file_path)

        with open(definition_file_path) as definition_file:
            definition = yaml.load(definition_file, Loader=yaml.SafeLoader)

            create_trigger_from_config(definition)


def create_partitions():
    query = "SELECT id FROM trend_directory.trend_store"

    with closing(connect()) as conn:
        conn.autocommit = True

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()

        for trend_store_id, in rows:
            create_partitions_for_trend_store(conn, trend_store_id, '1 day')
