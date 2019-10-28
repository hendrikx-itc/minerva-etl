import os
import sys
import datetime

import yaml
import pytz

import minerva
from minerva.storage.trend.granularity import str_to_granularity
from minerva.harvest.plugins import get_plugin
from minerva.db import connect
from minerva.commands.load_data import create_store_db_context
from minerva.directory.entitytype import NoSuchEntityType


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'load-sample-data', help='command for loading sample data'
    )

    cmd.add_argument(
        '-i', '--instance-root',
        help='root directory of the instance definition'
    )

    cmd.set_defaults(cmd=load_sample_data_cmd)


def load_sample_data_cmd(args):
    instance_root = (
        args.instance_root or os.environ.get('INSTANCE_ROOT') or os.getcwd()
    )

    sys.stdout.write(
        "Loading sample data from '{}' ...\n".format(instance_root)
    )

    load_sample_data(instance_root)

    sys.stdout.write("Done\n")


def load_sample_data(instance_root):
    sys.path.append(os.path.join(instance_root, 'sample-data'))

    definition_file_path = os.path.join(
        instance_root, 'sample-data/definition.yaml'
    )

    with open(definition_file_path) as definition_file:
        definition = yaml.load(definition_file, Loader=yaml.SafeLoader)

        for name, data_set_config in definition.items():
            generate_and_load(name, data_set_config)


def generate_and_load(name, config):
    interval_count = 10

    print("Loading dataset '{}' of type '{}'".format(
        name, config['data_type']
    ))

    data_set_generator = __import__(name)

    if 'granularity' in config:
        granularity = str_to_granularity(config['granularity'])

    now = pytz.utc.localize(datetime.datetime.utcnow())

    end = granularity.truncate(now)

    start = end - (granularity.delta * interval_count)

    timestamp_range = granularity.range(start, end)

    target_dir = "/tmp"

    data_source = 'u2020-pm'

    plugin = get_plugin(config['data_type'])
    parser = plugin.create_parser({})
    storage_provider = create_store_db_context(
        data_source, parser.store_command(), connect
    )

    action = {} #'load-sample-data'

    with storage_provider() as store:
        for timestamp in timestamp_range:
            print(' - {}'.format(timestamp))

            file_path = data_set_generator.generate(target_dir, timestamp, granularity)

            with open(file_path) as data_file:
                packages_generator = parser.load_packages(data_file, file_path)

                packages = minerva.storage.trend.datapackage.DataPackage.merge_packages(packages_generator)

                for package in packages:
                    try:
                        store(package, action)
                    except NoSuchEntityType as exc:
                        # Suppress messages about missing entity types
                        pass



