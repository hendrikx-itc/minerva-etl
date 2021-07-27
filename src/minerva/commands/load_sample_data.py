import os
from typing import Optional

import sys
import datetime
import subprocess
from pathlib import Path
from importlib import import_module

import yaml
import pytz

from minerva.storage.trend.granularity import str_to_granularity
from minerva.storage.trend.datapackage import DataPackage
from minerva.harvest.plugins import get_plugin
from minerva.db import connect
from minerva.loading.loader import create_store_db_context
from minerva.directory.entitytype import NoSuchEntityType
from minerva.commands import ConfigurationError
from minerva.instance import INSTANCE_ROOT_VARIABLE


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'load-sample-data', help='command for loading sample data'
    )

    cmd.add_argument(
        '-i', '--instance-root',
        help='root directory of the instance definition'
    )

    cmd.add_argument(
        '--interval-count', default=30, type=int,
        help='number of intervals for which to generate trend data'
    )

    cmd.add_argument(
        'dataset', nargs='?', help='name of the dataset to load'
    )

    cmd.set_defaults(cmd=load_sample_data_cmd)


def load_sample_data_cmd(args):
    instance_root = (
        args.instance_root or os.environ.get(INSTANCE_ROOT_VARIABLE) or os.getcwd()
    )

    sys.stdout.write(
        "Loading sample data from '{}' ...\n".format(instance_root)
    )

    try:
        load_sample_data(instance_root, args.interval_count, args.dataset)
    except ConfigurationError as exc:
        sys.stdout.write('{}\n'.format(str(exc)))

    sys.stdout.write("Done\n")


def load_sample_data(instance_root: str, interval_count: int, data_set: Optional[str]=None):
    sys.path.append(os.path.join(instance_root, 'sample-data'))

    definition_file_path = Path(
        instance_root, 'sample-data/definition.yaml'
    )

    if not definition_file_path.is_file():
        raise ConfigurationError(f"No sample data definition found: {definition_file_path}")

    with definition_file_path.open() as definition_file:
        definitions = yaml.load(definition_file, Loader=yaml.SafeLoader)

        for definition in definitions:
            definition_type, config = definition.popitem()

            # If the data set is specified, then only generate the specified
            # data set.
            if (data_set is None) or (data_set == config['name']):
                if definition_type == 'native':
                    generate_and_load(config, interval_count)
                elif definition_type == 'command':
                    cmd_generate_and_load(config)


def cmd_generate_and_load(config):
    name = config['name']

    print("Loading dataset '{}'".format(name))

    data_set_generator = import_module(name)

    target_dir = '/tmp'

    for cmd in data_set_generator.generate(target_dir):
        print(' - executing: {}'.format(' '.join(cmd)))

        subprocess.run(cmd)


def generate_and_load(config, interval_count: int):
    name = config['name']

    data_set_generator = __import__(name)

    if 'granularity' in config:
        granularity = str_to_granularity(config['granularity'])

    now = pytz.utc.localize(datetime.datetime.utcnow())

    end = granularity.truncate(now)

    start = end - (granularity.delta * interval_count)

    timestamp_range = granularity.range(start, end)

    print("Loading dataset '{}' of type '{}': {} - {}".format(
        name, config['data_type'], start, end
    ))

    target_dir = "/tmp"

    data_source = config['data_source']

    plugin = get_plugin(config['data_type'])

    if not plugin:
        raise ConfigurationError(
            "No plugin found for data type '{}'".format(config['data_type'])
        )

    parser = plugin.create_parser(config.get('parser_config'))
    storage_provider = create_store_db_context(
        data_source, parser.store_command(), connect
    )

    action = {} #'load-sample-data'

    with storage_provider() as store:
        for timestamp in timestamp_range:
            print(' ' * 60, end='\r')
            print(' - {}'.format(timestamp), end='\r')

            file_path = data_set_generator.generate(
                target_dir, timestamp, granularity
            )

            with open(file_path) as data_file:
                packages_generator = parser.load_packages(data_file, file_path)

                packages = DataPackage.merge_packages(packages_generator)

                for package in packages:
                    try:
                        store(package, action)
                    except NoSuchEntityType as exc:
                        # Suppress messages about missing entity types
                        pass

    print(' ' * 60, end='\r')
    print('Loaded data for {} intervals'.format(interval_count))
