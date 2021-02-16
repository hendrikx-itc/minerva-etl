import os
from typing import Optional

import sys
import datetime
from pathlib import Path
from importlib import import_module

import yaml
import pytz

from minerva.storage.trend.granularity import str_to_granularity
from minerva.commands import ConfigurationError
from minerva.instance import INSTANCE_ROOT_VARIABLE


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'generate-sample-data', help='command for generating sample data'
    )

    cmd.add_argument(
        '-i', '--instance-root',
        help='root directory of the instance definition'
    )

    cmd.add_argument(
        '--timestamp', default=datetime.datetime.now(), type=datetime.datetime,
        help='timestamp for which to generate data'
    )

    cmd.add_argument(
        '-t', '--target-directory', default=Path("/tmp"), type=Path,
        help="directory where generated files will be written"
    )

    cmd.add_argument(
        'dataset', nargs='?', help='name of the dataset to load'
    )

    cmd.set_defaults(cmd=generate_sample_data_cmd)


def generate_sample_data_cmd(args):
    instance_root = (
        args.instance_root or os.environ.get(INSTANCE_ROOT_VARIABLE) or os.getcwd()
    )

    try:
        generate_sample_data(instance_root, args.timestamp, args.target_directory, args.dataset)
    except ConfigurationError as exc:
        sys.stdout.write('{}\n'.format(str(exc)))


def generate_sample_data(instance_root: str, timestamp: datetime.datetime, target_directory: Path, data_set: Optional[str]=None):
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
                    generate(config, timestamp, target_directory)
                elif definition_type == 'command':
                    cmd_generate(config, target_directory)


def cmd_generate(config, target_directory: Path):
    name = config['name']

    data_set_generator = import_module(name)

    for cmd in data_set_generator.generate(target_directory):
        print(' - executing: {}'.format(' '.join(cmd)))


def generate(config, timestamp: datetime.datetime, target_directory: Path):
    name = config['name']

    data_set_generator = __import__(name)

    if 'granularity' in config:
        granularity = str_to_granularity(config['granularity'])

    now = pytz.utc.localize(timestamp)

    target_timestamp = granularity.truncate(now)

    file_path = data_set_generator.generate(
        target_directory, target_timestamp, granularity
    )

    print(f"Generated file '{file_path}'")
