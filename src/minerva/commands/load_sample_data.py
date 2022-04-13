import gzip
import os
from typing import Optional

import sys
import datetime
import subprocess
from pathlib import Path
from importlib import import_module
import tempfile

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
        "load-sample-data", help="command for loading sample data"
    )

    cmd.add_argument(
        "-i", "--instance-root", help="root directory of the instance definition"
    )

    cmd.add_argument(
        "--interval-count",
        default=30,
        type=int,
        help="number of intervals for which to generate trend data",
    )

    cmd.add_argument("dataset", nargs="?", help="name of the dataset to load")

    cmd.set_defaults(cmd=load_sample_data_cmd)


def load_sample_data_cmd(args):
    instance_root = (
        args.instance_root or os.environ.get(INSTANCE_ROOT_VARIABLE) or os.getcwd()
    )

    sys.stdout.write("Loading sample data from '{}' ...\n".format(instance_root))

    try:
        load_sample_data(instance_root, args.interval_count, args.dataset)
    except ConfigurationError as exc:
        sys.stdout.write("{}\n".format(str(exc)))

    sys.stdout.write("Done\n")


def load_sample_data(
    instance_root: str, interval_count: int, data_set: Optional[str] = None
):
    sys.path.append(os.path.join(instance_root, "sample-data"))

    definition_file_path = Path(instance_root, "sample-data/definition.yaml")

    if not definition_file_path.is_file():
        raise ConfigurationError(
            f"No sample data definition found: {definition_file_path}"
        )

    with definition_file_path.open() as definition_file:
        definitions = yaml.load(definition_file, Loader=yaml.SafeLoader)

        for definition in definitions:
            definition_type, config = definition.popitem()

            # If the data set is specified, then only generate the specified
            # data set.
            if (data_set is None) or (data_set == config["name"]):
                if definition_type == "native":
                    generate_and_load(config, interval_count)
                elif definition_type == "command":
                    cmd_generate_and_load(config)


def cmd_generate_and_load(config):
    name = config["name"]

    print(f"Loading dataset '{name}'")

    data_set_generator = import_module(name)

    target_dir = tempfile.mkdtemp()

    for cmd in data_set_generator.generate(target_dir):
        print(" - executing: {}".format(" ".join(cmd)))

        subprocess.run(cmd, shell=False, check=True)


def generate_and_load(config, interval_count: int):
    name = config["name"]

    data_set_generator = __import__(name)

    if "granularity" in config:
        granularity = str_to_granularity(config["granularity"])

    now = pytz.utc.localize(datetime.datetime.utcnow())

    end = granularity.truncate(now)

    start = end - (granularity.delta * interval_count)

    timestamp_range = granularity.range(start, end)

    print(
        "Loading dataset '{}' of type '{}': {} - {}".format(
            name, config["data_type"], start, end
        )
    )

    target_dir = tempfile.mkdtemp()

    data_source = config["data_source"]

    plugin = get_plugin(config["data_type"])

    if not plugin:
        raise ConfigurationError(
            "No plugin found for data type '{}'".format(config["data_type"])
        )

    parser = plugin.create_parser(config.get("parser_config"))
    storage_provider = create_store_db_context(
        data_source, parser.store_command(), connect
    )

    action = {}

    file_count = 0

    with storage_provider() as store:

        def process_file(data_file, file_path):
            packages_generator = parser.load_packages(data_file, file_path)

            packages = DataPackage.merge_packages(packages_generator)

            for package in packages:
                try:
                    store(package, action)
                except NoSuchEntityType:
                    # Suppress messages about missing entity types
                    pass

            print(f"- {file_path}")

        for timestamp in timestamp_range:
            print(" " * 60, end="\r")
            print(f" - {timestamp}", end="\r")

            generate_result = data_set_generator.generate(
                target_dir, timestamp, granularity
            )

            try:
                # Older data generators return one file path per timestamp
                with open_file(Path(generate_result)) as data_file:
                    process_file(data_file, generate_result)
                    file_count += 1
            except TypeError:
                # Newer data generators return a generator of file paths per timestamp
                for file_path in generate_result:
                    with open_file(Path(file_path)) as data_file:
                        process_file(data_file, file_path)
                        file_count += 1

    print(" " * 60, end="\r")
    print(f"Loaded {file_count} files for {interval_count} intervals")


def open_file(file_path: Path):
    if file_path.suffix == ".gz":
        return gzip.open(file_path)
    else:
        return file_path.open()
