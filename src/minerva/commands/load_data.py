# -*- coding: utf-8 -*-
import logging
from pathlib import Path

from minerva.loading.loader import Loader, create_regex_filter
from minerva.util import k
from minerva.commands import ListPlugins, load_json


class ConfigurationError(Exception):
    pass


package_name = "minerva_harvesting"


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'load-data', help='command for loading data'
    )

    cmd.add_argument(
        "file", nargs="*",
        help="path of file that will be processed"
    )

    cmd.add_argument(
        "--type",
        help="type of data file(s) to process"
    )

    cmd.add_argument(
        "-l", "--list-plugins", action=ListPlugins,
        help="list installed Harvester plug-ins"
    )

    cmd.add_argument(
        "--parser-config", type=Path,
        help="parser specific configuration"
    )

    cmd.add_argument(
        "--pretend", action="store_true", default=False,
        help="only process data, do not write to database"
    )

    cmd.add_argument(
        "--show-progress", action="store_true",
        dest="show_progress", default=False, help="show progressbar"
    )

    cmd.add_argument(
        "--debug", action="store_true", dest="debug",
        default=False, help="produce debug output"
    )

    cmd.add_argument(
        "--dn-filter", type=create_regex_filter, default=k(True),
        help="filter by distinguished name"
    )

    cmd.add_argument(
        "--column-filter", type=create_regex_filter,
        default=k(True), help="filter by trend name"
    )

    cmd.add_argument(
        "--data-source", default="load-data", help="data source to use"
    )

    cmd.add_argument(
        "--statistics", action="store_true", dest="statistics", default=False,
        help="show statistics like number of packages, entities, etc."
    )

    cmd.add_argument(
        "--merge-packages", action="store_true", default=False,
        help="merge packages by entity type and granularity"
    )

    cmd.set_defaults(cmd=load_data_cmd(cmd))


def load_data_cmd(cmd_parser, stop_on_missing_entity_type=False):
    def cmd(args):
        if args.parser_config is not None:
            parser_config = load_json(args.parser_config)
        else:
            parser_config = None

        loader = Loader()
        loader.debug = args.debug
        loader.data_source = args.data_source
        loader.merge_packages = args.merge_packages
        loader.stop_on_missing_entity_type = stop_on_missing_entity_type

        if args.debug:
            logging.root.setLevel(logging.DEBUG)

        loader.statistics = args.statistics
        loader.pretend = args.pretend

        if 'type' not in args:
            cmd_parser.print_help()
            return

        for file_path_str in args.file:
            file_path = Path(file_path_str)

            if file_path.is_file():
                loader.load_data(args.type, parser_config, file_path)
            else:
                print(f"No such file: {file_path}")

    return cmd
