"""Provides the 'deduce' sub-command."""
import json

from minerva.commands import LoadHarvestPlugin, ListPlugins, load_json
from minerva.harvest.trend_config_deducer import deduce_config


def setup_deduce_parser(subparsers):
    """Setup the argument parser for the 'deduce' sub-command."""
    cmd = subparsers.add_parser(
        "deduce", help="command for deducing trend stores from data"
    )

    cmd.add_argument("file_path", nargs="?", help="path of file that will be processed")

    cmd.add_argument(
        "-p",
        "--plugin",
        action=LoadHarvestPlugin,
        help="harvester plug-in to use for processing file(s)",
    )

    cmd.add_argument(
        "-l",
        "--list-plugins",
        action=ListPlugins,
        help="list installed Harvester plug-ins",
    )

    cmd.add_argument(
        "--parser-config", type=load_json, help="parser specific configuration"
    )

    cmd.add_argument(
        "--data-source",
        default="default",
        help="name of the data source of the trend store",
    )

    cmd.add_argument(
        "--granularity", default="1 day", help="granularity of the new trend store"
    )

    cmd.add_argument(
        "--partition-size", default=86400, help="partition size of the trend store"
    )

    cmd.set_defaults(cmd=deduce_trend_store_cmd(cmd))


def deduce_trend_store_cmd(cmd_parser):
    def cmd(args):
        if "plugin" not in args:
            cmd_parser.print_help()
            return

        parser = args.plugin.create_parser(args.parser_config)

        config = deduce_config(args.file_path, parser)

        print(json.dumps(config, sort_keys=True, indent=4))

    return cmd
