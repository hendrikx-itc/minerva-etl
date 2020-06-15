import json
import sys
import argparse
from collections import OrderedDict

import yaml

from minerva.util.tabulate import render_table
from minerva.harvest.plugins import iter_entry_points, \
    get_plugin as get_harvest_plugin


class ConfigurationError(Exception):
    pass


class ListPlugins(argparse.Action):
    def __init__(self, option_strings, dest=argparse.SUPPRESS,
                 default=argparse.SUPPRESS, help=None):
        super(ListPlugins, self).__init__(
            option_strings=option_strings, dest=dest, default=default, nargs=0,
            help=help
        )

    def __call__(self, parser, namespace, values, option_string=None):
        for entry_point in iter_entry_points():
            print(entry_point.name)

        sys.exit(0)


class LoadHarvestPlugin(argparse.Action):
    def __init__(self, option_strings, dest=argparse.SUPPRESS,
                 default=argparse.SUPPRESS, help=None):
        super(LoadHarvestPlugin, self).__init__(
            option_strings=option_strings, dest=dest, default=default,
            nargs=1, help=help
        )

    def __call__(self, parser, namespace, values, option_string=None):
        plugin_name = values[0]

        plugin = get_harvest_plugin(plugin_name)

        if plugin is None:
            print("Data type '{0}' not supported".format(plugin_name))
            sys.exit(1)

        setattr(namespace, self.dest, plugin)


def load_json(path):
    with open(path) as config_file:
        return json.load(config_file)


class SqlSrc(str):
    @staticmethod
    def representer(dumper, data):
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')


def ordered_yaml_dump(data, stream=None, Dumper=yaml.Dumper, **kwds):
    class OrderedDumper(Dumper):
        pass

    def _dict_representer(dumper, data: OrderedDict):
        return dumper.represent_mapping(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            data.items()
        )

    OrderedDumper.add_representer(OrderedDict, _dict_representer)
    OrderedDumper.add_representer(SqlSrc, SqlSrc.representer)

    return yaml.dump(data, stream, OrderedDumper, **kwds)


def show_rows(column_names, rows, show_cmd=print):
    column_align = "<" * len(column_names)
    column_sizes = ["max"] * len(column_names)

    for line in render_table(column_names, column_align, column_sizes, rows):
        show_cmd(line)


def show_rows_from_cursor(cursor, show_cmd=print):
    """
    Take the results from a query executed on a cursor and show them in a
    table with the field names as column names.
    :param cursor: Psycopg2 cursor where a query has been executed
    :param show_cmd: function that writes the lines
    :return:
    """
    show_rows(
        [c.name for c in cursor.description],
        cursor.fetchall(),
        show_cmd
    )