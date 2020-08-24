import json
import sys
import argparse

from minerva.util.tabulate import render_table
from minerva.harvest.plugins import list_plugins, \
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
        for name in list_plugins():
            print(name)

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


def show_rows(column_names, rows, show_cmd=print):
    column_align = ["<"] * len(column_names)
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
