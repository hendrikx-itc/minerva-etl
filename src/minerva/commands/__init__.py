import json
import sys
import argparse
from collections import OrderedDict

import yaml

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
