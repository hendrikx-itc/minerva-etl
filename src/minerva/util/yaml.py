from typing import Type
from collections import OrderedDict

import yaml


class SqlSrc(str):
    @staticmethod
    def representer(dumper, data: str) -> str:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


def dict_representer(dumper: Type[yaml.Dumper], data: OrderedDict):
    return dumper.represent_mapping(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, data.items()
    )


def ordered_yaml_dump(data, stream=None, Dumper=yaml.Dumper, **kwds):
    class OrderedDumper(Dumper):
        pass

    OrderedDumper.add_representer(OrderedDict, dict_representer)
    OrderedDumper.add_representer(SqlSrc, SqlSrc.representer)

    return yaml.dump(data, stream, OrderedDumper, **kwds)


def ordered_yaml_load(
    stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict
) -> OrderedDict:
    class OrderedLoader(Loader):
        pass

    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))

    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_mapping
    )

    return yaml.load(stream, OrderedLoader)  # nosec
