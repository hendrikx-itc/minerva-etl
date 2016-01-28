# -*- coding: utf-8 -*-
import os
import re
from io import StringIO
import pkg_resources

from configobj import ConfigObj


class ConfigError(Exception):
    """
    Base class for any configuration loading related exceptions.
    """
    pass


def get_config(default, configfile):
    config = ConfigObj(StringIO(default))

    if os.path.isfile(configfile):
        custom_config = ConfigObj(configfile)
        config.merge(custom_config)
    else:
        print("'{0}' doesn't exist. Using default config.".format(configfile))

    return config


def parse_size(size_str):
    unit_symbols = ["B", "kB", "MB", "GB", "TB"]
    size_units = dict(
        (unit.lower(), pow(1024, order))
        for order, unit in enumerate(unit_symbols)
    )

    pattern = "([1-9][0-9]*)({0:s})".format("|".join(unit_symbols))

    m = re.match(pattern, size_str, re.IGNORECASE)

    if m is None:
        raise Exception("{0:s} does not match pattern {1:s}".format(
            size_str, pattern))

    amount, unit = m.groups()

    return int(amount) * size_units[unit.lower()]


def get_defaults(package, name):
    """
    Return a string representing a default/template config file named `name`.
    """
    return pkg_resources.resource_string(package, "defaults/{}".format(name))


def load_config(defaults, path):
    """
    Load a configuration from file `path`, merge it with the default
    configuration and return a ConfigObj instance. So if any config option is
    missing, it is filled in with a default.

    Raises a :exc:`ConfigError` when the specified file doesn't exist.
    """
    if not os.path.isfile(path):
        raise ConfigError("config file '{0}' doesn't exist".format(path))

    config = ConfigObj(defaults)
    custom_config = ConfigObj(path)
    config.merge(custom_config)

    return config
