# -*- coding: utf-8 -*-
"""
Unit tests for the storing of data packages.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import os

import psycopg2
from configobj import ConfigObj

from minerva.instance.error import ConfigurationError

INSTANCES_PATH = "/etc/minerva/instances"
INSTANCE_TYPES_PATH = "/usr/lib/minerva/instance_types"


class MinervaInstance(object):
    def __init__(self, config):
        self.config = config

    def type(self):
        return MinervaInstanceType(self.config.get("type", "default"))

    def get_db_uri(self, user):
        return "postgresql://{user}@{host}:{port}/{name}".format(
            user=user,
            host=self.config["database"]["host"],
            port=self.config["database"]["port"],
            name=self.config["database"]["name"])

    def connect(self, **kwargs):
        db_conf = self.config["database"]

        merged_kwargs = {
            "database": db_conf.get("name"),
            "host": db_conf.get("host"),
            "port": db_conf.get("port")}

        merged_kwargs.update(kwargs)

        return psycopg2.connect(**merged_kwargs)

    @staticmethod
    def load(name):
        return MinervaInstance(MinervaInstance.load_config(name))

    @staticmethod
    def load_config(name):
        instance_config_path = os.path.join(INSTANCES_PATH,
                                            "{}.conf".format(name))

        if not(os.path.isfile(instance_config_path)):
            raise ConfigurationError("no such instance '{}'".format(name))

        return ConfigObj(instance_config_path)


class MinervaInstanceType(object):
    def __init__(self, name):
        self.name = name

    def path(self):
        return os.path.join(INSTANCE_TYPES_PATH, self.name)
