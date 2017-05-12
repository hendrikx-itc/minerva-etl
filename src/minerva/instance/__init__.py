# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import os

import psycopg2.extras
from configobj import ConfigObj

from minerva.instance.error import ConfigurationError

INSTANCES_PATH = "/etc/minerva/instances"
CLASSES_PATH = "/usr/lib/minerva/classes"


class MinervaInstance(object):
    def __init__(self, config):
        self.config = config

    def minerva_class(self):
        return MinervaClass(self.config.get("class", "default"))

    def get_db_uri(self, user):
        return "postgresql://{user}@{host}:{port}/{name}".format(
            user=user,
            host=self.config["database"]["host"],
            port=self.config["database"]["port"],
            name=self.config["database"]["name"])

    def connect_logging(self, logger, **kwargs):
        db_conf = self.config["database"]

        merged_kwargs = {
            "database": db_conf.get("name"),
            "host": db_conf.get("host"),
            "port": db_conf.get("port")}

        merged_kwargs.update(kwargs)

        merged_kwargs["connection_factory"] = psycopg2.extras.LoggingConnection

        conn = psycopg2.connect(**merged_kwargs)
        conn.initialize(logger)

        return conn

    def connect(self, **kwargs):
        """
        Return new database connection.

        The kwargs are merged with the database configuration of the instance
        and passed directly to the psycopg2 connect function.
        """
        db_conf = self.config["database"]

        merged_kwargs = {
            "database": db_conf.get("name"),
            "host": db_conf.get("host"),
            "port": db_conf.get("port")}

        merged_kwargs.update(kwargs)

        return psycopg2.connect(**merged_kwargs)

    def connect_ro(self, **kwargs):
        """
        Return new read-only database connection.

        The kwargs are merged with the read-only database configuration of the
        instance and passed directly to the psycopg2 connect function.
        """
        db_conf = self.config["database_ro"]

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


class MinervaClass(object):
    def __init__(self, name):
        self.name = name

    def path(self):
        return os.path.join(CLASSES_PATH, self.name)
