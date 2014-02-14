# -*- coding: utf-8 -*-
"""Distutils install script."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import sys

from setuptools import setup

sys.path.insert(0, "src")
from minerva import __version__

setup(
    name="minerva",
    author="Hendrikx ITC",
    author_email="info@hendrikx-itc.nl",
    version=__version__,
    license="GPL",
    install_requires=["pytz", "psycopg2>=2.2.1", "DBUtils", "PyYAML",
                      "configobj", "python-dateutil", "pyparsing"],
    test_suite="nose.collector",
    packages=[
        "minerva",
        "minerva.util",
        "minerva.test",
        "minerva.db",
        "minerva.system",
        "minerva.directory",
        "minerva.storage",
        "minerva.storage.trend",
        "minerva.storage.trend.test",
        "minerva.storage.attribute",
        "minerva.storage.notification",
        "minerva.storage.geospatial",
        "minerva.xmldochandler",
        "minerva.xmldochandler.xmlschema",
        "minerva.xmlschemaparser",
        "minerva.schemacontextbuilder",
        "minerva.instance"],
    package_dir={"": "src"},
    package_data={
        "minerva": ["defaults/*"]
    },
    scripts=[
        "scripts/attribute-to-csv",
        "scripts/backup-views",
        "scripts/create-relations",
        "scripts/load-tags",
        "scripts/link-entity-tags",
        "scripts/link-trend-tags",
        "scripts/link-attribute-tags",
        "scripts/create-network",
        "scripts/create-virtual-entities",
        "scripts/create-csv",
        "scripts/create-aliases",
        "scripts/job",
        "scripts/load-data",
        "scripts/load-datapackage",
        "scripts/query-entities",
        "scripts/load-elements",
        "scripts/populate-entity-tagarray",
        "scripts/populate-gis",
        "scripts/cleanup-trend-data",
<<<<<<< HEAD
        "scripts/materialize-attribute-curr"]
=======
        "scripts/materialize-attribute-curr",
        "scripts/compact-attribute",
        "scripts/cleanup-orphaned-data",
        "scripts/migrate-trendstore",
        "scripts/trend-to-csv"]
>>>>>>> origin/develop
)
