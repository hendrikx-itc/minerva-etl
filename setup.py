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
    install_requires=[
        "pytz", "psycopg2>=2.2.1", "PyYAML", "configobj",
        "python-dateutil", "pyparsing"
    ],
    tests_require=["nose2"],
    test_suite="nose2.collector",
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
        "minerva.instance"
    ],
    package_dir={"": "src"},
    package_data={
        "minerva": ["defaults/*"]
    },
    scripts=[
        "scripts/attribute-to-csv",
        "scripts/backup-views",
        "scripts/cleanup-orphaned-data",
        "scripts/cleanup-trend-data",
        "scripts/create-aliases",
        "scripts/create-csv",
        "scripts/create-network",
        "scripts/create-relations",
        "scripts/create-virtual-entities",
        "scripts/job",
        "scripts/link-attribute-tags",
        "scripts/link-entity-tags",
        "scripts/link-trend-tags",
        "scripts/load-data",
        "scripts/load-datapackage",
        "scripts/load-elements",
        "scripts/load-tags",
        "scripts/materialize-attribute-curr",
        "scripts/materialize-relation-all",
        "scripts/migrate-trendstore",
        "scripts/minerva-dump",
        "scripts/populate-entity-tagarray",
        "scripts/populate-gis",
        "scripts/query-entities",
        "scripts/trend-to-csv",
        "scripts/vacuum-full-trend-data",
    ]
)
