# -*- coding: utf-8 -*-
"""Distutils install script."""
import sys
from setuptools import setup
from distutils.util import convert_path


# Load module with version
ns = {}

mod_path = 'src/minerva/__init__.py'

with open(mod_path) as mod_file:
    exec(mod_file.read(), ns)


setup(
    name="minerva",
    author="Hendrikx ITC",
    author_email="info@hendrikx-itc.nl",
    version=ns['__version__'],
    license="GPL",
    install_requires=[
        "pytz", "psycopg2>=2.2.1", "PyYAML", "configobj",
        "python-dateutil", "pyparsing"
    ],
    packages=[
        "minerva",
        "minerva.commands",
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
        "minerva.xmldochandler",
        "minerva.xmldochandler.xmlschema",
        "minerva.xmlschemaparser",
        "minerva.schemacontextbuilder",
        "minerva.instance",
        "minerva.harvest"
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
        "scripts/query-entities",
        "scripts/trend-to-csv",
        "scripts/vacuum-full-trend-data",
        "scripts/processfile",
        "scripts/minerva"
    ]
)
