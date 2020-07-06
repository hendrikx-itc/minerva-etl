# -*- coding: utf-8 -*-
"""Distutils install script."""
from setuptools import setup

# Load module with version
ns = {}

mod_path = 'src/minerva/__init__.py'

with open(mod_path) as mod_file:
    exec(mod_file.read(), ns)


setup(
    name="minerva-etl",
    author="Hendrikx ITC",
    author_email="info@hendrikx-itc.nl",
    version=ns['__version__'],
    license="GPL",
    python_requires='>3.6',
    install_requires=[
        "pytz", "psycopg2>=2.8", "PyYAML", "configobj",
        "python-dateutil", "pyparsing", "jinja2"
    ],
    extras_require={
        "tests": ["pytest", "docker"]
    },
    packages=[
        "minerva",
        "minerva.commands",
        "minerva.util",
        "minerva.test",
        "minerva.db",
        "minerva.system",
        "minerva.directory",
        "minerva.instance",
        "minerva.storage",
        "minerva.storage.trend",
        "minerva.storage.trend.test",
        "minerva.storage.attribute",
        "minerva.storage.notification",
        "minerva.harvest",
        "minerva.trigger",
        "minerva.loading",
        "minerva.loading.csv",
    ],
    package_dir={"": "src"},
    entry_points={
        'console_scripts': [
            'minerva = minerva.commands.minerva_cli:main'
        ]
    },
    include_package_data=True
)
