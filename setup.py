# -*- coding: utf-8 -*-
from os import path
from setuptools import setup

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


classifiers = '''\
Development Status :: 2 - Pre-Alpha
Intended Audience :: Developers
License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)
Programming Language :: Python
Programming Language :: Python :: 3
Programming Language :: Python :: 3.5
Programming Language :: Python :: 3.6
Programming Language :: Python :: 3.7
Topic :: Database
Topic :: Software Development
Topic :: Software Development :: Libraries :: Python Modules
'''

setup(
    name='minerva-etl',
    author='Hendrikx ITC',
    author_email='info@hendrikx-itc.nl',
    version='5.0.0.dev6',
    license='GPL',
    description='Minerva ETL client library and commands',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/hendrikx-itc/python-minerva',
    project_urls={
        'Source': 'https://github.com/hendrikx-itc/python-minerva'
    },
    python_requires='>=3.5',
    install_requires=[
        'pytz', 'psycopg2>=2.2.1', 'PyYAML', 'configobj',
        'python-dateutil', 'pyparsing'
    ],
    packages=[
        'minerva',
        'minerva.commands',
        'minerva.util',
        'minerva.test',
        'minerva.db',
        'minerva.system',
        'minerva.directory',
        'minerva.storage',
        'minerva.storage.trend',
        'minerva.storage.trend.test',
        'minerva.storage.attribute',
        'minerva.storage.notification',
        'minerva.harvest'
    ],
    package_dir={'': 'src'},
    package_data={
        'minerva': ['defaults/*']
    },
    entry_points={
        'console_scripts': [
            'minerva=minerva.commands.minerva:main'
        ]
    }
)
