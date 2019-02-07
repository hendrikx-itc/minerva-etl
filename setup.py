# -*- coding: utf-8 -*-
from setuptools import setup


setup(
    name='minerva-etl',
    author='Hendrikx ITC',
    author_email='info@hendrikx-itc.nl',
    version='5.0.0.dev2',
    license='GPL',
    description='Minerva ETL client library and commands',
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
