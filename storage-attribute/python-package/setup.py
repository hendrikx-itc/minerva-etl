# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""

from setuptools import setup


setup(
    name="attribute",
    version="1.1.0",
    author="Hendrikx ITC",
    author_email="info@hendrikx-itc.nl",
    install_requires=["minerva>=3"],
    packages=["minerva_storage_attribute"],
    package_dir={"": "src"},
    package_data = {"minerva_storage_attribute": ["defaults/*"]},
    entry_points= {"minerva.storage.plugins":
        ["attribute = minerva_storage_attribute:create"]},
    scripts = ["scripts/link-attribute-tags", "scripts/attribute-to-csv"]
)
