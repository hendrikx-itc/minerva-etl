# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.storage.attribute.plugin import AttributePlugin as AttributePlugin3
from minerva.storage.attribute.plugin_v4 import AttributePlugin as AttributePlugin4


def create(conn, api_version=3):
    if api_version == 3:
        return AttributePlugin3(conn)
    elif api_version == 4:
        return AttributePlugin4(conn)
    else:
        raise Exception("Unsupported API version: {}".format(api_version))