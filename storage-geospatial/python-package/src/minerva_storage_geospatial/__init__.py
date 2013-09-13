# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from tables import SCHEMA
from plugin import GeospatialPlugin


def create(conn, api_version=3):
    if api_version == 3:
        return GeospatialPlugin(conn)
    elif api_version == 4:
        return GeospatialPlugin(conn)
    else:
        raise Exception("Unsupported API version: {}".format(api_version))


class GeospatialPluginV4(object):
    def __init__(self, conn):
        raise NotImplementedError()
