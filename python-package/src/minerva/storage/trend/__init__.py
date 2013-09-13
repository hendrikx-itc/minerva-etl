# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.storage.trend.plugin import TrendPlugin as TrendPluginV3
from minerva.storage.trend.plugin_v4 import TrendPlugin as TrendPluginV4
from minerva.storage.trend.helpers import get_previous_timestamp, \
    get_most_recent_timestamp, get_next_timestamp


def create(conn, api_version=3):
    if api_version == 3:
        return TrendPluginV3(conn)
    elif api_version == 4:
        return TrendPluginV4(conn)
    else:
        raise Exception("Unsupported API version: {}".format(api_version))


# Hack for backward compatibility with code that uses
create.get_previous_timestamp = get_previous_timestamp
create.get_next_timestamp = get_next_timestamp
create.get_most_recent_timestamp = get_most_recent_timestamp
