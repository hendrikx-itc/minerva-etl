# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import datetime

import pytz

from minerva.storage.attribute.datapackage import DataPackage


def create_simple_package():
    timestamp = pytz.utc.localize(datetime.datetime(2013, 8, 30, 15, 30))
    attribute_names = ["power", "height", "state"]
    rows = [
        (123001, (405, 0.0, "enabled")),
        (123002, (300, 10.5, "enabled")),
        (123003, (41033, 22.3, "enabled")),
        (123004, (880, 30.0, "enabled"))
    ]

    return DataPackage(timestamp, attribute_names, rows)
