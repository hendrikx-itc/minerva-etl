# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import time
import datetime
from calendar import timegm

import pytz


def to_unix_timestamp(t):
    """Return Unix timestamp for datetime instance."""
    return timegm(t.utctimetuple())


def from_unix_timestamp(ts):
    """Return datetime from unix timestamp."""
    timetuple = time.gmtime(ts)
    return pytz.UTC.localize(datetime.datetime(*timetuple[:6]))
