# -*- coding: utf-8 -*-
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
