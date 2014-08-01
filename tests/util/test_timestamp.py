from datetime import datetime

import pytz
from nose.tools import eq_

from minerva.util.timestamp import to_unix_timestamp, \
    from_unix_timestamp


TIMEZONE = "Europe/Amsterdam"


def test_to_unix_timestamp():
    tzinfo = pytz.timezone(TIMEZONE)

    naive_timestamp = datetime(2013, 5, 16, 9, 30)
    timestamp = tzinfo.localize(naive_timestamp)

    unix_timestamp = to_unix_timestamp(timestamp)

    expected_unix_timestamp = 1368689400

    delta = unix_timestamp - expected_unix_timestamp

    eq_(delta, 0)

    eq_(unix_timestamp, expected_unix_timestamp)

    timestamp = pytz.utc.localize(datetime(1970, 1, 5, 8, 0))
    unix_timestamp = to_unix_timestamp(timestamp)

    eq_(unix_timestamp, 4 * 86400 + 8 * 3600)


def test_from_unix_timestamp():
    timestamp = from_unix_timestamp(0)
    expected_timestamp = pytz.utc.localize(datetime(1970, 1, 1, 0, 0, 0))
    eq_(timestamp, expected_timestamp)

    timestamp = from_unix_timestamp(1365022800)
    expected_timestamp = pytz.utc.localize(datetime(2013, 4, 3, 21, 0, 0))
    eq_(timestamp, expected_timestamp)
