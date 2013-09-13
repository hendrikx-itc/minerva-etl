import time
import datetime
from calendar import timegm

import pytz


class Partitioning(object):
    def __init__(self, size):
        self.size = size

    def index(self, timestamp):
        unix_timestamp = to_unix_timestamp(timestamp)
        index, remainder = divmod(unix_timestamp, self.size)

        if remainder > 0:
            return index
        else:
            return index - 1

    def index_to_interval(self, partition_index):
        unix_timestamp_start = partition_index * self.size
        unix_timestamp_end = unix_timestamp_start + self.size
        start = from_unix_timestamp(unix_timestamp_start)
        end = from_unix_timestamp(unix_timestamp_end)

        return start, end

#
# unix timestamp epoch = 1970-01-01 00:00:00+00
#
# partition size is in seconds
#
# partitions are intervals defined as blocks of partition size in the unix
# timestamp range.
#
# if partition size is 86400, then the first interval will be
#
# 1970-01-01 00:00:00+00 - 1970-01-02 00:00:00+00
#
# if partition size is 86400 * 4, then the first interval will be
#
# 1970-01-01 00:00:00+00 - 1970-01-05 00:00:00+00
#
# if granularity is 86400 then the previously named interval will contain
# the following timestamps:
#
#   86400 * 1 =  86400 = 1970-01-02 00:00:00+00
#   86400 * 2 = 172800 = 1970-01-03 00:00:00+00
#   86400 * 3 = 259200 = 1970-01-04 00:00:00+00
#   86400 * 4 = 345600 = 1970-01-05 00:00:00+00
#

def to_unix_timestamp(t):
    return timegm(t.utctimetuple())


def from_unix_timestamp(ts):
    timetuple = time.gmtime(ts)
    return pytz.UTC.localize(datetime.datetime(*timetuple[:6]))
