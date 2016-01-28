from minerva.util.timestamp import to_unix_timestamp, from_unix_timestamp


class Partitioning:
    def __init__(self, size):
        self.size = size

    def index(self, timestamp):
        unix_timestamp = to_unix_timestamp(timestamp)
        index, remainder = divmod(unix_timestamp, self.size)

        if remainder > 0:
            return index
        else:
            return index - 1

    def timestamp(self, index):
        return from_unix_timestamp(index * self.size)

    def index_to_interval(self, partition_index):
        return (
            self.timestamp(partition_index),
            self.timestamp(partition_index + 1)
        )

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
