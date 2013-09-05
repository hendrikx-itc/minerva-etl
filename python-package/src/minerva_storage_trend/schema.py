from minerva.db.query import Table

name = "trend"

partition = Table(name, "partition")
modified = Table(name, "modified")
trendstore = Table(name, "trendstore")


def reset(cursor):
    cursor.execute("DELETE FROM trend.trend CASCADE")
    cursor.execute("DELETE FROM trend.partition CASCADE")
    cursor.execute("DELETE FROM trend.trendstore CASCADE")
    cursor.execute("DELETE FROM trend.modified")


system_columns = ['entity_id', 'timestamp', 'modified']
system_columns_set = set(system_columns)
