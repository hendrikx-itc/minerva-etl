from contextlib import closing

from minerva.db.util import create_copy_from_file, create_copy_from_query


class Existence:
    def __init__(self, conn):
        self.conn = conn
        self.dns = []

    def mark_existing(self, dns):
        self.dns.extend(dns)

    def flush(self, timestamp):
        column_names = ["dn"]

        with closing(self.conn.cursor()) as cursor:
            cursor.copy_expert(
                create_copy_from_query(
                    'directory.existence_staging', column_names
                ),
                create_copy_from_file(((dn,) for dn in self.dns), ("s",))
            )

            cursor.execute(
                'SELECT directory.transfer_existence(%s)', (timestamp,)
            )

        self.conn.commit()

        self.dns = []
