import os
import re
import logging
from contextlib import closing
import psycopg2.extras

from minerva.db import parse_db_url


QUERY_SEP = "\n"


def connect():
    db_url = os.getenv("TEST_DB_URL")

    if db_url is None:
        raise Exception("Environment variable TEST_DB_URL not set")

    scheme, user, password, host, port, database = parse_db_url(db_url)

    if scheme != "postgresql":
        raise Exception("Only PostgreSQL connections are supported")

    conn = psycopg2.connect(
        database=database, user=user, password=password, host=host, port=port,
        connection_factory=psycopg2.extras.LoggingConnection)

    logging.info("connected to database {0}/{1}".format(host, database))

    conn.initialize(logging.getLogger(""))

    return conn


def clear_database(conn):
    with closing(conn.cursor()) as cursor:
        cursor.execute("TRUNCATE directory.entitytype CASCADE")
        cursor.execute("TRUNCATE directory.entitytag CASCADE")
        cursor.execute("TRUNCATE directory.entitytaglink CASCADE")

    drop_all_tables(conn, "delta", ".*")


def drop_all_tables(conn, schema, table_name_regex):
    regex = re.compile(table_name_regex)

    tables = [table for table in get_tables(conn, schema)
              if regex.match(table)]

    for table_name in tables:
        drop_table(conn, schema, table_name)

        logging.info("dropped table {0}".format(table_name))


def get_tables(conn, schema):
    with closing(conn.cursor()) as cursor:
        query = QUERY_SEP.join([
            "SELECT table_name",
            "FROM information_schema.tables",
            "WHERE table_schema='{0}'".format(schema)])

        cursor.execute(query)

        return [table_name for table_name, in cursor.fetchall()]


def drop_table(conn, schema, table):
    with closing(conn.cursor()) as cursor:
        cursor.execute("DROP TABLE IF EXISTS {0}.{1} CASCADE".format(schema,
                                                                     table))

    conn.commit()
