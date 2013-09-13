import os
import re
import logging
from contextlib import closing
import psycopg2.extras
from functools import wraps

from minerva.db import parse_db_url
import minerva.directory.helpers as directory_helpers
import minerva.system.helpers as system_helpers

QUERY_SEP = "\n"


def connect():
    db_url = os.getenv("TEST_DB_URL")

    if db_url is None:
        raise Exception("Environment variable TEST_DB_URL not set")

    scheme, user, password, host, port, database = parse_db_url(db_url)

    if scheme != "postgresql":
        raise Exception("Only PostgreSQL connections are supported")

    conn = psycopg2.connect(database=database, user=user, password=password,
         host=host, port=port, connection_factory=psycopg2.extras.LoggingConnection)

    logging.info("connected to database {0}/{1}".format(host, database))

    conn.initialize(logging.getLogger(""))

    return conn


def with_connection(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        with closing(connect()) as conn:
            return f(conn, *args, **kwargs)

    return decorated


def clear_database(cursor):
    cursor.execute("DELETE FROM directory.entitytype CASCADE")
    cursor.execute("DELETE FROM directory.datasource CASCADE")
    cursor.execute("DELETE FROM directory.tag CASCADE")
    cursor.execute("DELETE FROM relation.type WHERE name <> 'self'")

    drop_all_tables(cursor, "data", ".*")


def drop_all_tables(cursor, schema, table_name_regex):
    regex = re.compile(table_name_regex)

    tables = [table for table in get_tables(cursor, schema) if regex.match(table)]

    for table_name in tables:
        drop_table(cursor, schema, table_name)

        logging.info("dropped table {0}".format(table_name))


def get_tables(cursor, schema):
    query = QUERY_SEP.join([
        "SELECT table_name",
        "FROM information_schema.tables",
        "WHERE table_schema='{0}'".format(schema)])

    cursor.execute(query)

    return [table_name for table_name, in cursor.fetchall()]


def drop_table(cursor, schema, table):
    cursor.execute("DROP TABLE {0}.{1}".format(schema, table))

