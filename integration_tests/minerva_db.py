import re
import logging
from contextlib import closing


QUERY_SEP = "\n"


def clear_database(conn):
    with closing(conn.cursor()) as cursor:
        cursor.execute("DELETE FROM directory.data_source")
        cursor.execute("DELETE FROM directory.entity_type")
        cursor.execute("DELETE FROM directory.tag")
        cursor.execute("DELETE FROM relation.type WHERE name <> 'self'")


def drop_all_tables(cursor, schema, table_name_regex):
    regex = re.compile(table_name_regex)

    tables = [table for table in get_tables(cursor, schema)
              if regex.match(table)]

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
