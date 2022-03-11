# -*- coding: utf-8 -*-
"""
Provides basic database functionality like dropping tables, creating users,
granting privileges, etc.
"""
__docformat__ = "restructuredtext en"
from contextlib import closing

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from minerva.db.error import ExistsError
from minerva.util.tabulate import render_table


def prepare_statements(conn, statements):
    """
    Prepare statements for the specified connection.
    """
    with closing(conn.cursor()) as cursor:
        for statement in statements:
            cursor.execute("PREPARE {0:s}".format(statement))


def disable_transactions(conn):
    """
    Set isolation level to ISOLATION_LEVEL_AUTOCOMMIT so that every query is
    automatically commited.
    """
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)


def full_table_name(name, schema=None):
    if schema is not None:
        return "\"{0:s}\".\"{1:s}\"".format(schema, name)
    else:
        return "\"{0}\"".format(name)


def get_column_names(conn, schema_name, table_name):
    """
    Return list of column names of specified schema and table
    """
    query = (
        "SELECT a.attname FROM pg_attribute a "
        "JOIN pg_class c ON c.oid = a.attrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s "
        "AND a.attnum > 0 AND not attisdropped")

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (schema_name, table_name))
        return [name for name, in cursor.fetchall()]


def grant(conn, object_type, privileges, object_name, group):
    """
    @param privileges = "CREATE"|"USAGE"|"ALL"
    """

    if hasattr(privileges, "__iter__"):
        privileges = ",".join(privileges)

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute("GRANT {1:s} ON {0:s} {2:s} TO GROUP {3:s}".format(
                object_type, privileges, object_name, group))
        except psycopg2.InternalError as exc:
            conn.rollback()

            # TODO: Find a more elegant way to avoid internal errors in 'GRANT'
            # actions.
            if exc.pgcode == psycopg2.errorcodes.INTERNAL_ERROR:
                pass
        else:
            conn.commit()


def drop_table(conn, schema, table):
    with closing(conn.cursor()) as cursor:
        cursor.execute("DROP TABLE {0:s}".format(
            full_table_name(table, schema)))

    conn.commit()


def drop_all_tables(conn, schema):
    with closing(conn.cursor()) as cursor:
        query = (
            "SELECT table_name FROM information_schema.tables WHERE "
            "table_schema = '{0:s}'").format(schema)
        cursor.execute(query)

        rows = cursor.fetchall()

        for (table_name, ) in rows:
            cursor.execute("DROP TABLE {0:s} CASCADE".format(
                full_table_name(table_name, schema)))

    conn.commit()


def table_exists(conn, schema, table):
    with closing(conn.cursor()) as cursor:
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE \
table_schema = '{0:s}' AND table_name = '{1:s}'".format(schema, table))

        (num, ) = cursor.fetchone()

        return num > 0


def column_exists(conn, schema, table, column):
    query = (
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND "
        "column_name = %s"
    )

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (schema, table, column))

        (num, ) = cursor.fetchone()

        return num > 0


def schema_exists(conn, name):
    with closing(conn.cursor()) as cursor:
        query = (
            "SELECT COUNT(*) FROM information_schema.schema "
            "WHERE schema_name = %s"
        )

        query_args = (name,)
        cursor.execute(query, query_args)

        (num, ) = cursor.fetchone()

        return num > 0


def create_schema(conn, name, owner):
    query = (
        "CREATE SCHEMA \"{0:s}\" "
        "AUTHORIZATION {1:s}"
    ).format(name, owner)

    if not schema_exists(conn, name):
        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

        conn.commit()
    else:
        raise ExistsError("Schema {0:s} already exists".format(name))


def alter_table_owner(conn, table, owner):
    with closing(conn.cursor()) as cursor:
        cursor.execute("ALTER TABLE {0} OWNER TO {1}".format(table, owner))

    conn.commit()


def create_user(conn, name, password=None, groups=None):
    query = "SELECT COUNT(*) FROM pg_catalog.pg_shadow WHERE usename = %s;"

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (name,))

        (num, ) = cursor.fetchone()

        if num == 0:
            if password is None:
                query = "CREATE USER {0:s}".format(name)
            else:
                query = "CREATE USER {0:s} LOGIN PASSWORD '{1:s}'".format(
                    name, password)

            cursor.execute(query)

            if groups is not None:
                if hasattr(groups, "__iter__"):
                    for group in groups:
                        query = "GRANT {0:s} TO {1:s}".format(group, name)
                        cursor.execute(query)
                else:
                    query = "GRANT {0:s} TO {1:s}".format(groups, name)
                    cursor.execute(query)

            conn.commit()


def create_group(conn, name, group=None):
    """
    @param conn: database connection
    @param name: name of the new group
    @param group: parent group
    """
    query = "SELECT COUNT(*) FROM pg_catalog.pg_group WHERE groname = %s;"

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (name,))

        (num, ) = cursor.fetchone()

        if num == 0:
            cursor.execute("CREATE ROLE \"{0:s}\"".format(name))

        if group is not None:
            query = "GRANT {0:s} TO {1:s}".format(group, name)
            cursor.execute(query)

        conn.commit()


def create_db(conn, name, owner):
    """
    Create a new database using template0 with UTF8 encoding.
    """
    query = "SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname = %s"

    create_query = sql.SQL(
        "CREATE DATABASE {} WITH ENCODING='UTF8' OWNER={} TEMPLATE template0"
    ).format(sql.Identifier(name), sql.Identifier(owner))

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (name,))

        (num, ) = cursor.fetchone()

        if num == 0:
            cursor.execute(create_query)

    conn.commit()


def show_table_data(conn, table: sql.Identifier):
    query = sql.SQL("SELECT * FROM {}").format(table)

    with conn.cursor() as cursor:
        cursor.execute(query)

        rows = cursor.fetchall()

        column_names = [column [0] for column in cursor.description]

    column_align = [">"] * len(column_names)
    column_sizes = ["max"] * len(column_names)

    for line in render_table(column_names, column_align, column_sizes, rows):
        print(line)
