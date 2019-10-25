# -*- coding: utf-8 -*-
"""
Alias related functions
"""
from io import StringIO
from contextlib import closing


class NoSuchAliasType(Exception):
    """
    Exception raised when no matching alias type is found
    """
    pass


def store(conn, aliases, type_name):
    """
    Store aliases of specified type in directory.alias
    :param conn: Minerva database connection
    :param aliases: iterable with items like (entity_id, alias)
    :param type_name: alias type_name
    """
    try:
        type_id = get_type_id(conn, type_name)
    except NoSuchAliasType:
        type_id = create_type(conn, type_name)

    _f = StringIO()

    for entity_id, alias in aliases:
        _f.write("{0}\t{1}\t{2}\n".format(entity_id, alias, type_id))

    _f.seek(0)

    tmp_table = "tmp_alias"

    with closing(conn.cursor()) as cursor:

        query = (
            "CREATE TEMPORARY TABLE \"{0}\" "
            "(LIKE directory.alias) ON COMMIT DROP".format(tmp_table)
        )

        cursor.execute(query)

        query = (
            "COPY \"{0}\" (entity_id, name, type_id) "
            "FROM STDIN".format(tmp_table)
        )

        cursor.copy_expert(query, _f)

        query = (
            "INSERT INTO directory.alias (entity_id, name, type_id) ON CONFLICT DO NOTHING"
            "SELECT tmp.entity_id, tmp.name, tmp.type_id FROM \"{0}\" tmp "
            "LEFT JOIN directory.alias a "
            "ON a.entity_id = tmp.entity_id AND a.type_id = tmp.type_id "
            "WHERE a.entity_id IS NULL ".format(tmp_table)
        )

        cursor.execute(query)


def create_type(conn, type_name):
    """
    Create alias type and return type id
    """
    query = "INSERT INTO directory.alias_type (name) VALUES (%s) RETURNING id"

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (type_name,))
        id, = cursor.fetchone()

    return id


def get_type_id(conn, type_name):
    """
    Return id of alias type
    """
    query = "SELECT id FROM directory.alias_type WHERE name = %s"

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (type_name,))

        if cursor.rowcount > 0:
            id, = cursor.fetchone()
            return id
        else:
            raise NoSuchAliasType


def flush(conn, type_name):
    """
    Delete aliases of specific type
    """
    try:
        type_id = get_type_id(conn, type_name)
    except NoSuchAliasType:
        return

    query = "DELETE FROM directory.alias WHERE type_id = %s"

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (type_id,))


def get_entity_id_by_alias(conn, type_name, alias):
    query = "SELECT entity_id FROM alias.{} WHERE alias = %s".format(alias)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (alias,))

        (entity_id,) = cursor.fetchone()

    return entity_id
