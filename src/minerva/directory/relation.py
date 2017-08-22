# -*- coding: utf-8 -*-
import StringIO
from contextlib import closing
from functools import partial

from minerva.directory.helpers import get_relationtype_id, \
    NoSuchRelationTypeError
from minerva.util import swap
from minerva.db.util import is_unique

__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2008-2017 Hendrikx-ITC B.V.
Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


def get_relation_name(conn, source_entitytype_name, target_entitytype_name):
    relation_name = None

    if source_entitytype_name is None or target_entitytype_name is None:
        return None

    relationtype_name = "{}->{}".format(source_entitytype_name,
                                        target_entitytype_name)
    query = "SELECT name FROM relation.type WHERE lower(name) = lower(%s)"

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (relationtype_name,))
        if cursor.rowcount > 0:
            relation_name, = cursor.fetchone()
        else:
            relation_name = None

    return relation_name


def add_relations(conn, relations, relationtype_name,
                  relationtype_cardinality=None):
    """
    Update relation.<relationtype_name> table
    :param conn: Minerva database connection
    :param relations: iterable of with items like (source_id, target_id)
    :param relationtype_name
    :param relationtype_cardinality
    """
    try:
        relationtype_id = get_relationtype_id(conn, relationtype_name)
    except NoSuchRelationTypeError:
        relationtype_id = create_relationtype(conn, relationtype_name,
                                              relationtype_cardinality)

    _f = StringIO.StringIO()

    for source_id, target_id in relations:
        _f.write("{0}\t{1}\t{2}\n".format(source_id, target_id,
                                          relationtype_id))

    _f.seek(0)

    tmp_table = "tmp_relation"

    with closing(conn.cursor()) as cursor:

        query = (
            "CREATE TEMPORARY TABLE \"{0}\" "
            "(LIKE relation.all)".format(tmp_table))

        cursor.execute(query)

        query = (
            "COPY \"{0}\" (source_id, target_id, type_id) "
            "FROM STDIN".format(tmp_table))

        cursor.copy_expert(query, _f)

        query = (
            "INSERT INTO relation.\"{0}\" (source_id, target_id, type_id) "
            "SELECT tmp.source_id, tmp.target_id, tmp.type_id "
            "FROM \"{1}\" tmp "
            "LEFT JOIN relation.\"{0}\" r "
            "ON r.source_id = tmp.source_id "
            "AND r.target_id = tmp.target_id "
            "WHERE r.source_id IS NULL ".format(relationtype_name, tmp_table))

        cursor.execute(query)

        cursor.execute("DROP TABLE {0}".format(tmp_table))


def create_relationtype(conn, name, cardinality=None):
    if cardinality is not None:
        query = (
            "INSERT INTO relation.type (name, cardinality) "
            "VALUES (%s, %s) "
            "RETURNING id")
        args = (name, cardinality)
    else:
        query = "INSERT INTO relation.type (name) VALUES (%s) RETURNING id"
        args = (name,)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)

        relationtype_id, = cursor.fetchone()

    return relationtype_id


def flush_relations(conn, relationtype_name):
    query = "TRUNCATE TABLE relation.\"{}\"".format(relationtype_name)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)


def reverse_relations(relations):
    return map(swap, relations)


def get_table_from_type_id(conn, relationtype_id):
    """
    Return relation table name from relation type id
    """
    with closing(conn.cursor()) as cursor:
        cursor.execute("SELECT name FROM relation.type WHERE id = %s",
                       (relationtype_id,))

        table_name, = cursor.fetchone()

    return table_name


def is_one_to_one(conn, relationtype_name):
    """
    Returns True when relation type is one to one, otherwise False.
    """
    unique = partial(is_unique, conn, "relation", relationtype_name)
    source_unique = unique("source_id")
    target_unique = unique("target_id")

    return source_unique and target_unique


def is_one_to_many(conn, relationtype_name):
    """
    Returns True when relation type is one to many, otherwise False
    """
    return is_unique(conn, "relation", relationtype_name, "source_id")


def is_many_to_one(conn, relationtype_name):
    """
    Returns True when relation type is many to one, otherwise False
    """
    return is_unique(conn, "relation", relationtype_name, "target_id")
