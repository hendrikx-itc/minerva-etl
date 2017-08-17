# -*- coding: utf-8 -*-
"""
Helper functions for the directory schema.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing
import re
import StringIO

import itertools

from minerva.util import identity, k, head
from minerva.directory.basetypes import DataSource, Entity, EntityType
from minerva.db.query import Table, Column
from minerva.db.error import translate_postgresql_exceptions


MATCH_ALL = re.compile(".*")


class InvalidNameError(Exception):
    """
    Exception raised in case of invalid name.
    """
    pass


class NoSuchRelationTypeError(Exception):
    """
    Exception raised when no matching relation type is found.
    """
    pass


create_datasource = DataSource.create

get_datasource_by_id = DataSource.get

get_datasource = DataSource.get_by_name

name_to_datasource = DataSource.from_name

create_entitytype = EntityType.create

get_entitytype_by_id = EntityType.get

get_entitytype = EntityType.get_by_name

name_to_entitytype = EntityType.from_name


def get_child_ids(cursor, base_entity, entitytype):
    """
    Return child ids for entitytype related to base_entity.
    """
    query = (
        "SELECT id FROM directory.entity "
        "WHERE entitytype_id = %s AND dn LIKE %s")

    args = (entitytype.id, base_entity.dn + ",%")

    cursor.execute(query, args)

    return (entity_id for entity_id, in cursor.fetchall())


def get_related_entity_ids(conn, base_entity, entitytype):
    """
    Return related entity ids for entitytype related to base_entity.
    """
    related_entity_ids = []

    source_entitytype = get_entitytype_by_id(conn, base_entity.entitytype_id)

    relation_tablename = "{}->{}".format(source_entitytype.name,
                                         entitytype.name)

    query = (
        "SELECT target_id FROM relation.\"{0}\" "
        "JOIN directory.entity e ON e.id = target_id AND e.entitytype_id = %s "
        "WHERE source_id = %s").format(relation_tablename)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (entitytype.id, base_entity.id))

        for target_id, in cursor.fetchall():
                related_entity_ids.append(target_id)

    return related_entity_ids


def get_related_entitytypes(conn, entity_type_name):
    """
    Return list of tuples (entitytype_id, entitytype_name) for a specific
    entity type
    """
    query = (
        "SELECT DISTINCT t_et.id, t_et.name "
        "FROM directory.entity s "
        "JOIN directory.entitytype s_et ON s_et.id = s.entitytype_id "
        "AND s_et.name = %s "
        "JOIN relation.all r ON r.source_id = s.id "
        "JOIN directory.entity t ON r.target_id = t.id "
        "JOIN directory.entitytype t_et ON t_et.id = t.entitytype_id")

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (entity_type_name,))
        return cursor.fetchall()


def get_filtered_relations(conn, relation_type, filter=(MATCH_ALL, MATCH_ALL)):
    """
    Return dict of related entity ids.

    :param relation_type: tuple specifying source and target entity type
        (source_entitytype_name, target_entitytype_name)
    :param filter: tuple specifying filter for source and/or target entities
        (source_dn_regex, target_dn_regex)
    """
    source_entitytype_name, target_entitytype_name = relation_type
    relationtype_name = "{}->{}".format(source_entitytype_name,
                                        target_entitytype_name)

    try:
        get_relationtype_id(conn, relationtype_name)
    except NoSuchRelationTypeError:
        query = (
            "SELECT s.id, s.dn, t.id, t.dn "
            "FROM relation.all r "
            "JOIN directory.entity s ON s.id = r.source_id "
            "JOIN directory.entitytype et_s ON et_s.id = s.entitytype_id  "
            "AND et_s.name = %s "
            "JOIN directory.entity t ON t.id = r.target_id "
            "JOIN directory.entitytype et_t ON et_t.id = t.entitytype_id "
            "AND et_t.name = %s")

        args = relation_type
    else:
        full_table_name = "relation.\"{}\"".format(relationtype_name)

        query = (
            "SELECT r.source_id, s.dn, r.target_id, t.dn "
            "FROM {} r "
            "JOIN directory.entity s ON s.id = r.source_id "
            "JOIN directory.entity t on t.id = r.target_id").format(
            full_table_name)

        args = tuple()

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)
        rows = cursor.fetchall()

    def predicate((s_id, s_dn, t_id, t_dn)):
        return bool(filter[0].match(s_dn) and filter[1].match(t_dn))

    relations = {}

    for s_id, s_dn, t_id, t_dn in itertools.ifilter(predicate, rows):
        relations.setdefault(s_id, []).append(t_id)

    return relations


def get_relations(cursor, relation_type_name):
    """
    Return dict of related entity ids specified by relation_type_name
    """
    table = Table("relation", relation_type_name)
    query = table.select([Column("source_id"), Column("target_id")])
    query.execute(cursor)
    relations = {}
    for (s_id, t_id) in cursor.fetchall():
        relations.setdefault(s_id, []).append(t_id)

    return relations


def get_relationtype_id(conn, name):
    query = "SELECT id FROM relation.type WHERE name = %s"
    args = (name,)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)

        if cursor.rowcount > 0:
            relationtype_id, = cursor.fetchone()

            return relationtype_id
        else:
            raise NoSuchRelationTypeError()


get_entity_by_id = Entity.get


def none_or(if_none=k(None), if_value=identity):
    def fn(value):
        if value is None:
            return if_none()
        else:
            return if_value(value)

    return fn


get_entity = Entity.get_by_dn

create_entity = Entity.create_from_dn


def make_entitytaglinks(conn, entity_ids, tag_names):
    """
    Create new entity tag links
    :param conn: A psycopg2 connection to the Minerva database.
    :param entity_ids: List of entity id's
    :param tag_names: List of Entity Tag names to be linked to entities.
    """
    tags = [create_entitytag(conn, tagname) for tagname in tag_names]

    try:
        with closing(conn.cursor()) as cursor:
            query = (
                "CREATE TEMPORARY TABLE temp_entitytaglink "
                "(LIKE directory.entitytaglink) ON COMMIT DROP")

            cursor.execute(query)

            for tag in tags:
                # Copying data to temporary table
                data = StringIO.StringIO()
                data.writelines(("{0:d}\t{1:d}\n".format(tag.id, entity_id)
                                 for entity_id in entity_ids))
                data.seek(0)
                query = "COPY temp_entitytaglink FROM STDIN"
                cursor.copy_expert(query, data)

            # Insert from Temporary to Real table
            query = (
                "INSERT INTO directory.entitytaglink "
                "(entitytag_id, entity_id) "
                "SELECT t.entitytag_id, t.entity_id "
                "FROM temp_entitytaglink t "
                "LEFT JOIN directory.entitytaglink etl ON "
                "etl.entity_id = t.entity_id and "
                "etl.tag_id = t.tag_id "
                "WHERE etl.entity_id IS NULL")

            cursor.execute(query)

    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()


@translate_postgresql_exceptions
def dns_to_entity_ids(cursor, dns):
    cursor.callproc("directory.dns_to_entity_ids", (dns,))

    return map(head, cursor.fetchall())


dn_to_entity = Entity.from_dn
