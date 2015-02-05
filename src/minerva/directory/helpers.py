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
import warnings
import re
import StringIO

import itertools
import psycopg2.errorcodes

from minerva.db.error import UniqueViolation
from minerva.directory.distinguishedname import split_parts, type_indexes, \
    explode, implode
from minerva.directory import DataSource, Entity, EntityType
from minerva.directory import helpers_v4


MATCH_ALL = re.compile(".*")


class NoSuchEntityTypeError(Exception):
    """
    Exception raised when no matching EntityType is found.
    """
    pass


class NoSuchEntityError(Exception):
    """
    Exception raised when no matching Entity is found.
    """
    pass


class NoSuchDataSourceError(Exception):
    """
    Exception raised when no matching DataSource is found.
    """
    pass


class NoSuitablePluginError(Exception):
    """
    Exception raised when no plugins found.
    """
    pass


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


def get_datasource_by_id(conn, datasource_id):
    """
    Return the datasource with the specified Id.
    """
    with closing(conn.cursor()) as cursor:
        cursor.execute("SELECT name, timezone \
FROM directory.datasource WHERE id=%s", (datasource_id,))

        if cursor.rowcount == 1:
            name, timezone = cursor.fetchone()

            return DataSource(datasource_id, name, "", timezone)
        else:
            raise NoSuchDataSourceError(
                "No datasource with id {0}".format(datasource_id))


def get_datasource(conn, name):
    """
    Return the datasource with the specified name.
    """
    with closing(conn.cursor()) as cursor:
        cursor.execute("SELECT id, name, timezone \
FROM directory.datasource WHERE lower(name)=lower(%s)", (name,))

        if cursor.rowcount == 1:
            datasource_id, name, timezone = cursor.fetchone()

            return DataSource(datasource_id, name, "", timezone)
        else:
            raise NoSuchDataSourceError(
                "No datasource with name {0}".format(name))


def add_datasource(conn, name, description, timezone):
    """
    Add a new datasource if none exists with the same name.

    :param conn: A psycopg2 connection to the Minerva database.
    :param name: The identifying name of the data source.
    :param description: A short description.
    :param timezone: Timezone of the data originating from the data source.
    """
    warnings.warn("deprecated", DeprecationWarning)

    with closing(conn.cursor()) as cursor:
        cursor.execute(
            "SELECT id FROM directory.datasource WHERE name=%s",
            (name,)
        )

        if cursor.rowcount == 1:
            (datasource_id,) = cursor.fetchone()
        else:
            try:
                query = (
                    "INSERT INTO directory.datasource "
                    "(id, name, description, timezone) "
                    "VALUES (DEFAULT, %s, %s, %s) RETURNING id"
                )

                cursor.execute(query, (name, description, timezone))
            except psycopg2.Error as exc:
                conn.rollback()

                if exc.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                    cursor.execute(
                        "SELECT id FROM directory.datasource WHERE name=%s",
                        (name,)
                    )

                    (datasource_id,) = cursor.fetchone()
                else:
                    raise exc
            else:
                (datasource_id,) = cursor.fetchone()

                conn.commit()

        return DataSource(datasource_id, name, "", timezone)


def create_datasource(conn, name, description, timezone):
    """
    Create new datasource

    :param conn: A psycopg2 connection to the Minerva database.
    :param name: identifying name of data source.
    :param description: A short description.
    :param timezone: Timezone of data originating from data source.
    """
    if "_" in name:
        raise InvalidNameError(
            "Datasource name '{0}' contains underscores. "
            "This is not allowed.".format(name)
        )

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(
                "INSERT INTO directory.datasource "
                "(id, name, description, timezone) "
                "VALUES (DEFAULT, %s, %s, %s) RETURNING id",
                (name, description, timezone)
            )
        except psycopg2.Error as exc:
            conn.rollback()

            if exc.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                cursor.execute(
                    "SELECT id FROM directory.datasource "
                    "WHERE name=%s",
                    (name,)
                )

                (datasource_id,) = cursor.fetchone()
            else:
                raise exc
        else:
            (datasource_id,) = cursor.fetchone()

            conn.commit()

    return DataSource(datasource_id, name, "", timezone)


def create_entitytype(conn, name, description):
    """
    Create a new entity type and add it to the database.
    """
    query = (
        "INSERT INTO directory.entitytype (id, name, description) "
        "VALUES (DEFAULT, %s, %s) RETURNING id"
    )

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query, (name, description))
        except psycopg2.Error as exc:
            conn.rollback()

            if exc.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                raise UniqueViolation(
                    "Entity type with name {} already exists".format(name))
            else:
                raise exc
        else:
            (entitytype_id,) = cursor.fetchone()

            conn.commit()

            return EntityType(entitytype_id, name, description)


def get_entitytype_by_id(conn, entitytype_id):
    """
    Return the entity type matching the specified Id.

    :param conn: A psycopg2 database connection.
    :param entitytype_id: Id of the entity type to return.
    """
    query = (
        "SELECT name, description "
        "FROM directory.entitytype "
        "WHERE id=%s"
    )

    args = (entitytype_id,)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)

        if cursor.rowcount > 0:
            (name, description) = cursor.fetchone()

            return EntityType(entitytype_id, name, description)
        else:
            raise NoSuchEntityTypeError("No enititytype with id {0}".format(
                entitytype_id))


def get_entitytype(conn, name):
    """
    Return the entitytype with name `name`.
    """
    query = (
        "SELECT id, name, description "
        "FROM directory.entitytype "
        "WHERE lower(name) = lower(%s)"
    )

    args = (name,)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)

        if cursor.rowcount > 0:
            (entitytype_id, name, description) = cursor.fetchone()

            return EntityType(entitytype_id, name, description)
        else:
            raise NoSuchEntityTypeError("No entitytype with name {0}".format(
                name))


def get_parent_ids(conn, base_entity, entitytype):
    """
    Return parent ids for entitytype related to base_entity.
    """
    dn_parts = explode(base_entity.dn)

    indexes = type_indexes(dn_parts, entitytype.name)

    res = []
    for index in indexes:
        if index < len(dn_parts) - 1:  # I want parents! not base_entity.
            res.append(get_entity(conn, implode(dn_parts[:index + 1])).id)

    return res


def get_child_ids(conn, base_entity, entitytype):
    """
    Return child ids for entitytype related to base_entity.
    """
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            "SELECT id FROM directory.entity "
            "WHERE entitytype_id = %s AND dn LIKE %s",
            (entitytype.id, base_entity.dn + ",%")
        )

        return (entity_id for entity_id, in cursor.fetchall())


def get_related_entity_ids_for_relation(conn, base_entity_id, relation_name):
    """
    Return related entity ids for entitytype related to base_entity.
    """
    query = (
        "SELECT target_id FROM relation.\"{0}\" "
        "WHERE source_id = %s"
    ).format(relation_name)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (base_entity_id, ))

        return [target_id for target_id, in cursor.fetchall()]
    
    return []


def get_related_entity_ids(conn, base_entity, entitytype):
    """
    Return related entity ids for entitytype related to base_entity.
    """
    related_entity_ids = []

    source_entitytype = get_entitytype_by_id(conn, base_entity.entitytype_id)

    relation_tablename = "{}->{}".format(
        source_entitytype.name, entitytype.name
    )

    query = (
        "SELECT target_id FROM relation.\"{0}\" "
        "JOIN directory.entity e ON e.id = target_id AND e.entitytype_id = %s "
        "WHERE source_id = %s"
    ).format(relation_tablename)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (entitytype.id, base_entity.id))

        for target_id, in cursor.fetchall():
                related_entity_ids.append(target_id)

    return related_entity_ids


def get_related_entitytypes(conn, entity_type_name):
    """
    Return list of tuples (entitytype_id, entitytype_name) for a specific
    entity type.
    """
    query = (
        "SELECT DISTINCT t_et.id, t_et.name "
        "FROM directory.entity s "
        "JOIN directory.entitytype s_et ON s_et.id = s.entitytype_id "
        "AND s_et.name = %s "
        "JOIN relation.all r ON r.source_id = s.id "
        "JOIN directory.entity t ON r.target_id = t.id "
        "JOIN directory.entitytype t_et ON t_et.id = t.entitytype_id"
    )

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
            "AND et_t.name = %s"
        )

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


def get_relations(conn, relation_type):
    """
    Return dict of related entity ids.

    :param relation_type: tuple specifying source and target entity type
        (source_entitytype_name, target_entitytype_name)
    """
    source_entitytype_name, target_entitytype_name = relation_type

    relationtype_name = "{}->{}".format(
        source_entitytype_name, target_entitytype_name
    )

    try:
        get_relationtype_id(conn, relationtype_name)
    except NoSuchRelationTypeError:
        query = (
            "SELECT r.source_id, r.target_id "
            "FROM relation.all r "
            "JOIN directory.entity s ON s.id = r.source_id "
            "JOIN directory.entitytype et_s ON et_s.id = s.entitytype_id  "
            "AND et_s.name = %s "
            "JOIN directory.entity t ON t.id = r.target_id "
            "JOIN directory.entitytype et_t ON et_t.id = t.entitytype_id "
            "AND et_t.name = %s "
        )

        args = relation_type
    else:
        full_table_name = "relation.\"{}\"".format(relationtype_name)

        query = "SELECT source_id, target_id FROM {}".format(full_table_name)

        args = tuple()

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)

        rows = cursor.fetchall()

    relations = {}

    for (s_id, t_id) in rows:
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


def add_entitytype(conn, name, description=""):
    """
    Add a new entity type if none exists with the same name.
    """
    warnings.warn("deprecated", DeprecationWarning)

    args = (name,)

    with closing(conn.cursor()) as cursor:
        cursor.callproc("directory.get_entitytype_id", args)

        if cursor.rowcount == 1:
            (entitytype_id,) = cursor.fetchone()

            return EntityType(entitytype_id, name, description)
        else:
            return create_entitytype(conn, name, description)


def get_entity_by_id(conn, entity_id):
    """
    Return entity with specified distinguished name.
    """
    args = (entity_id,)

    with closing(conn.cursor()) as cursor:
        cursor.callproc("directory.getentitybyid", args)

        if cursor.rowcount == 1:
            (dn, entitytype_id, entity_name, parent_id) = cursor.fetchone()

            return Entity(entity_id, entity_name, entitytype_id, dn, parent_id)
        else:
            raise NoSuchEntityError("No entity with id {0}".format(entity_id))


def get_entity(conn, dn):
    """
    Return entity with specified distinguished name.
    """
    args = (dn,)

    with closing(conn.cursor()) as cursor:
        cursor.callproc("directory.getentitybydn", args)

        if cursor.rowcount == 1:
            row = cursor.fetchone()
            entity_id, entitytype_id, entity_name, parent_id = row

            return Entity(entity_id, entity_name, entitytype_id, dn, parent_id)
        else:
            raise NoSuchEntityError("No entity with dn {0}".format(dn))


def add_entity(conn, dn):
    """
    :param conn: A psycopg2 connection to a Minerva Directory database.
    :param dn: The distinguished name of the entity.
    """
    warnings.warn("deprecated", DeprecationWarning)

    with closing(conn.cursor()) as cursor:
        cursor.callproc("directory.dn_to_entity", (dn,))

        row = cursor.fetchone()
        id, first_appearance, name, entitytype_id, _dn, parent_id = row

    conn.commit()

    return Entity(id, name, entitytype_id, dn, parent_id)


def create_entity(conn, dn):
    """
    :param conn: A psycopg2 connection to a Minerva Directory database.
    :param dn: The distinguished name of the entity.
    """
    dnparts = split_parts(dn)

    if len(dnparts) == 0:
        raise Exception("Invalid DN: '{0}'".format(dn))

    with closing(conn.cursor()) as cursor:
        entity = helpers_v4.create_entity(cursor, dn)

    conn.commit()

    return entity


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
                "WHERE etl.entity_id IS NULL"
            )

            cursor.execute(query)

    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
