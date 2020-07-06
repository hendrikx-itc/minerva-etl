# -*- coding: utf-8 -*-
"""
Helper functions for the directory schema.
"""
import re
from typing import List

from minerva.directory import Entity, EntityType
from psycopg2 import sql

from minerva.util import identity, k, fst
from minerva.db.error import translate_postgresql_exceptions
from minerva.directory.distinguishedname import entity_type_name_from_dn


MATCH_ALL = re.compile(".*")


def dns_to_entity_ids(cursor, dns: List[str]) -> List[int]:
    return aliases_to_entity_ids(
        cursor, 'dn', dns, entity_type_name_from_dn(dns[0])
    )


@translate_postgresql_exceptions
def aliases_to_entity_ids(cursor, namespace: str, aliases: list, entity_type: str) -> List[int]:
    cursor.callproc(
        "alias_directory.aliases_to_entity_ids", (namespace, aliases, entity_type)
    )

    return list(map(fst, cursor.fetchall()))


def names_to_entity_ids(cursor, entity_type: str, names: list) -> List[int]:
    """
    Map names to entity ID's, create any missing entities, and return the
    corresponding entity ID's.

    :param cursor: psycopg2 cursor
    :param entity_type: case insensitive name of the entity type
    :param names: names of entities for which to return the ID's
    :return:
    """
    # First get the correct casing for the entity type name, because the table
    # name uses that specific casing.
    query = sql.SQL(
        'SELECT name FROM directory.entity_type WHERE lower(name) = %s'
    )

    cursor.execute(query, (entity_type.lower(),))

    entity_type_name, = cursor.fetchone()

    query = sql.SQL(
        'WITH lookup_list AS (SELECT unnest(ARRAY[%s]::text[]) AS name) '
        'SELECT l.name, e.id FROM lookup_list l '
        'LEFT JOIN entity.{} e ON l.name = e.name '
    ).format(sql.Identifier(entity_type_name))

    unmapped_names = []
    entity_ids = []

    cursor.execute(query, (names,))

    for name, entity_id in cursor.fetchall():
        if entity_id is None:
            unmapped_names.append(name)
        else:
            entity_ids.append(entity_id)

    if len(unmapped_names) > 0:
        entity_ids.extend(
            create_entities_from_names(cursor, entity_type_name, unmapped_names)
        )

    return entity_ids


def create_entities_from_names(cursor, entity_type: str, names: list) -> List[int]:
    entity_ids = []

    insert_query = sql.SQL(
        'INSERT INTO entity.{}(name) '
        'VALUES (%s) '
        'ON CONFLICT DO NOTHING '
        'RETURNING id'
    ).format(sql.Identifier(entity_type))

    for name in names:
        cursor.execute(insert_query, (name,))

        entity_id, = cursor.fetchone()

        entity_ids.append(entity_id)

    return entity_ids


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


def get_child_ids(cursor, base_entity: Entity, entity_type: EntityType) -> List[int]:
    """
    Return child ids for entity_type related to base_entity.
    """
    query = (
        "SELECT id FROM directory.entity "
        "WHERE entitytype_id = %s AND name LIKE %s")

    args = (entity_type.id, base_entity.name + ",%")

    cursor.execute(query, args)

    return (entity_id for entity_id, in cursor.fetchall())


def none_or(if_none=k(None), if_value=identity):
    def fn(value):
        if value is None:
            return if_none()
        else:
            return if_value(value)

    return fn
