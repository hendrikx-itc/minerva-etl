# -*- coding: utf-8 -*-
"""
Helper functions for the directory schema.
"""
import re
from typing import List

from psycopg2 import sql

from minerva.util import identity, k, fst
from minerva.db.error import translate_postgresql_exceptions
from minerva.directory.distinguishedname import entity_type_name_from_dn


MATCH_ALL = re.compile(".*")


@translate_postgresql_exceptions
def aliases_to_entity_ids(cursor, namespace: str, aliases: list, entity_type: str) -> List[int]:
    cursor.callproc(
        "alias_directory.aliases_to_entity_ids", (namespace, aliases, entity_type)
    )

    return list(map(fst, cursor.fetchall()))


def names_to_entity_ids(cursor, entity_type: str, names: List[str]) -> List[int]:
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

    entity_ids = []

    cursor.execute(query, (names,))

    rows = cursor.fetchall()

    for name, entity_id in rows:
        if entity_id is None:
            entity_id = create_entity_from_name(cursor, entity_type_name, name)

        entity_ids.append(entity_id)

    return entity_ids


def create_entities_from_names(cursor, entity_type: str, names: list) -> List[int]:
    return [
        create_entity_from_name(cursor, entity_type, name)
        for name in names
    ]


def create_entity_from_name(cursor, entity_type: str, name: str) -> int:
    insert_query = sql.SQL(
        'INSERT INTO entity.{}(name) '
        'VALUES (%s) '
        'ON CONFLICT DO NOTHING '
        'RETURNING id'
    ).format(sql.Identifier(entity_type))

    cursor.execute(insert_query, (name,))

    if cursor.rowcount == 0:
        select_query = sql.SQL(
            'SELECT id FROM entity.{} WHERE name = %s'
        ).format(sql.Identifier(entity_type))

        cursor.execute(select_query, (name,))

        entity_id, = cursor.fetchone()
    else:
        entity_id, = cursor.fetchone()

    return entity_id


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


def none_or(if_none=k(None), if_value=identity):
    def fn(value):
        if value is None:
            return if_none()
        else:
            return if_value(value)

    return fn
