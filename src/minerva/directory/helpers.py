# -*- coding: utf-8 -*-
"""
Helper functions for the directory schema.
"""
from contextlib import closing
import re
from io import StringIO

from minerva.util import identity, k, fst
from minerva.db.error import translate_postgresql_exceptions
from minerva.directory.distinguishedname import entity_type_name_from_dn


MATCH_ALL = re.compile(".*")


def dns_to_entity_ids(cursor, dns):
    return aliases_to_entity_ids(cursor, 'dn', dns, entity_type_name_from_dn(dns[0]))


@translate_postgresql_exceptions
def aliases_to_entity_ids(cursor, namespace: str, aliases: list, entity_type: str):
    cursor.callproc("alias_directory.aliases_to_entity_ids", (namespace, aliases, entity_type))

    return list(map(fst, cursor.fetchall()))


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


def get_child_ids(cursor, base_entity, entity_type):
    """
    Return child ids for entitytype related to base_entity.
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
