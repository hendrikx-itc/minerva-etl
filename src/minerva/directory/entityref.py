# -*- coding: utf-8 -*-
from typing import Tuple

from minerva.directory.helpers import aliases_to_entity_ids, \
    dns_to_entity_ids, names_to_entity_ids
from minerva.directory.distinguishedname import entity_type_name_from_dn
from minerva.directory import EntityType


class EntityRef:
    """
    The abstract base class for types representing a reference to a single
    entity.
    """
    def to_argument(self):
        """
        Return a tuple (placeholder, value) that can be used in queries:

        cursor.execute("SELECT {}".format(placeholder), (value,))
        """
        raise NotImplementedError()

    def get_entity_type(self, cursor):
        """
        Return the entity type corresponding to the referenced entity.
        """
        raise NotImplementedError()

    @classmethod
    def map_to_entity_ids(cls, entity_refs):
        raise NotImplementedError()


class EntityIdRef(EntityRef):
    """
    A reference to an entity by its Id.
    """
    entity_id: int

    def __init__(self, entity_id: int):
        self.entity_id = entity_id

    def to_argument(self) -> Tuple[str, int]:
        return "%s", self.entity_id

    def get_entity_type(self, cursor) -> EntityType:
        return EntityType.get_by_entity_id(self.entity_id)(cursor)

    @classmethod
    def map_to_entity_ids(cls, entity_refs):
        def f(cursor):
            return entity_refs

        return f


class EntityDnRef(EntityRef):
    """
    A reference to an entity by its distinguished name.
    """
    dn: str
    alias: str

    def __init__(self, dn):
        self.alias = self.dn = dn

    def to_argument(self) -> Tuple[str, str]:
        return "(directory.dn_to_entity(%s)).id", self.dn

    def get_entity_type(self, cursor) -> EntityType:
        return EntityType.get_by_name(
            entity_type_name_from_dn(self.dn))(cursor)

    @classmethod
    def map_to_entity_ids(cls, entity_refs):
        def f(cursor):
            return dns_to_entity_ids(cursor, entity_refs)

        return f


def _create_alias_ref_class(alias_type: str, entity_type: str):
    if alias_type == 'dn':
        return EntityDnRef

    else:
        class EntityAliasRef(EntityRef):
            """
            A reference to an entity by an alias.
            """

            def __init__(self, alias):
                self.alias = alias

            def to_argument(self):
                return (
                    "(alias_directory.get_entity_by_alias(%s, %s)).id",
                    (alias_type, self.alias)
                )

            def get_entity_type(self, cursor):
                return alias_type

            @classmethod
            def map_to_entity_ids(cls, entity_refs):
                def f(cursor):
                    return aliases_to_entity_ids(
                        cursor, alias_type, entity_refs, entity_type
                    )

                return f

        return EntityAliasRef


_alias_ref_classes = {}  # alias_type, entity_type -> EntityAliasRef class


def entity_alias_ref_class(alias_type: str, entity_type: str):
    if alias_type not in _alias_ref_classes.keys():
        _alias_ref_classes[alias_type] = _create_alias_ref_class(
            alias_type, entity_type
        )

    return _alias_ref_classes[alias_type]


def _create_name_ref_class(entity_type: str):
    class EntityNameRef(EntityRef):
        """
        A reference to an entity by an alias.
        """

        def __init__(self, alias):
            self.alias = alias

        def to_argument(self):
            raise NotImplementedError

        def get_entity_type(self, cursor):
            return entity_type

        @classmethod
        def map_to_entity_ids(cls, entity_refs):
            def f(cursor):
                return names_to_entity_ids(
                    cursor, entity_type, entity_refs
                )

            return f

    return EntityNameRef


_name_ref_classes = {}  # entity_type -> EntityAliasRef class


def entity_name_ref_class(entity_type: str):
    if entity_type not in _name_ref_classes:
        _name_ref_classes[entity_type] = _create_name_ref_class(entity_type)

    return _name_ref_classes[entity_type]
