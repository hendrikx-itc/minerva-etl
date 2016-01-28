# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.directory.helpers import dns_to_entity_ids
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
    def __init__(self, entity_id):
        self.entity_id = entity_id

    def to_argument(self):
        return "%s", self.entity_id

    def get_entity_type(self, cursor):
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
    def __init__(self, dn):
        self.dn = dn

    def to_argument(self):
        return "(directory.dn_to_entity(%s)).id", self.dn

    def get_entity_type(self, cursor):
        return EntityType.get_by_name(entity_type_name_from_dn(self.dn))(cursor)

    @classmethod
    def map_to_entity_ids(cls, entity_refs):
        def f(cursor):
            return dns_to_entity_ids(cursor, entity_refs)

        return f
