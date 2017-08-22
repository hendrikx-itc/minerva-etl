# -*- coding: utf-8 -*-
from minerva.directory.distinguishedname import entitytype_name_from_dn
from minerva.directory.basetypes import EntityType

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class EntityRef(object):

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

    def get_entitytype(self, cursor):
        """
        Return the entitytype corresponding to the referenced entity.
        """
        raise NotImplementedError()


class EntityIdRef(EntityRef):

    def __init__(self, entity_id):
        self.entity_id = entity_id

    def to_argument(self):
        return "%s", self.entity_id

    def get_entitytype(self, cursor):
        cursor.execute(
            "SELECT entitytype_id FROM directory.entity WHERE id = %s", (
                self.entity_id,))

        if cursor.rowcount > 0:
            entitytype_id, = cursor.fetchone()

            return EntityType.get(cursor, entitytype_id)
        else:
            raise Exception(
                "no entity found with id {}".format(self.entity_id))


class EntityDnRef(EntityRef):

    def __init__(self, dn):
        self.dn = dn

    def to_argument(self):
        return "(directory.dn_to_entity(%s)).id", self.dn

    def get_entitytype(self, cursor):
        return EntityType.get_by_name(cursor, entitytype_name_from_dn(self.dn))
