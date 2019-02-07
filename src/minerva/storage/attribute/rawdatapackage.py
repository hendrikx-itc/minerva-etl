# -*- coding: utf-8 -*-
"""Provides the RawDataPackage class."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.directory.distinguishedname import entity_type_name_from_dn
from minerva.directory.helpers import aliases_to_entity_ids
from minerva.storage.attribute.datapackage import DataPackage


class RawDataPackage(DataPackage):

    """
    A datapackage class with refining functionality.

    The RawDataPackage class just adds some helper functions to
    :class:`~minerva.storage.attribute.datapackage.DataPackage`.
    """

    def get_entitytype_name(self):
        """Return the entity type name from the first Distinguished Name."""
        if self.alias_type == 'dn':
            if self.rows:
                first_dn = self.rows[0][0]

                return entity_type_name_from_dn(first_dn)

        else:
            return self.alias_type

    def get_key(self):
        """Return key by which to merge this package with other packages."""
        return self.get_entitytype_name()

    def refine(self, cursor):
        """
        Return a DataPackage with 'refined' data of this package.

        This means that all distinguished names are translated to entity Ids.

        """
        aliases, timestamps, value_rows = zip(*self.rows)

        entity_ids = aliases_to_entity_ids(cursor, self.alias_type, (aliases), self.get_entitytype_name())

        rows = zip(entity_ids, timestamps, value_rows)
        return DataPackage(self.attribute_names, rows)
