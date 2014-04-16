# -* -coding: utf - 8 -* -
"""Provides the legacy AttributePlugin class."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing

from minerva.directory.helpers_v4 import get_entity, get_entitytype_by_id

from minerva.storage.attribute import datapackage as dp
from minerva.storage.attribute import rawdatapackage as rdp
from minerva.storage.attribute.attribute import Attribute
from minerva.storage.attribute.attributestore import AttributeStore
from minerva.storage.attribute.retrieve import retrieve, retrieve_current, \
    retrieve_attributes_for_entity


class AttributePlugin(object):
    DataPackage = dp.DataPackage
    RawDataPackage = rdp.RawDataPackage

    def __init__(self, conn):
        self.conn = conn

    def store(self, datasource, entitytype, datapackage):
        attributes = [Attribute(name) for name in datapackage.attribute_names]

        with closing(self.conn.cursor()) as cursor:
            attributestore = AttributeStore.from_attributes(
                cursor, datasource, entitytype, attributes)

        self.conn.commit()

        attributestore.store_txn(datapackage).run(self.conn)

    def retrieve_attributes_for_entity(self, entity_id, attributes):
        return retrieve_attributes_for_entity(self.conn, entity_id, attributes)

    def retrieve(self, datasource, entitytype, attribute_names, entities,
                 timestamp=None):
        attributestore = AttributeStore(datasource, entitytype)

        return retrieve(self.conn, attributestore.history_table, attribute_names,
                        entities, timestamp)

    def retrieve_current(self, datasource, entitytype, attribute_names,
                         entities, limit=None):

        attributestore = AttributeStore(datasource, entitytype)

        return retrieve_current(self.conn, attributestore.table,
                                attribute_names, entities)

    def store_raw(self, datasource, rawdatapackage):
        with closing(self.conn.cursor()) as cursor:
            datapackage = rawdatapackage.refine(cursor)

            dn = rawdatapackage.rows[0][0]
            entity = get_entity(cursor, dn)
            entitytype = get_entitytype_by_id(cursor, entity.entitytype_id)

        self.store(datasource, entitytype, datapackage)

    def get_attribute_by_id(self, attribute_id):
        with closing(self.conn.cursor()) as cursor:
            return Attribute.get(cursor, attribute_id)
