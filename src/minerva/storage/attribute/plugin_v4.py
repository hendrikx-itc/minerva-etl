# -* -coding: utf - 8 -* -
"""Provides the AttributePlugin class."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import json
from contextlib import closing

from minerva.directory.entitytype import EntityType
from minerva.storage.attribute.attribute import Attribute
from minerva.storage.attribute.attributestore import AttributeStore
from minerva.storage.attribute.datapackage import DataPackage
from minerva.storage.attribute.rawdatapackage import RawDataPackage
from minerva.storage.attribute.retrieve import retrieve, retrieve_current, \
    retrieve_attributes_for_entity


class AttributePlugin(object):
    RawDataPackage = RawDataPackage

    def __init__(self, conn):
        self.conn = conn

    def store(self, datasource, entitytype, datapackage):
        attributes = datapackage.deduce_attributes()

        with closing(self.conn.cursor()) as cursor:
            attribute_store = AttributeStore.from_attributes(
                cursor, datasource, entitytype, attributes)

        self.conn.commit()

        attribute_store.store_txn(datapackage).run(self.conn)

    def retrieve_attributes_for_entity(self, entity_id, attributes):
        return retrieve_attributes_for_entity(self.conn, entity_id, attributes)

    def retrieve(self, datasource, entitytype, attribute_names, entities,
                 timestamp=None):
        attributestore = AttributeStore(datasource, entitytype)

        return retrieve(self.conn, attributestore.table, attribute_names,
                        entities, timestamp)

    def retrieve_current(self, datasource, entitytype, attribute_names,
                         entities):

        attribute_store = AttributeStore(datasource, entitytype)

        return retrieve_current(self.conn, attribute_store.curr_table,
                                attribute_names, entities)

    def store_raw(self, datasource, raw_data_package):
        if not raw_data_package.is_empty():
            with closing(self.conn.cursor()) as cursor:
                data_package = raw_data_package.refine(cursor)

            self.conn.commit()

            dn = raw_data_package.rows[0][0]
            entity = get_entity(self.conn, dn)
            entity_type = EntityType.get(entity.entitytype_id)(cursor)

            self.store(datasource, entity_type, data_package)

    def get_attribute_by_id(self, attribute_id):
        with closing(self.conn.cursor()) as cursor:
            return Attribute.get(cursor, attribute_id)

    @staticmethod
    def load_rawdatapackage(stream):
        return RawDataPackage.from_dict(json.load(stream))

    @staticmethod
    def load_datapackage(stream):
        return DataPackage.from_dict(json.load(stream))
