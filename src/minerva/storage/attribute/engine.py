# -* -coding: utf - 8 -* -
"""Provides the AttributePlugin class."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import json
from contextlib import closing

from minerva.directory.helpers import get_entity, get_entitytype_by_id
from minerva.storage.attribute.attribute import Attribute
from minerva.storage.attribute.attributestore import AttributeStore
from minerva.storage.attribute.datapackage import DataPackage
from minerva.storage.attribute.retrieve import retrieve, retrieve_current, \
    retrieve_attributes_for_entity


class AttributeEngine():
    def __init__(self, conn):
        self.conn = conn

    def store(self, data_source, data_package):
        attribute_descriptors = data_package.deduce_attributes()

        with closing(self.conn.cursor()) as cursor:
            entity_type = data_package.get_entity_type(cursor)

            attribute_store = AttributeStore.from_attributes(
                data_source, entity_type, attribute_descriptors
            )(cursor)

        self.conn.commit()

        attribute_store.store_txn(data_package).run(self.conn)

    def retrieve_attributes_for_entity(self, entity_id, attributes):
        return retrieve_attributes_for_entity(self.conn, entity_id, attributes)

    def retrieve(
            self, datasource, entitytype, attribute_names, entities,
            timestamp=None):
        attributestore = AttributeStore(datasource, entitytype)

        return retrieve(
            self.conn, attributestore.table, attribute_names, entities,
            timestamp
        )

    def retrieve_current(
            self, datasource, entitytype, attribute_names, entities):

        attributestore = AttributeStore(datasource, entitytype)

        return retrieve_current(
            self.conn, attributestore.curr_table, attribute_names, entities)

    def get_attribute_by_id(self, attribute_id):
        with closing(self.conn.cursor()) as cursor:
            return Attribute.get(cursor, attribute_id)

    @staticmethod
    def load_datapackage(stream):
        return DataPackage.from_dict(json.load(stream))
