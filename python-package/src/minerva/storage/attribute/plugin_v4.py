# -* -coding: utf - 8 -* -
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing
from datetime import datetime
import pytz

from minerva.directory.helpers import get_entity, get_entitytype_by_id
from minerva.storage.attribute.attributestore import AttributeStore
from minerva.storage.attribute.basetypes import RawDataPackage
from minerva.storage.attribute.storage import refine_data_rows
from minerva.storage.attribute.retrieve import retrieve, retrieve_current, \
    retrieve_attributes_for_entity
from minerva.storage.attribute.helpers import get_attribute_by_id


class DeltaPlugin(object):
    RawDataPackage = RawDataPackage

    def __init__(self, conn):
        self.conn = conn

    def store(self, datasource, entitytype, timestamp, attribute_names,
              data_rows):
        attributestore = AttributeStore(datasource, entitytype)
        attributestore.store(self.conn, attribute_names, timestamp, data_rows)

    def retrieve_attributes_for_entity(self, entity_id, attributes):
        return retrieve_attributes_for_entity(self.conn, entity_id, attributes)

    def retrieve(self, datasource, entitytype, attribute_names, entities,
                 timestamp=None):
        attributestore = AttributeStore(datasource, entitytype)

        return retrieve(self.conn, attributestore.table, attribute_names,
                        entities, timestamp)

    def retrieve_current(self, datasource, entitytype, attribute_names,
                         entities, limit=None):

        attributestore = AttributeStore(datasource, entitytype)

        return retrieve_current(self.conn, attributestore.table,
                                attribute_names, entities, limit)

    def store_raw(self, datasource, raw_datapackage):
        if not raw_datapackage.is_empty():
            refined_data_rows = self.refine_data_rows(raw_datapackage.rows)

            self.conn.commit()

            naive_timestamp = datetime.strptime(raw_datapackage.timestamp,
                                                "%Y-%m-%dT%H:%M:%S")
            timestamp = pytz.utc.localize(naive_timestamp)

            dn = raw_datapackage.rows[0][0]
            entity = get_entity(self.conn, dn)
            entitytype = get_entitytype_by_id(self.conn, entity.entitytype_id)

            self.store(datasource, entitytype, timestamp,
                       raw_datapackage.attribute_names, refined_data_rows)

    def refine_data_rows(self, raw_data_rows):
        with closing(self.conn.cursor()) as cursor:
            return refine_data_rows(cursor, raw_data_rows)

    def get_attribute_by_id(self, attribute_id):
        return get_attribute_by_id(self.conn, attribute_id)
