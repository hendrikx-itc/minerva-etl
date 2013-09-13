# -* -coding: utf - 8 -* -
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from datetime import datetime
from contextlib import closing

import pytz

from minerva.directory.helpers import get_entity, get_entitytype_by_id

from minerva.storage.attribute.datapackage import DataPackage
from minerva.storage.attribute.attribute import Attribute
from minerva.storage.attribute.attributestore import AttributeStore
from minerva.storage.attribute.storage import refine_data_rows
from minerva.storage.attribute.retrieve import retrieve, retrieve_current, \
    retrieve_attributes_for_entity
from minerva.storage.attribute.helpers import get_attribute_by_id


class DeltaPlugin(object):
    def __init__(self, conn):
        self.conn = conn

    def store(self, datasource, entitytype, timestamp, attribute_names, rows):
        datapackage = DataPackage(timestamp, attribute_names, rows)

        attributes = [Attribute(name) for name in attribute_names]

        with closing(self.conn.cursor()) as cursor:
            attributestore = AttributeStore.from_attributes(
                cursor, datasource, entitytype, attributes)

        self.conn.commit()

        attributestore.store(datapackage).run(self.conn)

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
                                attribute_names, entities)

    def store_raw(self, datasource, timestamp_str, attribute_names,
                  raw_data_rows):
        if len(raw_data_rows) > 0:
            refined_data_rows = self.refine_data_rows(raw_data_rows)

            naive_timestamp = datetime.strptime(timestamp_str,
                                                "%Y-%m-%dT%H:%M:%S")
            timestamp = pytz.utc.localize(naive_timestamp)

            dn = raw_data_rows[0][0]
            entity = get_entity(self.conn, dn)
            entitytype = get_entitytype_by_id(self.conn, entity.entitytype_id)

            self.store(datasource, entitytype, timestamp, attribute_names,
                       refined_data_rows)

    def refine_data_rows(self, raw_data_rows):
        return refine_data_rows(self.conn, raw_data_rows)

    def get_attribute_by_id(self, attribute_id):
        return get_attribute_by_id(self.conn, attribute_id)
