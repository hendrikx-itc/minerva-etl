# -* -coding: utf - 8 -* -
"""
Provides PostgreSQL specific storage functionality using delta.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""

from datetime import datetime
import pytz

from minerva.directory.helpers import get_entity, get_entitytype_by_id

from minerva_storage_delta.basetypes import AttributeStore
from minerva_storage_delta.storage import store, refine_data_rows
from minerva_storage_delta.retrieve import retrieve, retrieve_current, \
		retrieve_attributes_for_entity
from minerva_storage_delta.tables import resolve_table_name
from minerva_storage_delta.helpers import get_attribute_by_id


class DeltaPlugin(object):
	def __init__(self, conn):
		self.conn = conn

	def store(self, datasource, entitytype, timestamp, attribute_names, data_rows):
		attributestore = AttributeStore(datasource, entitytype)

		store(self.conn, attributestore, attribute_names, timestamp, data_rows)

	def retrieve_attributes_for_entity(self, entity_id, attributes):
		return retrieve_attributes_for_entity(self.conn, entity_id, attributes)

	def retrieve(self, datasource, entitytype, attribute_names, entities,
				timestamp=None):
		attributestore = AttributeStore(datasource, entitytype)

		return retrieve(self.conn, attributestore.table, attribute_names, entities,
				timestamp)

	def retrieve_current(self, datasource, entitytype, attribute_names, entities,
			limit=None):

		attributestore = AttributeStore(datasource, entitytype)

		return retrieve_current(self.conn, attributestore.table, attribute_names,
				entities)

	def store_raw(self, datasource, timestamp_str, attribute_names, raw_data_rows):
		if len(raw_data_rows) > 0:
			refined_data_rows = self.refine_data_rows(raw_data_rows)

			naive_timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
			timestamp = pytz.utc.localize(naive_timestamp)

			dn = raw_data_rows[0][0]
			entity = get_entity(self.conn, dn)
			entitytype = get_entitytype_by_id(self.conn, entity.entitytype_id)

			self.store(datasource, entitytype, timestamp, attribute_names,
					refined_data_rows)

	def refine_data_rows(self, raw_data_rows):
		return refine_data_rows(self.conn, raw_data_rows)

	def resolve_table_name(self, table_name):
		return resolve_table_name(table_name)

	def get_attribute_by_id(self, attribute_id):
		return get_attribute_by_id(self.conn, attribute_id)
