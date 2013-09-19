# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.db.query import Table

from minerva.directory.basetypes import DataSource, EntityType

from minerva.storage.delta.tables import get_table_name, get_table_name_curr, \
		SCHEMA


class RawDataPackage(object):
	def __init__(self, timestamp, attribute_names, rows):
		self.timestamp = timestamp
		self.attribute_names = attribute_names
		self.rows = rows

	def get_entitytype_name(self):
		if self.rows:
			first_dn = self.rows[0][0]

			return entitytype_name_from_dn(first_dn)

	def get_key(self):
		return self.timestamp, self.get_entitytype_name()

	def as_tuple(self):
		"""
		Return the legacy tuple (timestamp, attribute_names, rows)
		"""
		return self.timestamp, self.attribute_names, self.rows

	def is_empty(self):
		return len(self.rows) == 0


class AttributeStore(object):
	def __init__(self, datasource, entitytype):
		self.datasource = datasource
		self.entitytype = entitytype

		if isinstance(datasource, str):
			datasource_name = datasource
		elif isinstance(datasource, DataSource):
			datasource_name = datasource.name

		if isinstance(entitytype, str):
			entitytype_name = entitytype
		elif isinstance(entitytype, EntityType):
			entitytype_name = entitytype.name

		table_name = "{0}_{1}".format(datasource_name, entitytype_name).lower()
		table_name = get_table_name(datasource, entitytype)
		table_curr_name = "{}_curr".format(table_name)

		self.table = Table(SCHEMA, table_name)
		self.table_curr = Table(SCHEMA, table_curr_name)


class Attribute(object):
	def __init__(self, id, name, description, datasource_id, entitytype_id):
		self.id = id
		self.name = name
		self.description = description
		self.datasource_id = datasource_id
		self.entitytype_id = entitytype_id

	def __repr__(self):
		return "<Attribute({0}/{1}/{2})>".format(self.name, self.datasource_id,
			self.entitytype_id)

	def __str__(self):
		return self.name


class AttributeTag(object):
	def __init__(self, id, name):
		self.id = id
		self.name = name

	def __repr__(self):
		return "<AttributeTag({0})>".format(self.name)

	def __str__(self):
		return self.name
