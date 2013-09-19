
# -*- coding: utf-8 -*-
"""
Provides PostgreSQL specific storage functionality using delta.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing

import psycopg2

from minerva.util import no_op
from minerva.db.postgresql import grant
from minerva.storage.generic import RecoverableError, NonRecoverableError


SCHEMA = "delta"


class InvalidDeltaTableName(Exception):
	pass


def get_table_name(datasource_name, entitytype_name):
	return "{0}_{1}".format(datasource_name, entitytype_name).lower()


def get_table_name_curr(datasource_name, entitytype_name):
	return  "{0}_curr".format(get_table_name(datasource_name, entitytype_name))


def resolve_table_name(table_name):
	"""
	Returns tuple (datasource_name, entitytype_name)
	"""
	try:
		parts = table_name.split("_")
		if parts[-1] == "curr":
			parts = parts[:-1]
		return (parts[0], "_".join(parts[1:]))
	except:
		raise InvalidDeltaTableName(table_name)


def create_table(conn, attributestore, attribute_names, data_types):
	columns = ['"{0}" {1}'.format(attribute_name, data_type)
		for attribute_name, data_type in zip(attribute_names, data_types)]

	if columns:
		attribute_columns_part = "{}, ".format(", ".join(columns))
	else:
		attribute_columns_part = ""

	hist_table_query = (
		"CREATE TABLE {0} "
		"(entity_id integer NOT NULL, "
		"\"timestamp\" timestamp with time zone NOT NULL, "
		"\"modified\" timestamp with time zone NOT NULL, "
		"hash character varying, "
		"{1}"
		"CONSTRAINT \"{2}_pkey\" PRIMARY KEY (entity_id, timestamp)) "
		"WITHOUT OIDS").format(attributestore.table.render(), attribute_columns_part, attributestore.table.name)

	alter_query = (
		"ALTER TABLE {} ALTER COLUMN modified "
		"SET DEFAULT CURRENT_TIMESTAMP").format(attributestore.table.render())

	index_query = (
		"CREATE INDEX \"idx_{0}\" ON {1} "
		"USING btree (modified)").format(attributestore.table.name, attributestore.table.render())

	trigger_query = (
		"CREATE TRIGGER update_modified_modtime BEFORE UPDATE ON {1} "
		"FOR EACH ROW EXECUTE PROCEDURE {0}.update_modified_column()").format(SCHEMA, attributestore.table.render())

	current_table_query = (
		"CREATE TABLE {0} "
		"(CONSTRAINT \"{1}_pkey\" PRIMARY KEY (entity_id)) "
		"INHERITS ({2})").format(attributestore.table_curr.render(), attributestore.table_curr.name, attributestore.table.render())

	owner_query = (
		"ALTER TABLE {} "
		"OWNER TO minerva_writer").format(attributestore.table.render())

	owner_query_curr = (
		"ALTER TABLE {} "
		"OWNER TO minerva_writer").format(attributestore.table_curr.render())

	with closing(conn.cursor()) as cursor:
		try:
			cursor.execute(hist_table_query)
			cursor.execute(alter_query)
			cursor.execute(index_query)
			cursor.execute(trigger_query)
			cursor.execute(current_table_query)
			cursor.execute(owner_query)
			cursor.execute(owner_query_curr)

			grant(conn, "TABLE", "SELECT", attributestore.table.render(), "minerva")
			grant(conn, "TABLE", "SELECT", attributestore.table_curr.render(), "minerva")
		except psycopg2.IntegrityError as exc:
			raise RecoverableError(str(exc), no_op)
		except psycopg2.ProgrammingError as exc:
			if exc.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
				raise RecoverableError(str(exc), no_op)
			else:
				raise NonRecoverableError("ProgrammingError({0}): {1}".format(
					exc.pgcode, exc.pgerror))
		else:
			conn.commit()
