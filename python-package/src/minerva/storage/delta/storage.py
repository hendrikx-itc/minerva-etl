# -*- coding: utf-8 -*-
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
from contextlib import closing
import hashlib
from functools import partial
import logging
from traceback import format_exc

import psycopg2

from minerva.util import first
from minerva.directory.helpers import NoSuchEntityError, get_entity, \
	create_entity
from minerva.directory.helpers_v4 import dns_to_entity_ids
from minerva.storage.generic import MaxRetriesError, RecoverableError, \
	NonRecoverableError, create_column, extract_data_types, check_column_types
from minerva.storage.datatype import TYPE_ORDER
from minerva.storage.exceptions import DataError

from minerva_storage_delta.tables import create_table, SCHEMA

DATATYPE_MISMATCH_ERRORS = set((
	psycopg2.errorcodes.DATATYPE_MISMATCH,
	psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE,
	psycopg2.errorcodes.INVALID_TEXT_REPRESENTATION))

MAX_RETRIES = 10
INITIAL_TYPE = TYPE_ORDER[0]


class DataTypeMismatchError(Exception):
	pass


class NoSuchTableError(Exception):
	pass


class UndefinedColumnError(Exception):
	pass


class NoRecordError(Exception):
	"""
	A rather generic exception to indicate that no record was found.
	"""
	def __init__(self):
		super(NoRecordError, self).__init__()


def refine_data_rows(cursor, rows):
	dns, value_rows = zip(*rows)

	entity_ids = dns_to_entity_ids(cursor, list(dns))

	refined_value_rows = map(refine_values, value_rows)

	return zip(entity_ids, refined_value_rows)


def refine_values(raw_values):
	values = []

	for value in raw_values:
		if type(value) is tuple:
			joined = ",".join(value)

			if len(joined) > 0:
				values.append(joined)
			else:
				values.append(None)
		elif len(value) == 0:
			values.append(None)
		else:
			values.append(value)

	return values


def store(conn, attributestore, column_names, timestamp, data_rows):
	for entity_id, values in data_rows:
		retry = True
		attempt = 0

		while retry is True:
			retry = False
			attempt += 1

			if attempt > MAX_RETRIES:
				raise MaxRetriesError("Max retries ({0}) reached".format(MAX_RETRIES))
			try:
				store_row(conn, attributestore, column_names, timestamp, entity_id, values)
			except DataTypeMismatchError as err:
				conn.rollback()
				logging.debug(format_exc())
				data_types = extract_data_types([(entity_id, values)])
				check_column_types(conn, SCHEMA, attributestore.table.name,
						column_names, data_types)
				retry = True
			except RecoverableError as err:
				conn.rollback()
				logging.debug(format_exc())
				err.fix()
				retry = True
			else:
				conn.commit()


def store_row(conn, attributestore, column_names, timestamp, entity_id, values):
	if len(values) != len(column_names):
		raise DataError(\
			"Number of values does not match number of attributes ({0} != {1})".\
				format(len(values), len(column_names)))

	values_hash = calc_hash(values)

	table = attributestore.table
	table_curr = attributestore.table_curr

	try:
		current_hash, current_timestamp = get_current_hash(conn, table_curr,
				entity_id)
	except NoSuchTableError as exc:
		data_types = extract_data_types([(entity_id, values)])
		fix = partial(create_table, conn, attributestore, column_names, data_types)
		raise RecoverableError(str(exc), fix)
	except NoRecordError:
		try:
			insert_in_current(conn, table_curr, column_names,
				timestamp, entity_id, values, values_hash)
		except UndefinedColumnError as exc:
			data_types = extract_data_types([(entity_id, values)])
			columns = zip(column_names, data_types)
			fix = partial(check_columns_exist, conn, table, columns)
			raise RecoverableError(str(exc), fix)

	else:
		if timestamp >= current_timestamp and current_hash == values_hash:
			pass # No updated attribute values; do nothing
		elif timestamp >= current_timestamp and current_hash != values_hash:
			if timestamp != current_timestamp:
				copy_to_archive(conn, attributestore, entity_id)
			remove_from_current(conn, table_curr, entity_id)
			try:
				insert_in_current(conn, table_curr, column_names,
					timestamp, entity_id, values, values_hash)
			except UndefinedColumnError as exc:
				data_types = extract_data_types([(entity_id, values)])
				columns = zip(column_names, data_types)
				fix = partial(check_columns_exist, conn, table, columns)
				raise RecoverableError(str(exc), fix)

		elif timestamp < current_timestamp:
			# This should not happen too much (maybe in a data recovering scenario),
			# we're dealing with attribute data that's older than the attribute data
			# in curr table
			archived_timestamps_and_hashes = get_archived_timestamps_and_hashes(
				conn, attributestore.table, entity_id)

			archived_timestamps = map(first, archived_timestamps_and_hashes)

			if timestamp > max(archived_timestamps):
				if values_hash == current_hash:
					# these (identical) attribute values are older than the ones in curr
					remove_from_current(conn, table_curr, entity_id,)
					try:
						insert_in_current(conn, table_curr, column_names, timestamp,
								entity_id, values, values_hash)
					except UndefinedColumnError as exc:
						data_types = extract_data_types([(entity_id, values)])
						columns = zip(column_names, data_types)
						fix = partial(check_columns_exist, conn, table, columns)
						raise RecoverableError(str(exc), fix)
				elif values_hash != current_hash:
					# attribute values in curr are up-to-date
					insert_in_archive(conn, table, column_names, timestamp,
						entity_id, values, values_hash)
			elif timestamp < min(archived_timestamps):
				# attribute data is older than all data in database
				insert_in_archive(conn, table, column_names, timestamp,
						entity_id, values, values_hash)
			elif timestamp in archived_timestamps:
				# replace attribute data with same timestamp in archive
				remove_from_archive(conn, table, entity_id, timestamp)
				insert_in_archive(conn, table, column_names, timestamp,
						entity_id, values, values_hash)
			else:
				archived_timestamps_and_hashes.sort()
				archived_timestamps_and_hashes.reverse() # Order from new to old

				#Determine where old attribute data should be placed in archive table
				for index, (ts, h) in enumerate(archived_timestamps_and_hashes):
					if timestamp > ts:
						(archived_timestamp, archived_hash) = \
							archived_timestamps_and_hashes[index - 1]
						break

				if values_hash == archived_hash:
					remove_from_archive(conn, table, entity_id, archived_timestamp)
					insert_in_archive(conn, table, column_names, timestamp,
						entity_id, values, values_hash)


def get_current_hash(conn, table_curr, entity_id):
	query = (
		"SELECT hash, timestamp "
		"FROM {} "
		"WHERE entity_id=%s").format(table_curr.render())

	with closing(conn.cursor()) as cursor:
		try:
			cursor.execute(query, (entity_id,))
		except psycopg2.ProgrammingError as exc:
			if exc.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
				raise NoSuchTableError(table_curr.name)
			else:
				raise NonRecoverableError("{0}, {1!s} in query '{2}'".\
					format(exc.pgcode, exc, query))
		if cursor.rowcount > 0:
			previous_hash, previous_timestamp = cursor.fetchone()
			return previous_hash, previous_timestamp
		else:
			raise NoRecordError()


def get_archived_timestamps_and_hashes(conn, table, entity_id):
	query = (
		"SELECT timestamp, hash "
		"FROM {} "
		"WHERE entity_id=%s").format(table.render())

	with closing(conn.cursor()) as cursor:
		cursor.execute(query, (entity_id,))
		return cursor.fetchall()


def copy_to_archive(conn, attributestore, entity_id):
	query = (
		"INSERT INTO {table} "
		"SELECT * FROM {table_curr} WHERE entity_id = %s").format(
				table=attributestore.table.render(),
				table_curr=attributestore.table_curr.render())

	with closing(conn.cursor()) as cursor:
		try:
			cursor.execute(query, (entity_id,))
		except psycopg2.IntegrityError as exc:
			fix = partial(sanitize_archive, conn, attributestore)
			raise RecoverableError(str(exc), fix)


def insert_in_archive(conn, table, value_columns, timestamp, entity_id, values,
		values_hash):

	system_columns = "entity_id", "timestamp", "hash"
	columns = system_columns + tuple(value_columns)
	columns_part = ", ".join(map(enquote_ident, columns))

	args_part = ", ".join(["%s"] * len(columns))

	insert_query = (
		"INSERT INTO {0} ({1}) "
		"VALUES ({2})").format(table.render(), columns_part, args_part)

	args = [entity_id, timestamp, values_hash] + values

	with closing(conn.cursor()) as cursor:
		try:
			cursor.execute(insert_query, args)

		except psycopg2.Error as exc:
			if exc.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
				fix = partial(remove_from_archive, conn, table, entity_id, timestamp)
				raise RecoverableError(str(exc), fix)
			elif exc.pgcode == psycopg2.errorcodes.UNDEFINED_COLUMN:
				fix = partial(check_columns_exist, conn, table, value_columns)
				raise RecoverableError(str(exc), fix)
			if exc.pgcode in DATATYPE_MISMATCH_ERRORS:
				raise DataTypeMismatchError(str(exc))
			else:
				raise NonRecoverableError("{0}, {1!s} in query '{2}'".format(
					exc.pgcode, exc, insert_query))


def insert_in_current(conn, table_curr, value_columns, timestamp, entity_id,
	values, values_hash):
	system_columns = "entity_id", "timestamp", "hash"
	columns = system_columns + tuple(value_columns)
	columns_part = ", ".join(map(enquote_ident, columns))
	args_part = ", ".join(["%s"] * len(columns))

	insert_query = (
		"INSERT INTO {0} ({1}) "
		"VALUES ({2})").format(table_curr.render(), columns_part, args_part)

	args = [entity_id, timestamp, values_hash]
	args.extend(values)

	with closing(conn.cursor()) as cursor:
		try:
			cursor.execute(insert_query, args)
		except psycopg2.Error as exc:
			if exc.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
				fix = partial(remove_from_current, conn, table_curr, entity_id)
				raise RecoverableError(str(exc), fix)
			elif exc.pgcode == psycopg2.errorcodes.UNDEFINED_COLUMN:
				raise UndefinedColumnError()
			if exc.pgcode in DATATYPE_MISMATCH_ERRORS:
				compiled_query = cursor.mogrify(insert_query, args)
				raise DataTypeMismatchError("{!s}\nIn query: {}".format(exc,
						compiled_query))
			else:
				raise NonRecoverableError("{0}, {1!s} in query '{2}'".format(
					exc.pgcode, exc, insert_query))


def check_columns_exist(conn, table, columns):
	for name, type in columns:
		create_column(conn, table.schema.name, table.name, name, type)


def remove_from_current(conn, table_curr, entity_id):
	query = (
		"DELETE FROM {} "
		"WHERE entity_id=%s").format(table_curr.render())
	args = (entity_id,)

	with closing(conn.cursor()) as cursor:
		cursor.execute(query, args)


def remove_from_archive(conn, table, entity_id, timestamp):
	query = (
		"DELETE FROM {} "
		"WHERE entity_id=%s AND timestamp=%s").format(table.render())
	args = (entity_id, timestamp)

	with closing(conn.cursor()) as cursor:
		cursor.execute(query, args)


def calc_hash(values):
	return hashlib.md5(str(values)).hexdigest()


def sanitize_archive(conn, attributestore):
	"""
	Remove 'impossible' records (same entity_id and timestamp as in curr) in
	archive table.
	"""
	query = (
		"DELETE FROM ONLY {table} USING {table_curr} WHERE "
		"{table}.entity_id = {table_curr}.entity_id AND "
		"{table}.timestamp = {table_curr}.timestamp").format(
			table=attributestore.table.render(),
			table_curr=attributestore.table_curr.render())

	with closing(conn.cursor()) as cursor:
		cursor.execute(query)
		affectedrows = cursor.rowcount

	logging.warning("Sanitized delta table {0} (deleted {1} rows)".format(
		attributestore.table.name, affectedrows))


enquote_ident = partial(str.format, '"{}"')
