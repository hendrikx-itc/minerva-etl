import StringIO
from contextlib import closing
import logging

from minerva.db.util import create_temp_table, drop_table, create_copy_from_file

SCHEMA = "delta"


def tag_attributes(conn, tag_links):
	"""
	Update delta.attribute_tag_link table

	:param conn: Minerva database connection
	:param tag_links: list of tuples like (attribute_id, tag_name)
	"""
	tmp_table_name = store_in_temp_table(conn, tag_links)

	query = (
		"INSERT INTO {0}.attribute_tag_link (attribute_id, tag_id) "
		"(SELECT tmp.attribute_id, tag.id "
		"FROM {1} tmp "
		"JOIN directory.tag tag ON tag.name = tmp.tag "
		"LEFT JOIN {0}.attribute_tag_link ttl ON "
			"ttl.attribute_id = tmp.attribute_id AND ttl.tag_id = tag.id "
		"WHERE ttl.attribute_id IS NULL)").format(SCHEMA, tmp_table_name)

	with closing(conn.cursor()) as cursor:
		cursor.execute(query)

	drop_table(conn, tmp_table_name)

	conn.commit()


def store_in_temp_table(conn, tag_links):
	"""
	Create temporay table with tag links

	:param conn: Minerva database connection
	:param tag_links: list of tuples like (attribute_id, tag_name)
	"""

	tmp_table_name = "tmp_attribute_tags"
	columns = [
		("attribute_id", "integer"),
		("tag", "varchar")
	]
	column_names = [col_name for col_name, col_type in columns]
	sql_columns = ["{} {}".format(*column) for column in columns]

	copy_from_file = create_copy_from_file(tag_links, ('d','s'))
	create_temp_table(conn, tmp_table_name, sql_columns)

	with closing(conn.cursor()) as cursor:
		cursor.copy_from(copy_from_file, tmp_table_name, columns=column_names)

	return tmp_table_name


def flush_tag_links(conn, tag_name):
	"""
	Remove tag links for a specific tag
	:param conn: Minerva database connection
	:param tag_name: tag specifying attribute tags links that will be removed
	"""
	query = (
		"DELETE FROM {0}.attribute_tag_link atl "
		"USING directory.tag tag "
		"WHERE tag.id = atl.tag_id AND tag.name = %s").format(SCHEMA)

	args = (tag_name, )


	with closing(conn.cursor()) as cursor:
		cursor.execute(query, args)
