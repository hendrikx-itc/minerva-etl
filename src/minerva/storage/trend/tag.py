# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing

from minerva.db.util import create_temp_table, drop_table, \
    create_copy_from_file

from minerva.storage.trend import schema


def tag_trends(conn, tag_links):
    """
    Update trend.trend_tag_link table

    :param conn: Minerva database connection
    :param tag_links: list of tuples like (trend_id, tag_name)
    """
    tmp_table_name = store_in_temp_table(conn, tag_links)

    query = (
        "INSERT INTO {0}.trend_tag_link (trend_id, tag_id) "
        "("
        "   SELECT tmp.trend_id, tag.id "
        "   FROM {1} tmp "
        "   JOIN directory.tag tag ON lower(tag.name) = lower(tmp.tag) "
        "   LEFT JOIN {0}.trend_tag_link ttl ON "
        "       ttl.trend_id = tmp.trend_id AND ttl.tag_id = tag.id "
        "   WHERE ttl.trend_id IS NULL"
        ")").format(schema.name, tmp_table_name)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)

    drop_table(conn, tmp_table_name)

    conn.commit()


def store_in_temp_table(conn, tag_links):
    """
    Create temporay table with tag links

    :param conn: Minerva database connection
    :param tag_links: list of tuples like (trend_id, tag_name)
    """

    tmp_table_name = "tmp_trend_tags"
    columns = [
        ("trend_id", "integer"),
        ("tag", "varchar")
    ]
    column_names = [col_name for col_name, col_type in columns]
    sql_columns = ["{} {}".format(*column) for column in columns]

    copy_from_file = create_copy_from_file(tag_links, ('d', 's'))
    create_temp_table(conn, tmp_table_name, sql_columns)

    with closing(conn.cursor()) as cursor:
        cursor.copy_from(copy_from_file, tmp_table_name, columns=column_names)

    return tmp_table_name


def flush_tag_links(conn, tag_name):
    """
    Remove tag links for a specific tag
    :param conn: Minerva database connection
    :param tag_name: tag specifying trend tags links that will be removed
    """
    query = (
        "DELETE FROM {0}.trend_tag_link ttl "
        "USING directory.tag tag "
        "WHERE tag.id = ttl.tag_id AND lower(tag.name)"
        " = lower(%s)").format(schema.name)

    args = (tag_name, )

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)
