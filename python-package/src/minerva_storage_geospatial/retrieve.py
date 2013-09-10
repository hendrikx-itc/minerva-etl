# -*- coding: utf-8 -*-
"""
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

import psycopg2

from minerva.directory.relation import get_relation_name
from minerva_storage_trend.trendstore import TrendStore
from minerva_storage_trend.granularity import create_granularity

from minerva_storage_delta.tables import get_table_name as get_delta_tablename
from minerva_storage_delta import SCHEMA as delta_schema

from minerva_storage_geospatial.tables import make_box_2d
from minerva_storage_geospatial.types import set_srid, transform_srid


def get_entities_in_region(conn, database_srid, region, region_srid,
                           entitytype):

    bbox2d = transform_srid(set_srid(make_box_2d(region), region_srid),
                            database_srid)

    relation_name = get_relation_name(conn, "Cell", entitytype.name)
    relation_site_cell_name = get_relation_name(conn, "Cell", "Site")

    query = (
        "SELECT r.target_id "
        "FROM relation.\"{0}\" r "
        "JOIN relation.\"{1}\" site_rel on site_rel.source_id = r.source_id "
        "JOIN gis.site site ON site.entity_id = site_rel.target_id "
        "AND site.position && {2}").format(
        relation_name, relation_site_cell_name, bbox2d)

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query, region)
        except psycopg2.ProgrammingError:
            conn.rollback()
            rows = []
        else:
            rows = cursor.fetchall()

    return [entity_id for entity_id, in rows]


def get_cells_in_region(conn, database_srid, region, region_srid):
    bbox2d = transform_srid(set_srid(make_box_2d(region), region_srid),
                            database_srid)

    relation_site_cell_name = get_relation_name(conn, "Cell", "Site")

    query = (
        "SELECT site_rel.source_id "
        "FROM relation.\"{0}\" site_rel on site_rel.source_id = r.source_id "
        "JOIN gis.site site ON site.entity_id = site_rel.target_id "
        "AND site.position && {1}").format(
        relation_site_cell_name, bbox2d)

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query, region)
        except psycopg2.ProgrammingError:
            conn.rollback()
            rows = []
        else:
            rows = cursor.fetchall()

    return [entity_id for entity_id, in rows]


def get_sites_in_region(conn, database_srid, region, region_srid):
    bbox2d = transform_srid(set_srid(make_box_2d(region), region_srid),
                            database_srid)

    query = (
        "SELECT site.entity_id "
        "FROM gis.site site "
        "WHERE site.position && {}").format(bbox2d)

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query, region)
        except psycopg2.ProgrammingError:
            conn.rollback()
            rows = []
        else:
            rows = cursor.fetchall()

    return [entity_id for entity_id, in rows]


def retrieve_trend(conn, database_srid, region, region_srid, datasource,
                   entitytype, attribute_name, granularity_str, timestamp,
                   limit=None):

    granularity = create_granularity(granularity_str)

    with closing(conn.cursor()) as cursor:
        trendstore = TrendStore.get(cursor, datasource, entitytype,
                                    granularity)

    partition = trendstore.partition(timestamp)
    table = partition.table()

    full_base_tbl_name = table.render()

    relation_name = get_relation_name(conn, "Cell", "Site")

    bbox2d = transform_srid(set_srid(make_box_2d(region), region_srid),
                            database_srid)

    query = (
        "SELECT base_table.entity_id, base_table.\"{0}\" "
        "FROM {1} base_table "
        "JOIN relation.\"{2}\" site_rel "
        "ON site_rel.source_id = base_table.entity_id "
        "JOIN gis.site site ON site.entity_id = site_rel.target_id "
        "AND site.position && {3} "
        "WHERE base_table.\"timestamp\" = %(timestamp)s").format(
        attribute_name, full_base_tbl_name, relation_name, bbox2d)

    args = {
        "left": region["left"],
        "bottom": region["bottom"],
        "right": region["right"],
        "top": region["top"],
        "timestamp": timestamp}

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query, args)
        except psycopg2.ProgrammingError:
            conn.rollback()
            rows = []
        else:
            rows = cursor.fetchall()

    return dict((entity_id, value) for entity_id, value in rows)


def retrieve_related_trend(conn, database_srid, region, region_srid,
                           datasource, entitytype, attribute_name,
                           granularity_str, timestamp, limit=None):

    granularity = create_granularity(granularity_str)

    with closing(conn.cursor()) as cursor:
        trendstore = TrendStore.get(cursor, datasource, entitytype,
                                    granularity)

    partition = trendstore.partition(timestamp)
    table = partition.table()

    full_base_tbl_name = table.render()

    relation_name = get_relation_name(conn, "Cell", entitytype.name)
    relation_cell_site_name = get_relation_name(conn, "Cell", "Site")

    bbox2d = transform_srid(set_srid(make_box_2d(region), region_srid),
                            database_srid)

    query = (
        "SELECT r.source_id, r.target_id, base_table.\"{0}\" "
        "FROM {1} base_table "
        "JOIN relation.\"{2}\" r ON r.target_id = base_table.entity_id "
        "JOIN relation.\"{3}\" site_rel on site_rel.source_id = r.source_id "
        "JOIN gis.site site ON site.entity_id = site_rel.target_id "
        "AND site.position && {4} "
        "WHERE base_table.\"timestamp\" = %(timestamp)s").format(
        attribute_name, full_base_tbl_name, relation_name,
        relation_cell_site_name, bbox2d)

    args = {
        "left": region["left"],
        "bottom": region["bottom"],
        "right": region["right"],
        "top": region["top"],
        "timestamp": timestamp}

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query, args)
        except psycopg2.ProgrammingError:
            conn.rollback()
            rows = []
        else:
            rows = cursor.fetchall()

    result = {}
    for entity_id, related_entity_id, value in rows:
        if entity_id not in result:
            result[entity_id] = {}
        result[entity_id][related_entity_id] = value

    return result


def retrieve_attribute(conn, database_srid, region, region_srid, datasource,
                       entitytype, attribute_name, srid, limit=None):

    tablename = get_delta_tablename(datasource.name, entitytype.name)
    full_base_tbl_name = "{0}.\"{1}\"".format(delta_schema, tablename)

    relation_name = get_relation_name(conn, "Cell", "Site")

    bbox2d = transform_srid(set_srid(make_box_2d(region), region_srid),
                            database_srid)

    query = (
        "SELECT base_table.entity_id, min(base_table.\"timestamp\"), "
        "base_table.\"{0}\" FROM {1} base_table "
        "JOIN relation.\"{2}\" site_rel "
        "ON site_rel.source_id = base_table.entity_id "
        "JOIN gis.site site ON site.entity_id = site_rel.target_id "
        "AND site.position && {3} "
        "GROUP BY base_table.entity_id, base_table.\"{0}\" "
        "ORDER BY min(base_table.\"timestamp\")").format(
        attribute_name, full_base_tbl_name, relation_name, bbox2d)

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query, region)
        except psycopg2.ProgrammingError:
            conn.rollback()
            rows = []
        else:
            rows = cursor.fetchall()

    result = {}
    for entity_id, timestamp, value in rows:
        if not value is None:
            if entity_id not in result:
                result[entity_id] = []
            result[entity_id].append((timestamp, value))

    return result


def retrieve_related_attribute(conn, database_srid, region, region_srid,
                               datasource, entitytype, attribute_name, limit):

    tablename = get_delta_tablename(datasource.name, entitytype.name)
    full_base_tbl_name = "{0}.\"{1}\"".format(delta_schema, tablename)

    relation_name = get_relation_name(conn, "Cell", entitytype.name)
    relation_cell_site_name = get_relation_name(conn, "Cell", "Site")

    bbox2d = transform_srid(set_srid(make_box_2d(region), region_srid),
                            database_srid)

    query = (
        "SELECT r.source_id, r.target_id, base_table.\"timestamp\", "
        "base_table.\"{0}\" FROM {1} base_table "
        "JOIN relation.\"{2}\" r ON r.target_id = base_table.entity_id "
        "JOIN relation.\"{3}\" site_rel on site_rel.source_id = r.source_id "
        "JOIN gis.site site ON site.entity_id = site_rel.target_id "
        "AND site.position && {4} "
        "ORDER BY base_table.\"timestamp\"").format(
        attribute_name, full_base_tbl_name, relation_name,
        relation_cell_site_name, bbox2d)

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query, region)
        except psycopg2.ProgrammingError:
            conn.rollback()
            rows = []
        else:
            rows = cursor.fetchall()

    result = {}
    for entity_id, related_id, timestamp, value in rows:
        if not value is None:
            if entity_id not in result:
                result[entity_id] = {}
            if related_id not in result[entity_id]:
                result[entity_id][related_id] = []

            result[entity_id][related_id].append(
                (timestamp, value))

    return result
