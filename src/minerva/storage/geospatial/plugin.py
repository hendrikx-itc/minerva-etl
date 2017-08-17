# -* -coding: utf - 8 -* -
"""
Provides GIS storage functionality using delta-like-tables.
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

from minerva.directory import helpers

from minerva.storage.geospatial.tables import get_column_srid, \
    create_sql_for_bbox
from minerva.storage.geospatial.storage import store_cells, store_sites
from minerva.storage.geospatial.retrieve import retrieve_attribute, \
    retrieve_related_attribute, retrieve_trend, retrieve_related_trend, \
    get_cells_in_region, get_sites_in_region, get_entities_in_region


class GeospatialPlugin(object):
    def __init__(self, conn):
        self.conn = conn

        with closing(conn.cursor()) as cursor:
            self.site_srid = get_column_srid(cursor, "site", "position")

    def store_cells(self, rows):
        # datarows: [(timestamp, Cell), ...)]
        store_cells(self.conn, rows)

    def store_sites(self, rows):
        # datarows: [(timestamp, Site), ...)]
        store_sites(self.conn, self.site_srid, rows)

    def get_entities_in_region(self, region, srid, entitytype):
        return get_entities_in_region(self.conn, self.site_srid, region,
                                      srid, entitytype)

    def get_cells_in_region(self, region, srid):
        return get_cells_in_region(self.conn, self.site_srid, region, srid)

    def get_sites_in_region(self, region, srid):
        return get_sites_in_region(self.conn, self.site_srid, region, srid)

    def retrieve_trend(self, region, srid, trend, timestamp, limit=None):
        entitytype = helpers.get_entitytype_by_id(self.conn,
                                                  trend["entitytype_id"])
        datasource = helpers.get_datasource_by_id(self.conn,
                                                  trend["datasource_id"])

        return retrieve_trend(self.conn, self.site_srid, region, srid,
                              datasource, entitytype, trend["name"],
                              trend["granularity"],
                              timestamp, limit)

    def retrieve_related_trend(self, region, srid, trend, timestamp,
                               limit=None):
        entitytype = helpers.get_entitytype_by_id(self.conn,
                                                  trend["entitytype_id"])
        datasource = helpers.get_datasource_by_id(self.conn,
                                                  trend["datasource_id"])

        return retrieve_related_trend(self.conn, self.site_srid, region, srid,
                                      datasource, entitytype, trend["name"],
                                      trend["granularity"], timestamp, limit)

    def retrieve_attribute(self, region, srid, attribute, limit=None):
        entitytype = helpers.get_entitytype_by_id(self.conn,
                                                  attribute["entitytype_id"])
        datasource = helpers.get_datasource_by_id(self.conn,
                                                  attribute["datasource_id"])

        return retrieve_attribute(self.conn, self.site_srid, region, srid,
                                  datasource, entitytype, attribute["name"],
                                  limit)

    def retrieve_related_attribute(self, region, srid, attribute, limit=None):
        entitytype = helpers.get_entitytype_by_id(self.conn,
                                                  attribute["entitytype_id"])
        datasource = helpers.get_datasource_by_id(self.conn,
                                                  attribute["datasource_id"])

        return retrieve_related_attribute(self.conn, self.site_srid,
                                          region, srid, datasource, entitytype,
                                          attribute["name"], limit)

    def create_sql_for_region(self, entitytype, region, srid):
        return create_sql_for_bbox(self.conn, entitytype, self.site_srid,
                                   region, srid)
