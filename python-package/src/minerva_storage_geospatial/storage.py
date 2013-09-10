# -*- codin# -*- coding: utf-8 -*-
"""
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
import logging

from minerva.storage.generic import MaxRetriesError, RecoverableError

from minerva_storage_geospatial.tables import MAX_RETRIES, store_cell, \
    store_site


def store_cells(conn, rows):
    for timestamp, cell in rows:
        retry = True
        attempt = 0
        while retry is True:
            retry = False
            attempt += 1

            if attempt > MAX_RETRIES:
                raise MaxRetriesError(
                    "Max retries ({0}) reached".format(MAX_RETRIES))
            try:
                with closing(conn.cursor()) as cursor:
                    store_cell(cursor, timestamp, cell)
            except RecoverableError as err:
                conn.rollback()
                logging.debug(str(err))
                err.fix()
                retry = True
            else:
                conn.commit()


def store_sites(conn, target_srid, rows):
    for timestamp, site in rows:
        if not site.position.x or not site.position.y:
            continue

        retry = True
        attempt = 0
        while retry is True:
            retry = False
            attempt += 1

            if attempt > MAX_RETRIES:
                raise MaxRetriesError(
                    "Max retries ({0}) reached".format(MAX_RETRIES))
            try:
                with closing(conn.cursor()) as cursor:
                    store_site(cursor, target_srid, timestamp, site)
            except RecoverableError as err:
                conn.rollback()
                logging.debug(str(err))
                err.fix()
                retry = True
            else:
                conn.commit()
