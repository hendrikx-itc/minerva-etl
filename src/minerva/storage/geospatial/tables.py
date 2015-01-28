# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2011 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import hashlib
from functools import partial
import logging

import psycopg2

from minerva.directory.relation import get_relation_name
from minerva.util import head
from minerva.storage.generic import RecoverableError, NonRecoverableError

from minerva.storage.geospatial.types import transform_srid


SCHEMA = "gis"
CELL_TABLENAME = "cell"
SITE_TABLENAME = "site"

MAX_RETRIES = 10


class DataTypeMismatchError(Exception):
    pass


class NoRecordError(Exception):
    pass


def calc_hash(values):
    return hashlib.md5(str(values)).hexdigest()


def insert_cell_in_current(cursor, timestamp, values_hash, cell):
    query = (
        'INSERT INTO "{0}"."{1}_curr" '
        '(entity_id, timestamp, hash, azimuth, type) '
        'VALUES (%s, %s, %s, %s, %s)'
    ).format(SCHEMA, CELL_TABLENAME)

    args = cell.entity_id, timestamp, values_hash, cell.azimuth, cell.type

    try:
        cursor.execute(query, args)
    except psycopg2.DatabaseError as exc:
        if exc.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
            fix = partial(
                remove_from_current, cursor, CELL_TABLENAME, cell.entity_id
            )
            raise RecoverableError(str(exc), fix)
        else:
            raise NonRecoverableError(
                "{0}, {1!s} in query '{2}'".format(exc.pgcode, exc, query)
            )
    except (psycopg2.DataError, psycopg2.ProgrammingError,
            psycopg2.IntegrityError) as exc:
        raise NonRecoverableError(
            "{0}, {1!s} in query '{2}'".format(exc.pgcode, exc, query)
        )


def insert_cell_in_archive(cursor, timestamp, values_hash, cell):
    query = (
        'INSERT INTO "{0}"."{1}" '
        '(entity_id, timestamp, hash, azimuth, type) '
        'VALUES (%s, %s, %s, %s, %s)'
    ).format(SCHEMA, CELL_TABLENAME)

    args = cell.entity_id, timestamp, values_hash, cell.azimuth, cell.type

    try:
        cursor.execute(query, args)
    except psycopg2.DatabaseError as exc:
        if exc.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
            fix = partial(remove_from_archive, cursor, CELL_TABLENAME,
                          timestamp, cell.entity_id)
            raise RecoverableError(str(exc), fix)
        else:
            raise NonRecoverableError(
                "{0}, {1!s} in query '{2}'".format(exc.pgcode, exc, query)
            )
    except (psycopg2.DataError, psycopg2.ProgrammingError,
            psycopg2.IntegrityError) as exc:
        raise NonRecoverableError(
            "{0}, {1!s} in query '{2}'".format(exc.pgcode, exc, query)
        )


def insert_site_in_current(cursor, target_srid, timestamp, site, values_hash):
    position_part = transform_srid(site.position.as_sql(), target_srid)

    query = (
        "INSERT INTO {0}.{1}_curr "
        "(entity_id, timestamp, hash, position) "
        "VALUES (%s, %s, %s, {2})"
    ).format(SCHEMA, SITE_TABLENAME, position_part)

    args = site.entity_id, timestamp, values_hash

    try:
        cursor.execute(query, args)
    except psycopg2.DatabaseError as exc:
        if exc.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
            fix = partial(remove_from_current, cursor, SITE_TABLENAME,
                          site.entity_id)
            raise RecoverableError(str(exc), fix)
        else:
            raise NonRecoverableError(
                "{0}, {1!s} in query '{2}'".format(exc.pgcode, exc, query)
            )
    except (psycopg2.DataError, psycopg2.ProgrammingError,
            psycopg2.IntegrityError) as exc:
        raise NonRecoverableError(
            "{0}, {1!s} in query '{2}'".format(exc.pgcode, exc, query)
        )


def insert_site_in_archive(cursor, target_srid, timestamp, site, values_hash):
    position_part = transform_srid(site.position.as_sql(), target_srid)

    query = (
        'INSERT INTO "{0}"."{1}" '
        '(entity_id, timestamp, position, hash) '
        'VALUES (%s, %s, {2}, %s)'
    ).format(SCHEMA, SITE_TABLENAME, position_part)

    args = site.entity_id, timestamp, values_hash

    try:
        cursor.execute(query, args)
    except psycopg2.DatabaseError as exc:
        if exc.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
            fix = partial(remove_from_archive, cursor, SITE_TABLENAME,
                          timestamp, site.entity_id)
            raise RecoverableError(str(exc), fix)
        else:
            raise NonRecoverableError(
                "{0}, {1!s} in query '{2}'".format(exc.pgcode, exc, query))
    except (psycopg2.DataError, psycopg2.ProgrammingError,
            psycopg2.IntegrityError) as exc:
        raise NonRecoverableError(
            "{0}, {1!s} in query '{2}'".format(exc.pgcode, exc, query))


def remove_from_current(cursor, tablename, entity_id):
    query = (
        'DELETE FROM "{0}"."{1}_curr" '
        'WHERE entity_id=%s'
    ).format(SCHEMA, tablename)

    args = (entity_id,)

    cursor.execute(query, args)


def remove_from_archive(cursor, tablename, timestamp, entity_id):
    query = (
        'DELETE FROM "{0}"."{1}" '
        'WHERE entity_id=%s AND timestamp=%s'
    ).format(SCHEMA, tablename)

    args = (entity_id, timestamp)

    cursor.execute(query, args)


def get_current_hash(cursor, tablename, entity_id):
    query = (
        'SELECT hash, timestamp '
        'FROM "{0}"."{1}_curr" '
        'WHERE entity_id=%s'
    ).format(SCHEMA, tablename)

    try:
        cursor.execute(query, (entity_id,))
    except psycopg2.ProgrammingError as exc:
        raise NonRecoverableError(
            "{0}, {1!s} in query '{2}'".format(exc.pgcode, exc, query)
        )

    if cursor.rowcount > 0:
        previous_hash, previous_timestamp = cursor.fetchone()
        return previous_hash, previous_timestamp
    else:
        raise NoRecordError()


def get_archived_timestamps_and_hashes(cursor, tablename, entity_id):
    query = (
        'SELECT timestamp, hash '
        'FROM "{0}"."{1}" '
        'WHERE entity_id = %s'
    ).format(SCHEMA, tablename)

    args = (entity_id,)

    cursor.execute(query, args)

    return cursor.fetchall()


def copy_to_archive(cursor, table_name, entity_id):
    query = (
        'INSERT INTO {0}."{1}" '
        'SELECT * FROM {0}."{1}_curr" '
        'WHERE entity_id = %s'
    ).format(SCHEMA, table_name)

    args = (entity_id,)

    try:
        cursor.execute(query, args)
    except psycopg2.IntegrityError as exc:
        fix = partial(sanitize_archive, cursor, table_name)
        raise RecoverableError(str(exc), fix)


def store_site(cursor, target_srid, timestamp, site):
    values_hash = calc_hash([site.position.x, site.position.y])

    try:
        current_hash, current_timestamp = get_current_hash(
            cursor, SITE_TABLENAME, site.entity_id)
    except NoRecordError:
        insert_site_in_current(
            cursor, target_srid, timestamp, site, values_hash
        )
    else:
        if timestamp >= current_timestamp and current_hash == values_hash:
            pass  # No updated attribute values; do nothing
        elif timestamp >= current_timestamp and current_hash != values_hash:
            if timestamp != current_timestamp:
                copy_to_archive(cursor, SITE_TABLENAME, site.entity_id)

            remove_from_current(cursor, SITE_TABLENAME, site.entity_id)
            insert_site_in_current(
                cursor, target_srid, timestamp, site, values_hash
            )

        elif timestamp < current_timestamp:
            # This should not happen too much (maybe in a data recovering
            # scenario), we're dealing with attribute data that's older than
            # the attribute data in curr table
            archived_timestamps_and_hashes = get_archived_timestamps_and_hashes(
                cursor, SITE_TABLENAME, site.entity_id
            )
            archived_timestamps = map(head, archived_timestamps_and_hashes)

            if timestamp > max(archived_timestamps):
                if values_hash == current_hash:
                    # these (identical) attribute values are older than the
                    # ones in curr
                    remove_from_current(cursor, SITE_TABLENAME, site.entity_id)
                    insert_site_in_current(cursor, timestamp, site,
                                           values_hash)
                elif values_hash != current_hash:
                    # attribute values in curr are up-to-date
                    insert_site_in_archive(cursor, target_srid, timestamp,
                                           site, values_hash)
            elif timestamp < min(archived_timestamps):
                # attribute data is older than all data in database
                insert_site_in_archive(cursor, target_srid, timestamp, site,
                                       values_hash)
            elif timestamp in archived_timestamps:
                # replace attribute data with same timestamp in archive
                remove_from_archive(cursor, SITE_TABLENAME, timestamp,
                                    site.entity_id)
                insert_site_in_archive(cursor, target_srid, timestamp, site,
                                       values_hash)
            else:
                archived_timestamps_and_hashes.sort()
                archived_timestamps_and_hashes.reverse()  # Order from new to old

                # Determine where old attribute data should be placed in
                # archive table
                for index, (ts, h) in enumerate(archived_timestamps_and_hashes):
                    if timestamp > ts:
                        archived_timestamp, archived_hash = archived_timestamps_and_hashes[index - 1]
                        break

                if values_hash == archived_hash:
                    remove_from_archive(cursor, SITE_TABLENAME, archived_timestamp, site.entity_id)
                    insert_site_in_archive(cursor, target_srid, timestamp, site, values_hash)


def store_cell(cursor, timestamp, cell):
    values_hash = calc_hash([cell.azimuth, cell.type])

    try:
        current_hash, current_timestamp = get_current_hash(
            cursor, CELL_TABLENAME, cell.entity_id)
    except NoRecordError:
        insert_cell_in_current(cursor, timestamp, values_hash, cell)
    else:
        if timestamp >= current_timestamp and current_hash == values_hash:
            pass  # No updated attribute values; do nothing
        elif timestamp >= current_timestamp and current_hash != values_hash:
            if timestamp != current_timestamp:
                copy_to_archive(cursor, CELL_TABLENAME, cell.entity_id)

            remove_from_current(cursor, CELL_TABLENAME, cell.entity_id)
            insert_cell_in_current(cursor, timestamp, values_hash, cell)

        elif timestamp < current_timestamp:
            # This should not happen too much (maybe in a data recovering
            # scenario), we're dealing with attribute data that's older than
            # the attribute data in curr table
            archived_timestamps_and_hashes = get_archived_timestamps_and_hashes(cursor, CELL_TABLENAME, cell.entity_id)
            archived_timestamps = [
                ts for (ts, h) in archived_timestamps_and_hashes]

            if timestamp > max(archived_timestamps):
                if values_hash == current_hash:
                    # these (identical) attribute values are older than the
                    # ones in curr
                    remove_from_current(cursor, CELL_TABLENAME, cell.entity_id)
                    insert_cell_in_current(cursor, timestamp, values_hash,
                                           cell)
                elif values_hash != current_hash:
                    # attribute values in curr are up-to-date
                    insert_cell_in_archive(cursor, timestamp, values_hash,
                                           cell)
            elif timestamp < min(archived_timestamps):
                # attribute data is older than all data in database
                insert_cell_in_archive(cursor, timestamp, values_hash, cell)
            elif timestamp in archived_timestamps:
                # replace attribute data with same timestamp in archive
                remove_from_archive(cursor, CELL_TABLENAME, timestamp,
                                    cell.entity_id)
                insert_cell_in_archive(cursor, timestamp, values_hash, cell)
            else:
                archived_timestamps_and_hashes.sort()
                archived_timestamps_and_hashes.reverse() # Order from new to old

                # Determine where old attribute data should be placed in
                # archive table
                for index, (ts, h) in enumerate(archived_timestamps_and_hashes):
                    if timestamp > ts:
                        (archived_timestamp, archived_hash) = archived_timestamps_and_hashes[index - 1]
                        break

                if values_hash == archived_hash:
                    remove_from_archive(cursor, CELL_TABLENAME,
                                        archived_timestamp, cell.entity_id)
                    insert_cell_in_archive(cursor, timestamp, values_hash,
                                           cell)


def sanitize_archive(cursor, table):
    """
    Remove 'impossible' records (same entity_id and timestamp as in curr) in
    archive table.
    """
    query = (
        "DELETE FROM ONLY \"{0}\".\"{1}\" USING \"{0}\".\"{1}_curr\" WHERE "
        "\"{0}\".\"{1}\".entity_id = \"{0}\".\"{1}_curr\".entity_id AND "
        "\"{0}\".\"{1}\".timestamp = \"{0}\".\"{1}_curr\".timestamp ".format(
            SCHEMA, table))

    cursor.execute(query)

    logging.warning(
        "Sanitized geospatial table {0} (deleted {1} rows)".format(
            table, cursor.rowcount
        )
    )


def make_box_2d(bbox):
    return (
        "ST_MakeBox2D("
        "ST_Point({left}, {bottom}), "
        "ST_Point({right}, {top})"
        ")").format(**bbox)


def set_srid(point, srid):
    return "ST_SetSRID({}, {})".format(point, srid)


def get_column_srid(cursor, table_name, column_name):
    query = (
        "SELECT srid "
        "FROM geometry_columns "
        "WHERE f_geometry_column = %s "
        "AND f_table_name = %s "
        "AND f_table_schema = %s")

    args = column_name, table_name, "gis"

    cursor.execute(query, args)

    return head(cursor.fetchone())


def create_sql_for_bbox(conn, entitytype, site_srid, region, srid):
    box2d = transform_srid(set_srid(make_box_2d(region), srid), site_srid)

    if entitytype.name.lower() == "cell":
        relation_name = get_relation_name(conn, entitytype.name, "site")
        return (
            "SELECT cell.entity_id AS id "
            "FROM gis.cell_curr cell "
            "JOIN relation.\"{}\" rel on rel.source_id = cell.entity_id "
            "JOIN gis.site_curr site on rel.target_id = site.entity_id "
            "WHERE site.position && {}"
        ).format(relation_name, box2d)
    else:
        relation_site_cell_name = get_relation_name(conn, "Cell", "Site")
        relation_name = get_relation_name(conn, "Cell", entitytype.name)
        return (
            "SELECT r.target_id AS id "
            "FROM relation.\"{}\" r "
            "JOIN relation.\"{}\" rel on rel.source_id = r.source_id "
            "JOIN gis.site_curr site on rel.target_id = site.entity_id "
            "WHERE site.position && {}"
        ).format(relation_name, relation_site_cell_name, box2d)
