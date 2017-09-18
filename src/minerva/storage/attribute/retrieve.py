# -*- coding: utf-8 -*-
from contextlib import closing
from decimal import Decimal

from itertools import groupby
from operator import itemgetter

from minerva.util.timestamp import to_unix_timestamp
from minerva.directory.entitytype import EntityType
from minerva.directory.entity import Entity


def enquote_ident(ident):
    return '"{}"'.format(ident)


def retrieve(conn, table, attribute_names, entities, ts=None):
    """
    Retrieve all attribute data or attribute data for a specific timestamp.

    :param conn: Minerva database connection
    :param table_name: name of table containing desired attribute data
    :param attribute_names: list of attributes to retrieve
    :param entities: list of entities to get attribute data for; None means
    all entities
    :param ts: timestamp of attribute data to retrieve
    """
    in_part = ""

    if entities is not None:
        entity_set = ",".join(map(str, entities))

        if entity_set:
            in_part = "entity_id in ({0:s})".format(entity_set)
        else:
            return []

    attribute_columns = ['d."{0}"'.format(a) for a in attribute_names]
    columns_part = ",".join(["d.entity_id", "d.timestamp"] + attribute_columns)

    if ts:
        if in_part:
            in_part = "AND " + in_part

        query = (
            "SELECT {0} FROM {1} d "
            "JOIN ("
            "SELECT entity_id AS entity_id, MAX(timestamp) AS timestamp "
            "FROM {1} "
            "WHERE timestamp < %s {2} GROUP BY entity_id) "
            "AS sub "
            "ON sub.entity_id = d.entity_id "
            "AND sub.timestamp = d.timestamp").format(
            columns_part, table.render(), in_part)
    else:
        query = "SELECT {0} FROM {1} d ".format(columns_part, table.render())

        if in_part:
            query += "WHERE " + in_part

    with closing(conn.cursor()) as cursor:
        if ts:
            cursor.execute(query, (ts,))
        else:
            cursor.execute(query)

        return cursor.fetchall()


def retrieve_current(conn, table, attribute_names, entities):
    """
    Retrieve current values.

    :param conn: Minerva database connection
    :param table_name: name of table containing desired attribute data
    :param attribute_names: list of attributes to retrieve
    :param entities: list of entities to get attribute data for; None means
    all entities
    :param ts: timestamp of attribute data to retrieve
    """
    in_part = ""

    if entities is not None:
        entity_set = ",".join(map(str, entities))

        if entity_set:
            in_part = "entity_id in ({0:s})".format(entity_set)
        else:
            return []

    attribute_columns = ['d."{0}"'.format(a) for a in attribute_names]
    columns_part = ",".join(["d.entity_id", "d.timestamp"] + attribute_columns)

    query = "SELECT {0} FROM {1} d ".format(columns_part, table.render())

    if in_part:
        query += "WHERE " + in_part

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)

        return cursor.fetchall()


def retrieve_related(conn, table_name, attribute_names, entities,
                     relation_table_name, limit=None):
    """
    Retrieve attribute data for related entities.

    TODO: implement way to get attribute data for a specific timestamp.
    """
    where_parts = []
    join_parts = []

    full_base_tbl_name = "attribute.\"{}\"".format(table_name)

    attr_columns = map(enquote_ident, attribute_names)

    query = (
        "SELECT r.source_id, r.target_id, \"timestamp\", {0} FROM {1} "
        "JOIN relation.\"{2}\" r ON r.target_id = {1}.entity_id ").format(
        ", ".join(attr_columns), full_base_tbl_name, relation_table_name)

    join_parts.append(
        " JOIN  ".format(
            full_base_tbl_name))

    where_parts = []

    if entities is not None:
        entity_set = ",".join(map(str, entities))
        if entity_set:
            where_parts.append("r.source_id IN ({0:s})".format(entity_set))
        else:
            return []

    if where_parts:
        query = query + " WHERE " + " AND ".join(where_parts)

    query = query + " ORDER BY \"timestamp\""

    if not limit is None:
        query = query + " LIMIT {0:d}".format(limit)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)

        return cursor.fetchall()


def retrieve_attributes_for_entity(conn, entity_id, attribute_ids):
    query = (
        "SELECT a.id, a.name, attribute_directory.to_table_name(astore) "
            "as table_name, et.name as entitytype_name "
        "FROM attribute_directory.attribute a "
        "JOIN attribute_directory.attributestore astore on a.attributestore_id = astore.id " 
        "JOIN directory.entitytype et ON et.id = astore.entitytype_id "
        "WHERE a.id IN ({}) "
        "ORDER BY table_name").format(",".join(map(str, attribute_ids)))

    with closing(conn.cursor()) as cursor:
        entity = Entity.get(entity_id)(cursor)

    result = {}

    def prepare_value(value):
        if not value is None:
            #if trendvalue is Decimal(Numeric) json cannot serialize it.
            if isinstance(value, Decimal):
                value = float(value)
        return value

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)

        column_names = [column.name for column in cursor.description]

        get_tablename = itemgetter(column_names.index("table_name"))
        get_name = itemgetter(column_names.index("name"))
        get_id = itemgetter(column_names.index("id"))
        get_entitytype = itemgetter(column_names.index("entitytype_name"))

        rows = cursor.fetchall()

        source_entitytype = get_entity_type_by_id(conn, entity.entitytype_id)
        for table_name, attrs_iter in groupby(rows, get_tablename):
            attrs = list(attrs_iter)
            attr_names = map(get_name, attrs)

            # Get entitytype of first entry, All in same table, SO all same
            # entitytype.
            target_entitytype_name = get_entitytype(attrs[0])
            if source_entitytype.name == target_entitytype_name:
                relation_table_name = "self"
            else:
                relation_table_name = "{}->{}".format(source_entitytype.name,
                                                      target_entitytype_name)

            data = retrieve_related(conn, table_name, attr_names, [entity.id],
                                    relation_table_name)

            for index, attr in enumerate(attrs):
                attr_id = get_id(attr)
                get_attr_value = itemgetter(index + 3)
                get_timestamp = itemgetter(2)

                result[attr_id] = [
                    (to_unix_timestamp(get_timestamp(row)),
                     prepare_value(get_attr_value(row))) for row in data]

    return result
