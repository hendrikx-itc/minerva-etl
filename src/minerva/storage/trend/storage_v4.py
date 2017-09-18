# -*- coding: utf-8 -*-
"""
Provides PostgreSQL specific storage functionality using arrays.
"""
from contextlib import closing
import logging
from itertools import chain
from operator import itemgetter
import re

import psycopg2.errorcodes

from minerva.util import first
from minerva.db.util import quote_ident
from minerva.db.query import Sql, Table, And, Or, Eq, Column, FromItem, \
        Argument, Literal, LtEq, Gt, ands, Select, WithQuery, As, Value, \
        Parenthesis, column_exists
from minerva.db.error import AggregationError


def table_or_view_exists(cursor, table):
    relkind_column = Column("relkind")

    criterion = And(
        Eq(Column("relname")),
        Parenthesis(Or(Eq(relkind_column), Eq(relkind_column))))

    query = Select(1, from_=Table("pg_class"), where_=criterion)

    args = table.name, "r", "v"

    query.execute(cursor, args)

    return cursor.rowcount > 0


def quote_ident(ident):
    if isinstance(ident, str):
        return enquote_column_name(ident)
    elif hasattr(ident, "__iter__"):
        return ".".join('"{}"'.format(part) for part in ident)
    elif isinstance(ident, Sql):
        return ident.render()
    else:
        raise Exception("invalid identifier '{}'".format(ident))


def ensure_column(c):
    if isinstance(c, str):
        return Column(c)
    else:
        return c


def retrieve(
        cursor, tables, columns, entities, start, end,
        subquery_filter=None, relation_table_name=None,
        limit=None, entitytype=None):
    """
    Retrieve data.

    :param cursor: Minerva database cursor
    :param columns: A list of column identifiers (possibly for different
            datasources)
    :param entities: List of entity Ids
    :param start: The start timestamp of the range of trend values
    :param end: The end timestamp of the range of trend values
    :param subquery_filter: optional subquery for additional filtering
        by JOINing on field 'id' = entity_id
    :param relation_table_name: optional relation table name
    for converting entity
        ids to related ones
    """
    all_rows = []

    if entities is not None and len(entities) == 0:
        return []

    # group tables by partition size signature to be able to JOIN them later
    tables_by_partition_signature = {}
    for table in tables:
        signature = table.name.split("_")[-1]

        tables_by_partition_signature.setdefault(signature, []).append(table)

    for tables in tables_by_partition_signature.values():
        params = []

        if start == end and start is not None and len(tables) > 1:
            cols = [As(Argument(), "timestamp"), Column("dn"),
                    As(Column("id"), "entity_id")]

            q = Select(
                cols, from_=Table("directory", "entity"),
                where_=Eq(Column("entity_type_id"), Value(entitytype.id)))

            with_query = WithQuery("t", query=q)
            params.append(start)

            base_timestamp_column = Column("t", "timestamp")
            base_entity_id_column = Column("t", "entity_id")

            from_item = FromItem(Table("t"))
            data_tables = tables
        else:
            with_query = None

            base_tbl = first(tables)

            base_timestamp_column = Column(base_tbl, "timestamp")
            base_entity_id_column = Column(base_tbl, "entity_id")

            from_item = FromItem(base_tbl)
            data_tables = tables[1:]

        for tbl in data_tables:
            timestamp_comparison = Eq(
                Column(tbl, "timestamp"), base_timestamp_column)
            entity_id_comparison = Eq(
                Column(tbl, "entity_id"), base_entity_id_column)
            join_condition = And(
                timestamp_comparison, entity_id_comparison)

            from_item = from_item.join(
                tbl, on=join_condition, join_type="LEFT")

        if subquery_filter:
            filter_tbl = Literal("({0}) AS filter".format(subquery_filter))
            from_item = from_item.join(
                filter_tbl, on=Eq(Column(
                    "filter", "id"), base_entity_id_column))

        if relation_table_name:
            relation_table = Table("relation", relation_table_name)

            join_condition = Eq(Column("r", "source_id"),
                                base_entity_id_column)

            from_item = from_item.left_join(As(relation_table, "r"),
                                            on=join_condition)

            entity_id_column = Column("r", "target_id")
        else:
            entity_id_column = base_entity_id_column

        partition_columns = [entity_id_column, base_timestamp_column] + [
            ensure_column(c) for c in columns
        ]

        where_parts = []

        if not with_query:
            if start == end and start is not None:
                condition = Eq(base_timestamp_column, Argument())
                where_parts.append(condition)
                params.append(start)
            else:
                if start is not None:
                    condition = Gt(base_timestamp_column, Argument())
                    where_parts.append(condition)
                    params.append(start)

                if end is not None:
                    condition = LtEq(base_timestamp_column, Argument())
                    where_parts.append(condition)
                    params.append(end)

        if entities is not None:
            condition = Literal(
                "{0} IN ({1:s})".format(
                    base_entity_id_column.render(), ",".join(
                        str(entity_id) for entity_id in entities)))
            where_parts.append(condition)

        if where_parts:
            where_clause = ands(where_parts)
        else:
            where_clause = None

        select = Select(
            partition_columns, with_query=with_query, from_=from_item,
            where_=where_clause, limit=limit)

        query = select.render()

        try:
            cursor.execute(query, params)
        except psycopg2.ProgrammingError as exc:
            msg = "{} in query: {}".format(exc, cursor.mogrify(query, params))
            raise Exception(msg)
        else:
            all_rows.extend(cursor.fetchall())

    return all_rows


def retrieve_related(
        conn, schema, relation_table_name, table_names,
        trend_names, start, end, subquery_filter=None, limit=None):
    """
    Retrieve data for entities of another entity type of trend data. Related
    entities are found via relation table.

    Example: retrieve utrancell data for a set of cell entities (very useful
        in GIS)

    :param conn: Minerva database connection
    :param datasource: A DataSource object
    :param granularity_period: The granularity period in seconds

    :param trend_names: A list of trend names
    (possibly for different datasources)
    :param start: The start timestamp of the range of trend values
    :param end: The end timestamp of the range of trend values
    :param subquery_filter: optional subquery for additional filtering
        by JOINing on field 'id' = entity_id
    :param limit: Optional limit
    """
    where_parts = []
    all_rows = []

    # group tables by partition size signature to
    # be able to JOIN them later
    tables_by_partition_signature = {}
    for table_name in table_names:
        signature = table_name.split("_")[-1]
        tables_by_partition_signature.setdefault(
            signature, []).append(table_name)

    for table_names in tables_by_partition_signature.values():
        full_base_tbl_name = "{0}.\"{1}\"".format(schema, table_names[0])

        query = (
            "SELECT r.source_id, r.target_id, {1}.\"timestamp\", {0} "
            "FROM {1} ").format(
                ", ".join(map(enquote_column_name, trend_names)),
                full_base_tbl_name)

        join_parts = []
        for table_name in table_names[1:]:
            full_tbl_name = "{0}.\"{1}\"".format(schema, table_name)
            join_parts.append(
                " JOIN {0} ON {0}.\"timestamp\" = {1}.\"timestamp\" AND "
                "{0}.entity_id = {1}.entity_id "
            ).format(full_tbl_name, full_base_tbl_name)

        join_parts.append(
            " JOIN relation.\"{0}\" r ON r.target_id = {1}.entity_id ".format(
                relation_table_name, full_base_tbl_name))

        if subquery_filter:
            join_parts.append(
                " JOIN ({0}) AS filter ON filter.id = r.source_id ".format(
                    subquery_filter))

        query += "".join(join_parts)

        where_parts = []

        params = []
        if start == end and start is not None:
            where_parts.append(
                "{0}.\"timestamp\" = %s".format(full_base_tbl_name))
            params = [start]
        else:
            if start is not None:
                where_parts.append(
                    "{0}.\"timestamp\" > %s".format(full_base_tbl_name))
                params.append(start)

            if end is not None:
                where_parts.append(
                    "{0}.\"timestamp\" <= %s".format(full_base_tbl_name))
                params.append(end)

        if where_parts:
            query += " WHERE " + " AND ".join(where_parts)

        if limit is not None:
            query += " LIMIT {0:d}".format(limit)

        with closing(conn.cursor()) as cursor:
            try:
                cursor.execute(query, params)
            except psycopg2.ProgrammingError:
                logging.debug(cursor.mogrify(query, params))
                conn.rollback()
                # TODO: Check error code
            else:
                all_rows.extend(cursor.fetchall())

    return all_rows


def retrieve_orderedby_time(cursor, tables, columns, entities, start, end):
    all_rows = retrieve(cursor, tables, columns, entities, start, end)
    all_rows.sort(key=itemgetter(1))

    return all_rows
