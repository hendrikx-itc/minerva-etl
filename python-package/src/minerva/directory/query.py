# -*- coding: utf-8 -*-

from future_builtins import map

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from operator import itemgetter
from contextlib import closing

import pyparsing
from pyparsing import alphanums, ZeroOrMore, Optional

from minerva.util import first, compose
from minerva.directory.helpers import get_entitytype_by_id, \
    get_relationtype_id, NoSuchRelationTypeError
from minerva.directory.query_types import Tag, Alias, Context, Query


class QueryError(Exception):
    pass


def parser():
    tag = pyparsing.Word(alphanums).setParseAction(compose(Tag, first))

    alias = pyparsing.Word(alphanums).setParseAction(compose(Alias, first))

    op_and = pyparsing.Keyword("+").suppress()

    context = pyparsing.operatorPrecedence(tag, [
        (op_and, 2, pyparsing.opAssoc.LEFT)]).setParseAction(
        compose(Context, first))

    cs_pair = context + alias

    q = ZeroOrMore(cs_pair) + Optional(context)

    q.setParseAction(Query)

    return q


parse = parser().parseString


def select(conn, minerva_query, relation_group_name):
    q, args, entity_id_column = compile_sql(minerva_query, relation_group_name)

    sql = "SELECT {} {}".format(entity_id_column, q)

    with closing(conn.cursor()) as cursor:
        cursor.execute(sql, args)

        rows = cursor.fetchall()

    return map(itemgetter(0), rows)


def compile_sql(minerva_query, relation_group_name, entity_id_column=None):
    """
    Compile SQL for selecting entities based on a Minerva Query.

    :param minerva_query: A Minerva Query instance.
    :param entity_id_column: The column containing an entity Id to start
    joining on.
    :returns: A tuple (sql, args, entity_id_column)
    """
    args = []
    query_parts = []

    if not minerva_query or minerva_query[0]["type"] != 'C':
        raise QueryError("query must start with a context(tag)")

    first_component = minerva_query[0]

    if entity_id_column:
        sql, entity_id_column = make_c_join(0, entity_id_column)
    else:
        sql, entity_id_column = make_c_from()

    query_parts.append(sql)
    args.append(tuple(map(str.lower, first_component['value'])))

    for index, component in enumerate(minerva_query[1:], start=1):
        if component['type'] == 'C':
            sql, entity_id_column = make_relation_join(
                index, entity_id_column, relation_group_name)

            query_parts.append(sql)

            sql = make_c_join(index, entity_id_column)

            query_parts.append(sql)
            args.append(tuple(map(str.lower, component['value'])))

        elif component['type'] == 'S':
            sql = make_s_join(index, entity_id_column)

            query_parts.append(sql)
            args.append(component['value'].lower())

        else:
            raise QueryError('query contains unknown type ' + component['type'])

    sql = " ".join(query_parts)

    return sql, args, entity_id_column


def get_entities_by_query(conn, minerva_query, relation_group_name):
    q, args, entity_id_column = compile_sql(minerva_query, relation_group_name)

    sql = (
        "SELECT entity.id, entity.dn, entity.entitytype_id "
        "{0} "
        "JOIN directory.entity entity ON entity.id = {1}").format(
        q, entity_id_column)

    with closing(conn.cursor()) as cursor:
        cursor.execute(sql, args)

        rows = cursor.fetchall()

    attr_names = ("id", "dn", "entitytype_id")

    return [dict(zip(attr_names, row)) for row in rows]


def get_related_entities_by_query(conn, minerva_query, relation_group_name,
                                  target_entitytype_id):
    # Quick Hack: get_entities_by_query -> get_related_entities on result
    entities = get_entities_by_query(conn, minerva_query, relation_group_name)
    attr_names = ("id", "dn", "entitytype_id")

    related_entities = []

    target_entitytype = get_entitytype_by_id(conn, target_entitytype_id)

    for entity in entities:
        if entity["entitytype_id"] == target_entitytype_id:
            related_entities.append(entity)
        else:
            source_entitytype = get_entitytype_by_id(conn,
                                                     entity["entitytype_id"])

            relationtype_name = "{}->{}".format(source_entitytype.name,
                                                target_entitytype.name)

            try:
                get_relationtype_id(conn, relationtype_name)
            except NoSuchRelationTypeError:
                continue
            else:
                query = (
                    "SELECT target_id, e.dn, e.entitytype_id "
                    "FROM relation.\"{0}\" "
                    "JOIN directory.entity e ON e.id = target_id "
                    "AND e.entitytype_id = %s "
                    "WHERE source_id = %s").format(relationtype_name)

                with closing(conn.cursor()) as cursor:
                    cursor.execute(query, (target_entitytype_id, entity["id"]))

                    rows = cursor.fetchall()

                if rows is not None:
                    related_entities.extend([dict(zip(attr_names, row))
                                             for row in rows])

    return related_entities


def iter_cs(query):
    full_pair_count, remainder = divmod(len(query), 2)

    for i in range(full_pair_count):
        yield query[i * 2]["value"], query[(i * 2) + 1]["value"]

    if remainder > 0:
        yield query[-1]["value"], None


def make_relation_join(index, entity_id_column, relation_group_name):
    relation_alias = "r_{0}".format(index)
    type_alias = "type_{0}".format(index)
    group_alias = "g_{0}".format(index)

    sql = (
        "\nJOIN relation.all_materialized {0} ON {0}.source_id = {1} "
        "\nJOIN relation.type {2} ON {0}.type_id = {2}.id "
        "\nJOIN relation.group {3} "
        "\n    ON {2}.group_id = {3}.id "
        "\n    AND {3}.name = '{4}'"
    ).format(
        relation_alias, entity_id_column, type_alias, group_alias,
        relation_group_name
    )
    entity_id_column = "{0}.target_id".format(relation_alias)

    return sql, entity_id_column


def make_c_from():
    sql = (
        '\nFROM directory.entitytaglink_tagarray t_first'
        '\nJOIN directory.tag tag_first'
        '\n    ON ARRAY[tag_first.id] @> t_first.tag_ids'
        '\n    AND lower(tag_first.name) IN %s'
    )

    return sql, 't_first.entity_id'


def make_c_join(index, entity_id_column):
    tag_alias = "t_{0}".format(index)
    taglink_alias = "tl_{0}".format(index)

    return (
        '\nJOIN directory.entitytaglink_tagarray {0}'
        '\n    ON {2} = {0}.entity_id'
        '\nJOIN directory.tag {1}'
        '\n    ON ARRAY[{1}.id] @> {0}.tag_ids'
        '\n    AND lower({1}.name) IN %s'
    ).format(taglink_alias, tag_alias, entity_id_column)


def make_s_join(index, entity_id_column):
    alias_alias = "a_{}".format(index)
    aliastype_alias = "at_{}".format(index)

    return (
        "\nJOIN directory.alias {0}"
        "\n    ON {0}.entity_id = {1} AND lower({0}.name) = %s"
        "\nJOIN directory.aliastype {2}"
        "\n    ON {2}.id = {0}.type_id"
        "\n    AND {2}.name = 'name'"
    ).format(alias_alias, entity_id_column, aliastype_alias)
