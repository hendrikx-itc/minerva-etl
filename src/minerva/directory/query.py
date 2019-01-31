# -*- coding: utf-8 -*-
from operator import itemgetter
from contextlib import closing

from minerva.directory import EntityType


class QueryError(Exception):
    pass


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

    if not minerva_query or minerva_query[0]['type'] not in ['C', 'any C']:
        raise QueryError("query must start with a context(tag)")

    first_component = minerva_query[0]

    if first_component['type'] == 'C':
        if entity_id_column:
            query_part, eld_alias = make_c_join(0, entity_id_column)
        else:
            query_part, entity_id_column, eld_alias = make_c_from()
    else:  # type == 'any C'
        if entity_id_column:
            query_part, eld_alias = make_any_c_join(0, entity_id_column)
        else:
            query_part, entity_id_column, eld_alias = make_any_c_from()

    query_parts.append(query_part)
    args.append([x.lower() for x in first_component['value']])

    last_type = first_component['type']

    for index, component in enumerate(minerva_query[1:], start=1):
        if component['type'] == 'C':
            if last_type == 'any C':
                query_part = make_c_and(eld_alias)

                query_parts.append(query_part)
                args.append([x.lower() for x in component['value']])
            else:
                query_part, entity_id_column = make_relation_join(
                    index, entity_id_column, relation_group_name)

                query_parts.append(query_part)

                query_part, eld_alias = make_c_join(index, entity_id_column)

                query_parts.append(query_part)
                args.append([x.lower() for x in component['value']])

        elif component['type'] == 'any C':
            query_part, entity_id_column = make_relation_join(
                index, entity_id_column, relation_group_name)

            query_parts.append(query_part)

            query_part, eld_alias = make_any_c_join(index, entity_id_column)

            query_parts.append(query_part)
            args.append([x.lower() for x in component['value']])

        elif component['type'] == 'S':
            query_part = make_s_join(index, eld_alias)

            query_parts.append(query_part)
            args.append(component['value'].lower())

        else:
            raise QueryError(
                'query contains unknown type ' + component['type'])

        last_type = component['type']

    sql = "\n".join(query_parts)

    return sql, args, entity_id_column


def get_entities_by_query(conn, minerva_query, relation_group_name):
    q, args, entity_id_column = compile_sql(minerva_query, relation_group_name)

    sql = (
        " SELECT entity.id, entity.name, entity.entity_type_id"
        " {0}"
        " JOIN directory.entity entity ON entity.id = {1}").format(
        q, entity_id_column)

    with closing(conn.cursor()) as cursor:
        cursor.execute(sql, args)

        rows = cursor.fetchall()

    attr_names = ("id", "name", "entity_type_id")

    return [dict(zip(attr_names, row)) for row in rows]


def get_entity_tags_by_query(cursor, minerva_query, relation_group_name):
    if len(minerva_query) == 0:
        return []

    query_part, args, entity_id_column = compile_sql(
        minerva_query, relation_group_name)

    sql = (
        " SELECT etl.tag_id"
        " {0}"
        " JOIN directory.entity_tag_link etl ON etl.entity_id = {1}"
        " GROUP BY etl.tag_id"
    ).format(query_part, entity_id_column)

    cursor.execute(sql, args)

    rows = cursor.fetchall()

    return [entity_tag for entity_tag, in rows]


def get_related_entities_by_query(
        conn, minerva_query, relation_group_name, target_entity_type_id):
    # Quick Hack: get_entities_by_query -> get_related_entities on result
    entities = get_entities_by_query(conn, minerva_query, relation_group_name)
    attr_names = ("id", "name", "entity_type_id")

    related_entities = []

    with closing(conn.cursor()) as cursor:
        target_entity_type = EntityType.get(target_entity_type_id)(cursor)

    for entity in entities:
        if entity["entity_type_id"] == target_entity_type_id:
            related_entities.append(entity)
        else:
            with closing(conn.cursor()) as cursor:
                source_entity_type = EntityType.get(
                    entity["entity_type_id"]
                )(cursor)

            relation_type_name = "{}->{}".format(
                source_entity_type.name, target_entity_type.name
            )

            query = (
                " SELECT target_id, e.name, e.entity_type_id"
                " FROM relation.\"{0}\""
                " JOIN directory.entity e ON e.id = target_id"
                " AND e.entity_type_id = %s"
                " WHERE source_id = %s").format(relation_type_name)

            with closing(conn.cursor()) as cursor:
                cursor.execute(query, (target_entity_type_id, entity["id"]))

                rows = cursor.fetchall()

            if rows is not None:
                related_entities.extend(
                    [dict(zip(attr_names, row)) for row in rows]
                )

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

    query_part = (
        " JOIN relation.all_materialized {0} ON {0}.source_id = {1} "
        " JOIN relation.type {2} ON {0}.type_id = {2}.id "
        " JOIN relation.group {3} "
        " ON {2}.group_id = {3}.id "
        " AND {3}.name = '{4}'"
    ).format(
        relation_alias, entity_id_column, type_alias, group_alias,
        relation_group_name
    )
    entity_id_column = "{0}.target_id".format(relation_alias)

    return query_part, entity_id_column


def make_c_from():
    query_part = (
        ' FROM (VALUES(NULL)) dummy'
        ' JOIN directory.entity_tag_link_denorm eld'
        ' ON %s <@ eld.tags'
    )

    return query_part, 'eld.entity_id', 'eld'


def make_c_join(index, entity_id_column):
    tag_link_alias = "eld_{0}".format(index)

    query_part = (
        ' JOIN directory.entity_tag_link_denorm {0}'
        ' ON {1} = {0}.entity_id'
        ' AND %s <@ {0}.tags'
    ).format(tag_link_alias, entity_id_column)

    return query_part, tag_link_alias


def make_c_and(tag_link_alias):
    query_part = (
        " AND %s <@ {0}.tags"
    ).format(
        tag_link_alias
    )

    return query_part


def make_any_c_from():
    query_part = (
        ' FROM (VALUES(NULL)) dummy'
        ' JOIN directory.entity_tag_link_denorm eld'
        ' ON %s && eld.tags'
    )

    return query_part, 'eld.entity_id', 'eld'


def make_any_c_join(index, entity_id_column):
    tag_link_alias = "eld_{0}".format(index)

    query_part = (
        ' JOIN directory.entity_tag_link_denorm {0}'
        ' ON {1} = {0}.entity_id'
        ' AND %s && {0}.tags'
    ).format(tag_link_alias, entity_id_column)

    return query_part, tag_link_alias


def make_s_join(index, eld_alias):
    return ' AND {0}.name = %s'.format(eld_alias)
