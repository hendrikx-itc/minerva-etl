from typing import Dict, Callable, List
import os
import argparse
import json
import re
from itertools import chain
from collections import OrderedDict

import yaml

AGGREGATION_INTERVAL = {
    '900': {'interval': '900'},
    '1800': {'interval': '1800'},
    '3600': {'interval': '3600'},
    'day': {'interval': '1 day'},
    'week': {'interval': '1 week'},
    'month': {'interval': '1 month'}
}


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'aggregation', help='commands for defining aggregations'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_time_parser(cmd_subparsers)
    setup_entity_parser(cmd_subparsers)


def setup_entity_parser(subparsers):
    cmd = subparsers.add_parser(
        'entity', help='define entity aggregation'
    )

    cmd.add_argument(
        '--aggregate-name', help='Name of aggregate to create'
    )

    cmd.add_argument(
        '--data-source', help="Name of target data source"
    )

    cmd.add_argument(
        '--map-name', nargs='*', help='Name mapping for part and aggregate'
    )

    cmd.add_argument(
        '--format', choices=['yaml', 'json'], default='yaml',
        help='format of definition'
    )

    cmd.add_argument(
        'relation', help='Use specified relation to map entities'
    )

    cmd.add_argument(
        'definition',
        help='Trend store definition file'
    )

    cmd.set_defaults(cmd=entity_aggregation)


def entity_aggregation(args):
    instance_root = os.environ.get('INSTANCE_ROOT') or os.getcwd()

    relation = load_relation(instance_root, args.relation)

    if args.map_name:
        mapping = dict(
            map_name.split(':')
            for map_name in args.map_name
        )

        map_name = lambda name: mapping[name]
    else:
        map_name = part_name_mapper_entity(
            new_data_source=args.data_source,
            new_entity_type=relation['target_entity_type']
        )

    with open(args.definition) as definition_file:
        if args.format == 'json':
            definition = json.load(definition_file)
        elif args.format == 'yaml':
            definition = ordered_load(definition_file, Loader=yaml.SafeLoader)

    if args.aggregate_name:
        base_name = args.aggregate_name
    else:
        name, ext = os.path.splitext(os.path.basename(args.definition))
        base_name = map_name(name)

    materialization_file_path = os.path.join(
        instance_root,
        'materialization',
        '{}.sql'.format(base_name)
    )

    print("Writing materialization to '{}'".format(materialization_file_path))

    sql_lines = define_entity_aggregation(definition, args.relation, map_name)

    with open(materialization_file_path, 'w') as out_file:
        out_file.writelines(sql_lines)

    aggregate_definition = define_aggregate_trend_store(
        definition, map_name, target_data_source=args.data_source,
        target_entity_type=relation['target_entity_type']
    )

    aggregate_trend_store_file_path = os.path.join(
        instance_root,
        'trend',
        '{}.yaml'.format(base_name)
    )

    print("Writing aggregate trend store to '{}'".format(
        aggregate_trend_store_file_path
    ))

    with open(aggregate_trend_store_file_path, 'w') as out_file:
        ordered_dump(aggregate_definition, stream=out_file, Dumper=yaml.SafeDumper, indent=2)


def ordered_load(stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict):
    class OrderedLoader(Loader):
        pass

    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))

    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping)

    return yaml.load(stream, OrderedLoader)


def ordered_dump(data, stream=None, Dumper=yaml.Dumper, **kwds):
    class OrderedDumper(Dumper):
        pass

    def _dict_representer(dumper, data):
        return dumper.represent_mapping(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            data.items())

    OrderedDumper.add_representer(OrderedDict, _dict_representer)

    return yaml.dump(data, stream, OrderedDumper, **kwds)


def load_relation(instance_root: str, relation: str) -> Dict:
    """
    :param instance_root:
    :param relation: Can be an absolute path, or a filename (with or without
    extension) relative to relation directory in instance root.
    :return:
    """
    path_variants = [
        relation,
        os.path.join(instance_root, 'relation', relation),
        os.path.join(instance_root, 'relation', '{}.yaml'.format(relation))
    ]

    try:
        yaml_file_path = next(
            path for path in path_variants if os.path.isfile(path)
        )
    except StopIteration:
        raise Exception("No such relation '{}'".format(relation))

    print("Using relation definition '{}'".format(yaml_file_path))

    with open(yaml_file_path) as yaml_file:
        return yaml.load(yaml_file, Loader=yaml.SafeLoader)


def define_entity_aggregation(
        data: Dict, relation: str, part_name_mapping: Callable[[str], str]
) -> List[str]:
    """
    Define the aggregations for all parts of the trend store

    :param data:
    :param relation:
    :param part_name_mapping: A function for mapping part name to aggregate part
     name
    :return: Lines of SQL
    """
    return list(
        chain(*(
            define_part_aggregation(
                part, relation, part_name_mapping(part['name'])
            )
            for part in data['parts']
        ))
    )


def define_part_aggregation(data, relation, name):
    return chain(
        aggregate_view_sql(data, relation, name),
        ['\n'],
        define_view_materialization_sql(data, name),
        ['\n'],
        link_trend_store_sql(name),
        ['\n'],
        define_fingerprint_sql(data, name)
    )


def aggregate_view_sql(data, relation, name):
    trend_columns = [
        '    {}(t."{}") AS "{}"'.format(
            trend['entity_aggregation'].lower(),
            trend['name'],
            trend['name']
        )
        for trend in data['trends']
    ]

    query_parts = [
        'CREATE VIEW trend."_{}" AS\n'.format(name),
        'SELECT\n',
        '  r.target_id AS entity_id,\n',
        '  timestamp,\n',
        '  count(*) AS samples,\n'
    ]

    query_parts.append(
        ',\n'.join(trend_columns)
    )

    query_parts.extend([
        '\nFROM trend."{}" t\n'.format(data['name']),
        'JOIN relation."{}" r ON t.entity_id = r.source_id\n'.format(relation),
        'GROUP BY timestamp, r.target_id;\n'
    ])

    return query_parts


def define_view_materialization_sql(data, name):
    return [
        'SELECT trend_directory.define_view_materialization(\n',
        "    id, '30m'::interval, '5m'::interval, '3 days'::interval, 'trend._{}'::regclass\n".format(name),
        ')\n',
        'FROM trend_directory.trend_store_part\n',
        "WHERE name = '{}';\n".format(name)
    ]


def link_trend_store_sql(name):
    return [
        "INSERT INTO trend_directory.materialization_trend_store_link(materialization_id, trend_store_part_id, timestamp_mapping_func)\n",
        "SELECT m.id, ttsp.id, 'trend.mapping_id(timestamp with time zone)'::regprocedure\n",
        "FROM trend_directory.materialization m, trend_directory.trend_store_part ttsp\n",
        "WHERE m::text = 'u2020-pm_v-cell_900' and ttsp in ('{}');\n".format(name)
    ]


def define_fingerprint_sql(data, name):
    return [
        'CREATE OR REPLACE FUNCTION trend."{}_fingerprint"(timestamp with time zone)\n'.format(name),
        'RETURNS trend_directory.fingerprint\n',
        'AS $$\n',
        '  SELECT modified.last, format(\'{{"{}": "%s"}}\', modified.last)::jsonb\n'.format(data['name']),
        '  FROM trend_directory.modified\n',
        '  JOIN trend_directory.trend_store_part ttsp ON ttsp.id = modified.trend_store_part_id\n',
        "  WHERE ttsp::name = '{}' AND modified.timestamp = $1;\n".format(data['name']),
        '$$ LANGUAGE sql STABLE;\n'
    ]


AGGREGATE_DATA_TYPE_MAPPING = {
    'smallint': 'bigint',
    'integer': 'bigint',
    'bigint': 'numeric',
    'float': 'double precision',
    'double precision': 'double precision',
    'real': 'double precision',
    'numeric': 'numeric'
}

PARTITION_SIZE_MAPPING = {
    '15m': '1d',
    '30m': '2d',
    '1h': '4d',
    '1d': '3month',
    '1w': '1y',
    '1month': '5y',
}


def define_aggregate_trend_store(
        data, map_part_name, target_data_source=None, target_entity_type=None,
        target_granularity=None):

    if target_data_source is None:
        target_data_source = data['data_source']

    if target_granularity is None:
        target_granularity = data['granularity']

    if target_entity_type is None:
        target_entity_type = data['entity_type']

    parts = [
        define_aggregate_part(part, map_part_name) for part in data['parts']
    ]

    aggregate_data = OrderedDict([
        ('data_source', target_data_source),
        ('entity_type', target_entity_type),
        ('granularity', target_granularity),
        ('partition_size', PARTITION_SIZE_MAPPING[target_granularity]),
        ('parts', parts)
    ])

    return aggregate_data


def define_aggregate_part(data, map_part_name):
    trends = [
        define_aggregate_trend(trend) for trend in data['trends']
    ]

    # If there is no samples column, we add one
    if not len([trend for trend in trends if trend['name'] == 'samples']):
        trends.insert(0, OrderedDict([
            ('name', 'samples'),
            ('data_type', 'smallint'),
            ('time_aggregation', 'sum'),
            ('entity_aggregation', 'sum'),
            ('extra_data', {})
        ]))

    return OrderedDict([
        ('name', map_part_name(data['name'])),
        ('trends', trends)
    ])


def aggregate_data_type(data_type):
    return AGGREGATE_DATA_TYPE_MAPPING.get(data_type, data_type)


def define_aggregate_trend(data):
    return OrderedDict([
        ('name', data['name']),
        ('data_type', aggregate_data_type(data['data_type'])),
        ('time_aggregation', data['time_aggregation']),
        ('entity_aggregation', data['entity_aggregation']),
        ('extra_data', data['extra_data'])
    ])


def part_name_mapper_entity(
        new_data_source=None, new_entity_type=None, new_granularity=None):
    """
    Map part names by replacing components of the name

    :return: Mapped name
    """
    def map_part_name(name):
        match = re.match('(.*)_(.*)_([1-9][0-8]*[mhdw])', name)

        data_source, entity_type, granularity = match.groups()

        if new_data_source is not None:
            data_source = new_data_source

        if new_entity_type is not None:
            entity_type = new_entity_type

        if new_granularity is not None:
            granularity = new_granularity

        return '{}_{}_{}'.format(data_source, entity_type, granularity)

    return map_part_name


def setup_time_parser(subparsers):
    cmd = subparsers.add_parser(
        'time', help='define time aggregation'
    )

    cmd.add_argument(
        '--format', choices=['yaml', 'json'], default='yaml',
        help='format of definition'
    )

    cmd.add_argument(
        'definition', type=argparse.FileType('r'),
        help='file containing relation definition'
    )

    cmd.set_defaults(cmd=time_aggregation)


def time_aggregation(args):
    instance_root = os.environ.get('INSTANCE_ROOT') or os.getcwd()

    with open(args.definition) as definition_file:
        if args.format == 'json':
            definition = json.load(definition_file)
        elif args.format == 'yaml':
            definition = ordered_load(definition_file, Loader=yaml.SafeLoader)

    map_name = part_name_mapper_time(args.aggregation)

    if args.aggregate_name:
        base_name = args.aggregate_name
    else:
        name, ext = os.path.splitext(os.path.basename(args.input))
        base_name = map_name(name)

    materialization_file_path = os.path.join(
        instance_root,
        'materialization',
        '{}.sql'.format(base_name)
    )

    print("Writing materialization to '{}'".format(materialization_file_path))

    sql_lines = define_aggregation(definition, args.aggregation, map_name)

    with open(materialization_file_path, 'w') as out_file:
        out_file.writelines(sql_lines)

    target_granularity = AGGREGATION_INTERVAL[args.aggregation]['interval']
    aggregate_data = define_aggregate_trend_store(
        definition, map_name, target_granularity=target_granularity
    )

    aggregate_trend_store_file_path = os.path.join(
        instance_root,
        'trend',
        '{}.json'.format(base_name)
    )

    print("Writing aggregate trend store to '{}'".format(
        aggregate_trend_store_file_path
    ))

    with open(aggregate_trend_store_file_path, 'w') as out_file:
        json.dump(aggregate_data, out_file, indent=2)


def part_name_mapper_time(new_suffix):
    """
    Map part names by cutting off the existing granularity suffix and appending
    the new suffix.

    :param new_suffix:
    :return: Mapped name
    """
    def map_part_name(name):
        # Strip existing suffix
        without_suffix = re.sub('_([1-9][0-8]*[mhdw])', '', name)

        return '{}_{}'.format(without_suffix, new_suffix)

    return map_part_name


def define_aggregation(
        data: Dict, aggregation: str, part_name_mapping: Callable[[str], str]
) -> List[str]:
    """
    Define the aggregations for all parts of the trend store

    :param data:
    :param part_name_mapping: A function for mapping part name to aggregate part
     name
    :return: Lines of SQL
    """
    return list(
        chain(*(
            define_part_time_aggregation(part, data['granularity'], aggregation, part_name_mapping(part['name']))
            for part in data['parts']
        ))
    )


def define_part_time_aggregation(part_data: Dict, source_granularity, aggregation: str, name: str) -> List[str]:
    """
    Use the source part definition to generate the aggregation SQL.

    :param part_data:
    :param name: Name of the aggregate part use for the function name, etc.
    :return: Lines of SQL
    """
    return (
        aggregate_function_sql(part_data, aggregation, name) + ['\n'] +
        define_materialization_sql(part_data, aggregation, name) + ['\n'] +
        define_trend_store_link(part_data, source_granularity, aggregation, name) + ['\n'] +
        define_timestamp_function(part_data, source_granularity, aggregation, name)
    )


def aggregate_function_sql(part_data, aggregation, target_name):
    target_granularity = AGGREGATION_INTERVAL[aggregation]['interval']

    trends = part_data['trends']

    trend_columns = [
        '    "{}" {}'.format(
            trend['name'],
            aggregate_data_type(trend['data_type'])
        )
        for trend in trends
    ]

    trend_column_expressions = [
        '      {}(t."{}") AS "{}"'.format(
            trend['time_aggregation'].lower(),
            trend['name'],
            trend['name']
        )
        for trend in trends
    ]

    column_expressions = [
        '      entity_id',
        '      $2 AS timestamp'
    ] + trend_column_expressions

    return [
        'CREATE OR REPLACE FUNCTION trend."{}"(timestamp with time zone)\n'.format(target_name),
        '  RETURNS TABLE (\n',
        '    "entity_id" integer,\n',
        '    "timestamp" timestamp with time zone,\n',
        ',\n'.join(trend_columns),
        '\n',
        '  )\n',
        'AS $$\n',
        'BEGIN\n',
        'RETURN QUERY EXECUTE $query$\n',
        '    SELECT\n',
        ',\n'.join(expr for expr in column_expressions),
        '\n',
        '    FROM trend."{}" AS t\n'.format(part_data['name']),
        '    WHERE $1 < timestamp AND timestamp <= $2\n',
        '    GROUP BY entity_id\n',
        "$query$ USING $1 - interval '{}', $1;\n".format(target_granularity),
        'END;\n',
        '$$ LANGUAGE plpgsql STABLE;\n'
    ]


def define_materialization_sql(part_data, aggregation, target_name):
    return [
        'SELECT trend_directory.define_function_materialization(\n',
        "    id, '30m'::interval, '5m'::interval, '3 days'::interval, 'trend.{}(timestamp with time zone)'::regprocedure\n".format(target_name),
        ')\n',
        'FROM trend_directory.trend_store_part\n',
        "WHERE name = '{}';\n".format(target_name),
    ]


def define_trend_store_link(part_data, source_granularity, aggregation, target_name):
    mapping_function = 'mapping_{}->{}'.format(source_granularity, aggregation)

    return [
        'INSERT INTO trend_directory.materialization_trend_store_link(materialization_id, trend_store_part_id, timestamp_mapping_func)\n',
        "SELECT m.id, ttsp.id, 'trend.{}(timestamp with time zone)'::regprocedure\n".format(mapping_function),
        'FROM trend_directory.materialization m, trend_directory.trend_store_part ttsp\n',
        "WHERE m::text = '{}' and ttsp.name = '{}';\n".format(
            target_name, part_data['name']
        )
    ]


def define_timestamp_function(part_data, source_granularity, aggregation: str, target_name: str):
    target_granularity = AGGREGATION_INTERVAL[aggregation]['interval']

    return [
        'CREATE OR REPLACE FUNCTION trend."{}_fingerprint"(timestamp with time zone)\n'.format(target_name),
        '  RETURNS trend_directory.fingerprint\n',
        'AS $$\n',
        "  SELECT max(modified.last), format('{%s}', string_agg(format('\"%s\":\"%s\"', t, modified.last), ','))::jsonb\n",
        "  FROM generate_series($1 - interval '{target_granularity}' + interval '{source_granularity}', $1, interval '{source_granularity}') t\n".format(
            target_granularity=target_granularity,
            source_granularity=source_granularity
        ),
        '  LEFT JOIN (\n'
        '    SELECT timestamp, last\n'
        '    FROM trend_directory.trend_store_part part\n'
        '    JOIN trend_directory.modified ON modified.trend_store_part_id = part.id\n'
        '    WHERE part.name = \'{}\'\n'
        '  ) modified ON modified.timestamp = t;\n'.format(part_data['name']),
        '$$ LANGUAGE sql STABLE;\n'
    ]
