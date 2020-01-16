from typing import Dict, List
import os
import json
import re
from itertools import chain
from collections import OrderedDict

import yaml

from minerva.commands import ConfigurationError, INSTANCE_ROOT_VARIABLE


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
        '--format', choices=['yaml', 'json'], default='yaml',
        help='format of definition'
    )

    cmd.add_argument(
        'definition',
        help='Aggregations definition file'
    )

    cmd.set_defaults(cmd=entity_aggregation)


def entity_aggregation(args):
    instance_root = os.environ.get(INSTANCE_ROOT_VARIABLE) or os.getcwd()

    with open(args.definition) as definition_file:
        if args.format == 'json':
            definition = json.load(definition_file)
        elif args.format == 'yaml':
            definition = ordered_load(definition_file, Loader=yaml.SafeLoader)

    source_definition_file_path = os.path.join(
        instance_root,
        'trend',
        '{}.yaml'.format(definition['entity_aggregation']['source'])
    )

    with open(source_definition_file_path) as source_definition_file:
        source_definition = ordered_load(
            source_definition_file, Loader=yaml.SafeLoader
        )

    relation = load_relation(
        instance_root, definition['entity_aggregation']['relation']
    )

    if definition['entity_aggregation']['entity_type'] != relation['target_entity_type']:
        raise ConfigurationError(
            'Entity type mismatch between definition and relation target: {} != {}'.format(
                definition['entity_aggregation']['entity_type'],
                relation['target_entity_type']
            )
        )

    write_entity_aggregations(
        instance_root, source_definition, definition['entity_aggregation']
    )

    aggregate_definition = define_aggregate_trend_store(
        source_definition, definition['entity_aggregation']
    )

    base_name = definition['entity_aggregation']['name']

    aggregate_trend_store_file_path = os.path.join(
        instance_root,
        'trend',
        '{}.yaml'.format(base_name)
    )

    print("Writing aggregate trend store to '{}'".format(
        aggregate_trend_store_file_path
    ))

    with open(aggregate_trend_store_file_path, 'w') as out_file:
        ordered_dump(
            aggregate_definition, stream=out_file, Dumper=yaml.SafeDumper,
            indent=2
        )


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


def write_entity_aggregations(
        instance_root: str, data: Dict, definition
) -> List[str]:
    """
    Define the aggregations for all parts of the trend store

    :param data:
    :param relation:
    :param part_name_mapping: A function for mapping part name to aggregate part
     name
    :return: Lines of SQL
    """
    for part in data['parts']:
        dest_part = next(
            dest_part
            for dest_part in definition['parts']
            if dest_part['source'] == part['name']
        )

        sql_lines = list(define_part_aggregation(
            part,
            definition['relation'],
            dest_part['name']
        ))

        materialization_file_path = os.path.join(
            instance_root,
            'materialization',
            '{}.sql'.format(dest_part['name'])
        )

        print("Writing materialization to '{}'".format(materialization_file_path))

        with open(materialization_file_path, 'w') as out_file:
            out_file.writelines(sql_lines)


def define_part_aggregation(data, relation, name):
    return chain(
        aggregate_view_sql(data, relation, name),
        ['\n'],
        define_view_materialization_sql(data, name),
        ['\n'],
        define_trend_store_link(data['name'], 'mapping_id', name),
        ['\n'],
        define_fingerprint_sql(data, name),
        ['\n'],
        enable_sql(name)
    )


def aggregate_view_sql(data, relation, name):
    trend_columns = [
        '  {}("{}") AS "{}"'.format(
            trend['entity_aggregation'],
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


def enable_sql(name):
    return [
        "UPDATE trend_directory.materialization SET enabled = true "
        "WHERE materialization::text = '{}';\n".format(name)
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


def define_aggregate_trend_store(data, definition):
    if definition.get('data_source') is None:
        target_data_source = data['data_source']
    else:
        target_data_source = definition.get('data_source')

    if definition.get('granularity') is None:
        target_granularity = data['granularity']
    else:
        target_granularity = definition.get('granularity')

    if definition.get('entity_type') is None:
        target_entity_type = data['entity_type']
    else:
        target_entity_type = definition.get('entity_type')

    source_parts = data['parts']

    def get_target_part_for(source_part_name):
        try:
            return next(
                target_part
                for target_part in definition['parts']
                if target_part['source'] == source_part_name
            )
        except StopIteration:
            raise ConfigurationError(
                "No definition found for source part '{}'".format(source_part_name)
            )

    parts = [
        define_aggregate_part(
            part,
            get_target_part_for(part['name'])
        ) for part in source_parts
    ]

    aggregate_data = OrderedDict([
        ('data_source', target_data_source),
        ('entity_type', target_entity_type),
        ('granularity', target_granularity),
        ('partition_size', PARTITION_SIZE_MAPPING[target_granularity]),
        ('parts', parts)
    ])

    return aggregate_data


def define_aggregate_part(data, definition):
    trends = [
        define_aggregate_trend(trend) for trend in data['trends']
    ]

    generated_trends = list(chain(
        data.get('generated_trends', []),
        definition.get('generated_trends', [])
    ))

    # If there is no samples column, we add one
    if not len([trend for trend in trends if trend['name'] == 'samples']):
        trends.insert(0, OrderedDict([
            ('name', 'samples'),
            ('data_type', 'smallint'),
            ('time_aggregation', 'sum'),
            ('entity_aggregation', 'sum'),
            ('extra_data', {})
        ]))
        
    items = [
        ('name', definition['name']),
        ('trends', trends)
    ]

    if generated_trends:
        items.append(('generated_trends', generated_trends))

    return OrderedDict(items)


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
        'definition', help='file containing relation definition'
    )

    cmd.set_defaults(cmd=time_aggregation)


def time_aggregation(args):
    instance_root = os.environ.get(INSTANCE_ROOT_VARIABLE) or os.getcwd()

    with open(args.definition) as definition_file:
        if args.format == 'json':
            definition = json.load(definition_file)
        elif args.format == 'yaml':
            definition = ordered_load(definition_file, Loader=yaml.SafeLoader)

    source_definition_file_path = os.path.join(
        instance_root,
        'trend',
        '{}.yaml'.format(definition['time_aggregation']['source'])
    )

    with open(source_definition_file_path) as source_definition_file:
        source_definition = ordered_load(
            source_definition_file, Loader=yaml.SafeLoader
        )

    write_time_aggregations(
        instance_root, source_definition, definition['time_aggregation']
    )

    aggregate_data = define_aggregate_trend_store(
        source_definition, definition['time_aggregation']
    )

    aggregate_trend_store_file_path = os.path.join(
        instance_root,
        'trend',
        '{}.yaml'.format(definition['time_aggregation']['name'])
    )

    print("Writing aggregate trend store to '{}'".format(
        aggregate_trend_store_file_path
    ))

    with open(aggregate_trend_store_file_path, 'w') as out_file:
        ordered_dump(aggregate_data, out_file, indent=2)


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


def write_time_aggregations(
        instance_root, source_definition: Dict, definition: Dict
) -> List[str]:
    """
    Define the aggregations for all parts of the trend store

    :param source_definition:
    :param part_name_mapping: A function for mapping part name to aggregate part
     name
    :return: Lines of SQL
    """
    for part in source_definition['parts']:
        try:
            dest_part = next(
                dest_part
                for dest_part in definition['parts']
                if dest_part['source'] == part['name']
            )
        except StopIteration:
            raise ConfigurationError(
                "No definition found for source part '{}'".format(part['name'])
            )

        materialization_file_path = os.path.join(
            instance_root,
            'materialization',
            '{}.sql'.format(dest_part['name'])
        )

        print(
            "Writing materialization to '{}'".format(materialization_file_path)
        )

        mapping_function = definition['mapping_function']
        target_granularity = definition['granularity']

        sql_lines = define_part_time_aggregation(
            part, source_definition['granularity'], mapping_function,
            target_granularity, dest_part['name']
        )

        with open(materialization_file_path, 'w') as out_file:
            out_file.writelines(sql_lines)


def define_part_time_aggregation(part_data: Dict, source_granularity, mapping_function, target_granularity, name) -> List[str]:
    """
    Use the source part definition to generate the aggregation SQL.

    :param part_data:
    :param name: Name of the aggregate part use for the function name, etc.
    :return: Lines of SQL
    """
    return (
        aggregate_function_sql(part_data, target_granularity, name) + ['\n'] +
        define_materialization_sql(name) + ['\n'] +
        define_trend_store_link(part_data['name'], mapping_function, name) + ['\n'] +
        define_timestamp_function(part_data, source_granularity, target_granularity, name) +
        ['\n'] + enable_sql(name)
    )


def aggregate_function_sql(part_data, target_granularity, target_name):
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
            trend['time_aggregation'],
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


def define_materialization_sql(target_name):
    return [
        'SELECT trend_directory.define_function_materialization(\n',
        "    id, '30m'::interval, '5m'::interval, '3 days'::interval, 'trend.{}(timestamp with time zone)'::regprocedure\n".format(target_name),
        ')\n',
        'FROM trend_directory.trend_store_part\n',
        "WHERE name = '{}';\n".format(target_name),
    ]


def define_trend_store_link(part_name, mapping_function, target_name):
    return [
        'INSERT INTO trend_directory.materialization_trend_store_link(materialization_id, trend_store_part_id, timestamp_mapping_func)\n',
        "SELECT m.id, tsp.id, 'trend.{}(timestamp with time zone)'::regprocedure\n".format(mapping_function),
        'FROM trend_directory.materialization m, trend_directory.trend_store_part tsp\n',
        "WHERE m::text = '{}' and tsp.name = '{}';\n".format(
            target_name, part_name
        )
    ]


def define_timestamp_function(
        part_data, source_granularity, target_granularity: str,
        target_name: str):
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
