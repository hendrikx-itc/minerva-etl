import re, os
from collections import OrderedDict
from itertools import chain
from pathlib import Path
from typing import List, Optional, Tuple, Union

import yaml
from psycopg2 import sql

from minerva.error import ConfigurationError
from minerva.instance import TrendStorePart, MinervaInstance, TrendStore, GeneratedTrend, Trend, Relation, \
    EntityAggregationType, ENTITY_AGGREGATION_TYPE_MAP
from minerva.instance.generating import translate_entity_aggregation_part_name
from minerva.storage.trend.granularity import str_to_granularity
from minerva.util.yaml import SqlSrc, ordered_yaml_dump


class AggregationContext:
    instance: MinervaInstance
    definition: dict
    source_definition: Optional[TrendStore]
    aggregation_file_path: Path

    def __init__(self, instance, definition: dict, aggregation_file_path: Path):
        self.instance = instance
        self.definition = definition
        self.aggregation_file_path = aggregation_file_path

        self.source_definition_file_path = None
        self.source_definition = self.instance.load_trend_store_by_name(
            self.definition['source']
        )

        self.configuration_check()

    def configuration_check(self):
        raise NotImplementedError()

    def generated_file_header(self) -> str:
        """
        Return a string that can be placed at the start of generated files as
        header.
        """
        relative_aggregation_file_path = self.instance.make_relative(
            self.aggregation_file_path
        )

        path = Path(self.definition['source'])

        if path.is_absolute():
            relative_source_definition_path = self.instance.make_relative(
                path
            )
        else:
            relative_source_definition_path = path

        return (
            '###########################################################################\n'
            '#\n'
            '# This file is automatically generated by the `minerva aggregation` command\n'
            '#\n'
            f'# definition:         {relative_aggregation_file_path}\n'
            f'# source trend store: {relative_source_definition_path}\n'
            '#\n'
            '###########################################################################\n'
        )


class EntityAggregationContext(AggregationContext):

    def configuration_check(self):
        # Check if the relation matches the aggregation
        relation = self.instance.load_relation(self.definition['relation'])

        if self.definition['entity_type'] != relation.target_entity_type:
            raise ConfigurationError(
                'Entity type mismatch between definition and relation target: {} != {}'.format(
                    self.definition['entity_type'],
                    relation.target_entity_type
                )
            )


class AggregationPart:
    name: str
    source: str


class TimeAggregationContext(AggregationContext):
    def configuration_check(self):
        pass


def time_aggregation_key_fn(pair: Tuple[Path, TimeAggregationContext]):
    file_path, aggregation_context = pair
    granularity_str = aggregation_context.definition['granularity']

    return str_to_granularity(granularity_str)


def create_base_name_to_name_translation(aggregation_definition: dict):
    return {element['source']: element['name'] for element in aggregation_definition['parts']}


def compile_entity_aggregation(aggregation_context: EntityAggregationContext):
    aggregation_type = ENTITY_AGGREGATION_TYPE_MAP[aggregation_context.definition['aggregation_type']]

    if aggregation_type is EntityAggregationType.VIEW:
        trend_store = aggregation_context.instance.load_trend_store_by_name(aggregation_context.definition['source'])
        relation = aggregation_context.instance.load_relation(aggregation_context.definition['relation'])
        name_translation = create_base_name_to_name_translation(aggregation_context.definition)

        generate_view_entity_aggregation(aggregation_context.instance.root, trend_store, relation, name_translation)

    elif aggregation_type is EntityAggregationType.VIEW_MATERIALIZATION:
        try:
            write_entity_aggregations(aggregation_context)
        except ConfigurationError as exc:
            print("Error generating aggregation: {}".format(exc))
            return 1

        try:
            aggregate_trend_store = define_aggregate_trend_store(
                aggregation_context
            )
        except ConfigurationError as exc:
            print("Error generating target trend store: {}".format(exc))
            return 1


        base_name = aggregation_context.definition['basename']

        aggregate_trend_store_file_path = aggregation_context.instance.trend_store_file_path(
            base_name
        )

        if os.path.exists(aggregate_trend_store_file_path):
            print("Adding to aggregate trend store '{}'".format(
                aggregate_trend_store_file_path
            ))

            with aggregate_trend_store_file_path.open('r') as in_file:
                existing_trend_store = TrendStore.from_dict(yaml.load(in_file, Loader=yaml.FullLoader))
                aggregate_trend_store.parts += existing_trend_store.parts

            with aggregate_trend_store_file_path.open('w') as out_file:
                out_file.write(aggregation_context.generated_file_header())
                ordered_yaml_dump(
                    aggregate_trend_store.to_dict(), stream=out_file, Dumper=yaml.SafeDumper,
                    indent=2
                )

        else:
            print("Writing aggregate trend store to '{}'".format(
                aggregate_trend_store_file_path
            ))

            with aggregate_trend_store_file_path.open('w') as out_file:
                out_file.write(aggregation_context.generated_file_header())
                ordered_yaml_dump(
                    aggregate_trend_store.to_dict(), stream=out_file, Dumper=yaml.SafeDumper,
                    indent=2
                )


def translate_source_part_name(aggregation_context: EntityAggregationContext, name: str) -> str:
    granularity = aggregation_context.source_definition.granularity

    data_source = aggregation_context.definition['data_source']
    entity_type = aggregation_context.definition['entity_type']

    pattern = f'_([^_]+)_{granularity}$'

    m = re.search(pattern, name)

    if m is None:
        raise ConfigurationError('Could not extract part specific string from {name}')

    part_specific_name = m.group(1)

    return f'{data_source}_{entity_type}_{part_specific_name}_{granularity}'


def generate_view_entity_aggregation(instance_root: Path, trend_store: TrendStore, relation: Relation,
                                     name_translation: dict):
    """
    Generate a plain SQL file that defines a view with an aggregation query
    """
    print(f'Generating entity aggregation views ({len(trend_store.parts)}) for {trend_store}')
    aggregation_directory_path = Path(instance_root, 'custom/post-init/entity-aggregation')

    if not aggregation_directory_path.is_dir():
        aggregation_directory_path.mkdir(parents=True)

    for part in trend_store.parts:
        part_name = name_translation.get(part.name,
                                         translate_entity_aggregation_part_name(part.name, relation.target_entity_type))

        file_name = f'{part_name}.sql'
        out_file_path = Path(aggregation_directory_path, file_name)

        with out_file_path.open('w') as out_file:
            sql = aggregation_view_sql(part_name, part, relation)

            out_file.write(sql)

        print(f"written entity aggregation to '{out_file_path}'")


def aggregation_view_sql(name: str, source_part: TrendStorePart, relation: Relation) -> str:
    return (
        f'CREATE VIEW trend."{name}" AS\n'
        f'{entity_aggregation_query(source_part, relation)};\n'
    )


def write_entity_aggregations(aggregation_context: EntityAggregationContext) -> None:
    """
    Generate and write aggregation materializations for all parts of the trend store

    :param aggregation_context: Complete context for entity aggregation to write
    :return: None
    """
    definition = aggregation_context.definition

    for part in aggregation_context.source_definition.parts:
        try:
            dest_part = next(
                dest_part
                for dest_part in definition['parts']
                if dest_part['source'] == part.name
            )
        except StopIteration:
            # Mapping definition is not found, so do a default mapping based
            # on naming rules defined in function translate_source_part_name
            dest_part = {
                'source': part.name,
                'name': translate_source_part_name(aggregation_context, part.name)
            }

        relations = {
            relation.name: relation
            for relation in aggregation_context.instance.load_relations()
        }

        relation = relations[definition['relation']]

        aggregation = define_part_entity_aggregation(
            part,
            relation,
            dest_part['name']
        )

        materialization_file_path = aggregation_context.instance.materialization_file_path(dest_part['name'])

        print("Writing materialization to '{}'".format(materialization_file_path))

        with materialization_file_path.open('w') as out_file:
            out_file.write(aggregation_context.generated_file_header())

            ordered_yaml_dump(
                aggregation, stream=out_file, Dumper=yaml.SafeDumper,
                indent=2
            )


def define_part_entity_aggregation(part: TrendStorePart, relation: Relation, name: str):
    mapping_function = 'trend.mapping_id'

    return OrderedDict([
        ('target_trend_store_part', name),
        ('enabled', True),
        ('processing_delay', '30m'),
        ('stability_delay', '5m'),
        ('reprocessing_period', '3 days'),
        ('sources', [
            OrderedDict([
                ('trend_store_part', part.name),
                ('mapping_function', mapping_function)
            ])
        ]),
        ('view', SqlSrc(entity_aggregation_query(part, relation))),
        ('fingerprint_function', SqlSrc(define_fingerprint_sql(part)))
    ])


def entity_aggregation_query(part: TrendStorePart, relation: Relation) -> str:
    """
    Return the SQL for a query that aggregates the specified trend store part
    using the specified relation.

    :param part: The trend store part to be aggregated
    :param relation: The entity relation to use
    :return: A query that aggregates the trend store part
    """
    trend_columns = [
        f'  {trend.entity_aggregation}("{trend.name}") AS "{trend.name}"'
        for trend in part.trends
    ]

    columns = [
        '  r.target_id AS entity_id',
        '  timestamp',
    ]

    # Add a samples column if it does not exist in the source part
    if not [c for c in part.trends if c.name == 'samples']:
        columns.append('  count(*) AS samples')

    columns.extend(trend_columns)

    columns_part = ',\n'.join(columns)

    return (
        'SELECT\n'
        f'{columns_part}\n'
        f'FROM trend."{part.name}" t\n'
        f'JOIN relation."{relation.name}" r ON t.entity_id = r.source_id\n'
        'GROUP BY timestamp, r.target_id'
    )


def define_view_materialization_sql(name) -> List[str]:
    return [
        'SELECT trend_directory.define_view_materialization(\n',
        "    id, '30m'::interval, '5m'::interval, '3 days'::interval, 'trend._{}'::regclass\n".format(name),
        ')\n',
        'FROM trend_directory.trend_store_part\n',
        "WHERE name = '{}';\n".format(name)
    ]


def define_fingerprint_sql(src_part: TrendStorePart):
    return (
        f'SELECT modified.last, format(\'{{"{src_part.name}": "%s"}}\', modified.last)::jsonb\n'
        'FROM trend_directory.modified\n'
        'JOIN trend_directory.trend_store_part ttsp ON ttsp.id = modified.trend_store_part_id\n'
        f"WHERE ttsp::name = '{src_part.name}' AND modified.timestamp = $1;\n"
    )


def enable_sql(name) -> sql.SQL:
    return sql.SQL(
        "UPDATE trend_directory.materialization SET enabled = true "
        "WHERE materialization::text = '{}';"
    ).format(sql.Literal(name))


AGGREGATE_DATA_TYPE_MAPPING_SUM = {
    'smallint': 'bigint',
    'integer': 'bigint',
    'bigint': 'numeric',
    'float': 'float',
    'double precision': 'double precision',
    'real': 'real',
    'numeric': 'numeric'
}

AGGREGATE_DATA_TYPE_MAPPING_AVG = {
    'smallint': 'numeric',
    'integer': 'numeric',
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
        aggregation_context: Union[EntityAggregationContext, TimeAggregationContext]
) -> TrendStore:
    definition = aggregation_context.definition
    src_trend_store = aggregation_context.source_definition

    if definition.get('data_source') is None:
        target_data_source = src_trend_store.data_source
    else:
        target_data_source = definition.get('data_source')

    if definition.get('granularity') is None:
        target_granularity = src_trend_store.granularity
    else:
        target_granularity = definition.get('granularity')

    if definition.get('entity_type') is None:
        target_entity_type = src_trend_store.entity_type
    else:
        target_entity_type = definition.get('entity_type')

    aggregate_parts = []

    for aggregate_part_def in definition['parts']:
        if 'source' in aggregate_part_def:
            try:
                src_part = next(
                    part
                    for part in src_trend_store.parts
                    if part.name == aggregate_part_def['source']
                )
            except StopIteration:
                raise ConfigurationError(
                    "Could not find part definition '{}' in source trend store '{}'".format(
                        aggregate_part_def['source'], definition['source']
                    )
                )

            aggregate_part = define_aggregate_part(src_part, aggregate_part_def)

            aggregate_parts.append(aggregate_part)
        else:
            aggregate_part = TrendStorePart.from_dict(aggregate_part_def)

            aggregate_parts.append(aggregate_part)

    return TrendStore(
        target_data_source,
        target_entity_type,
        target_granularity,
        PARTITION_SIZE_MAPPING[target_granularity],
        aggregate_parts
    )


def define_aggregate_part(data: TrendStorePart, definition):
    trends = [
        define_aggregate_trend(trend) for trend in data.trends
    ]

    added_generated_trends = [
        GeneratedTrend(
            generated_trend_def['name'],
            generated_trend_def['data_type'],
            generated_trend_def.get('description'),
            generated_trend_def['expression'],
            generated_trend_def.get('extra_data')
        )
        for generated_trend_def in definition.get('generated_trends', [])
    ]

    generated_trends = list(chain(
        data.generated_trends,
        added_generated_trends
    ))

    # If there is no samples column, we add one
    if not len([trend for trend in trends if trend.name == 'samples']):
        trends.insert(
            0,
            Trend(
                name='samples',
                data_type='integer',
                description='Number of source records',
                time_aggregation='sum',
                entity_aggregation='sum',
                extra_data={}
            )
        )

    return TrendStorePart(
        definition['name'],
        trends,
        generated_trends
    )


def aggregate_data_type(data_type: str, aggregate_method: str) -> str:
    if aggregate_method.upper() == 'SUM':
        return AGGREGATE_DATA_TYPE_MAPPING_SUM.get(data_type, data_type)
    elif aggregate_method.upper() == 'AVG':
        return AGGREGATE_DATA_TYPE_MAPPING_AVG.get(data_type, data_type)
    else:
        return data_type


def define_aggregate_trend(source_trend: Trend):
    return Trend(
        source_trend.name,
        aggregate_data_type(source_trend.data_type, source_trend.time_aggregation),
        '',
        source_trend.time_aggregation,
        source_trend.entity_aggregation,
        source_trend.extra_data
    )


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


def compile_time_aggregation(aggregation_context: TimeAggregationContext) -> TrendStore:
    write_time_aggregations(aggregation_context)

    aggregate_trend_store = define_aggregate_trend_store(aggregation_context)

    aggregate_trend_store_file_path = Path(
        aggregation_context.instance.root,
        'trend',
        '{}.yaml'.format(aggregation_context.definition['name'])
    )

    print(f"Writing aggregate trend store to '{aggregate_trend_store_file_path}'")

    with aggregate_trend_store_file_path.open('w') as out_file:
        ordered_yaml_dump(aggregate_trend_store.to_dict(), out_file, indent=2)

    return aggregate_trend_store


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


def write_time_aggregations(aggregation_context: TimeAggregationContext):
    """
    Define the aggregations for all parts of the trend store and write them to files

    :param aggregation_context:
    """
    for agg_part in aggregation_context.definition['parts']:
        if 'source' in agg_part:
            try:
                source_part = next(
                    part
                    for part in aggregation_context.source_definition.parts
                    if agg_part['source'] == part.name
                )
            except StopIteration:
                raise ConfigurationError(
                    "No source definition found for aggregation part '{}'(source: {})".format(
                        agg_part['name'], agg_part['source']
                    )
                )

            materialization_file_path = aggregation_context.instance.materialization_file_path(agg_part['name'])

            print(
                "Writing materialization to '{}'".format(materialization_file_path)
            )

            mapping_function = aggregation_context.definition['mapping_function']
            target_granularity = aggregation_context.definition['granularity']

            aggregate_definition = define_part_time_aggregation(
                source_part, aggregation_context.source_definition.granularity, mapping_function,
                target_granularity, agg_part['name']
            )

            with materialization_file_path.open('w') as out_file:
                ordered_yaml_dump(
                    aggregate_definition, stream=out_file, Dumper=yaml.SafeDumper,
                    indent=2
                )


def define_part_time_aggregation(
        source_part: TrendStorePart, source_granularity: str, mapping_function: str, target_granularity: str, name: str
) -> OrderedDict:
    return OrderedDict([
        ('target_trend_store_part', name),
        ('enabled', True),
        ('processing_delay', '30m'),
        ('stability_delay', '5m'),
        ('reprocessing_period', '3 days'),
        ('sources', [
            OrderedDict([
                ('trend_store_part', source_part.name),
                ('mapping_function', mapping_function)
            ])
        ]),
        ('function', aggregate_function(source_part, target_granularity)),
        ('fingerprint_function', SqlSrc(fingerprint_function_sql(source_part, source_granularity, target_granularity)))
    ])


def aggregate_function(part_data: TrendStorePart, target_granularity) -> OrderedDict:
    """
    Generate the YAML data for the aggregation function of a time aggregation.

    :param part_data: The part that needs to be aggregated
    :param target_granularity:
    :return: definition of the function as an OrderedDict
    """
    trend_columns = [
        '  "{}" {}'.format(
            trend.name,
            aggregate_data_type(trend.data_type, trend.time_aggregation)
        )
        for trend in part_data.trends
    ]

    trend_column_expressions = [
        '      {}(t."{}") AS "{}"'.format(
            trend.time_aggregation,
            trend.name,
            trend.name
        )
        for trend in part_data.trends
    ]

    column_expressions = [
        '      entity_id',
        '      $2 AS timestamp',
    ]

    result_columns = [
        '  "entity_id" integer',
        '  "timestamp" timestamp with time zone',
    ]

    if len([trend for trend in part_data.trends if trend.name == 'samples']) == 0:
        column_expressions.append('      (count(*))::smallint AS samples')
        result_columns.append('  samples smallint')

    column_expressions += trend_column_expressions

    result_columns += trend_columns

    return_type = (
        'TABLE (\n' +
        ',\n'.join(result_columns) +
        '\n' +
        ')\n'
    )

    src = (
        'BEGIN\n' +
        'RETURN QUERY EXECUTE $query$\n' +
        '    SELECT\n' +
        ',\n'.join(expr for expr in column_expressions) +
        '\n' +
        '    FROM trend."{}" AS t\n'.format(part_data.name) +
        '    WHERE $1 < timestamp AND timestamp <= $2\n' +
        '    GROUP BY entity_id\n' +
        "$query$ USING $1 - interval '{}', $1;\n".format(target_granularity) +
        'END;\n'
    )

    return OrderedDict([
        ('return_type', SqlSrc(return_type)),
        ('src', SqlSrc(src)),
        ('language', 'plpgsql')
    ])


def define_materialization_sql(target_name: str) -> List[str]:
    return [
        'SELECT trend_directory.define_function_materialization(\n',
        "    id, '30m'::interval, '5m'::interval, '3 days'::interval,\n"
        "    'trend.{}(timestamp with time zone)'::regprocedure\n".format(target_name),
        ')\n',
        'FROM trend_directory.trend_store_part\n',
        "WHERE name = '{}';\n".format(target_name),
    ]


def define_trend_store_link(part_name: str, mapping_function: str, target_name: str) -> List[str]:
    return [
        'INSERT INTO trend_directory.materialization_trend_store_link(\n'
        'materialization_id, trend_store_part_id, timestamp_mapping_func)\n',
        "SELECT m.id, tsp.id, 'trend.{}(timestamp with time zone)'::regprocedure\n".format(mapping_function),
        'FROM trend_directory.materialization m, trend_directory.trend_store_part tsp\n',
        "WHERE m::text = '{}' and tsp.name = '{}';\n".format(
            target_name, part_name
        )
    ]


def fingerprint_function_sql(
        part_data: TrendStorePart, source_granularity: str, target_granularity: str) -> str:
    return (
        "SELECT max(modified.last), format('{%s}', string_agg(format('\"%s\":\"%s\"', t, modified.last), ','))::jsonb\n"
        f"FROM generate_series($1 - interval '{target_granularity}' + interval '{source_granularity}', $1, interval '{source_granularity}') t\n"  # noqa: E501
        'LEFT JOIN (\n'
        '  SELECT timestamp, last\n'
        '  FROM trend_directory.trend_store_part part\n'
        '  JOIN trend_directory.modified ON modified.trend_store_part_id = part.id\n'
        f'  WHERE part.name = \'{part_data.name}\'\n'
        ') modified ON modified.timestamp = t;\n'
    )
