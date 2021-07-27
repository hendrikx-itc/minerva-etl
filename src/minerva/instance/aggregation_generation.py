#!/usr/bin/env python3
"""
A tool to automatically generate time aggregations for all raw data trend
stores (containing the word 'raw' in the trend store title).
"""
import re
from pathlib import Path
from collections import OrderedDict, defaultdict
from typing import Dict, Tuple, Set

from minerva.commands.aggregation import (
    TimeAggregationContext, compile_time_aggregation, EntityAggregationContext,
    compile_entity_aggregation
)
from minerva.instance import (
    MinervaInstance, TrendStore, Relation,
    EntityAggregationType, ENTITY_AGGREGATION_TYPE_MAP_REVERSE
)
from minerva.instance.generating import translate_entity_aggregation_part_name
from minerva.util.yaml import ordered_yaml_dump

STANDARD_AGGREGATIONS = {
    "15m": [
        ("15m", "1h"),
        ("15m", "1d"),
        ("1d", "1w"),
        ("1d", "1month"),
    ],
    "30m": [
        ("30m", "1h"),
        ("30m", "1d"),
        ("1d", "1w"),
        ("1d", "1month"),
    ],
    "1d": [
        ("1d", "1w"),
        ("1d", "1month"),
    ]
}


def generate_standard_aggregations(instance: MinervaInstance):
    trend_path = Path(instance.root, 'trend')

    created_entity_aggregations = set()

    for file_path in trend_path.rglob('*.yaml'):
        generate_standard_aggregations_for(instance, file_path, created_entity_aggregations)


def generate_standard_aggregations_for(instance: MinervaInstance, trend_store_path: Path,
                                       created_entity_aggregations: Set[str]):
    trend_path = Path(instance.root, 'trend')

    relative_path = trend_store_path.absolute().relative_to(trend_path)
    trend_store = MinervaInstance.load_trend_store_from_file(trend_store_path)
    aggregation_hints = instance.load_aggregation_hints()

    if trend_store.title and "raw" in trend_store.title.lower():
        print(relative_path)

        generate_aggregations(
            instance.root, relative_path, trend_store, aggregation_hints, created_entity_aggregations
        )


def generate_aggregations(
        instance_root: Path, source_path: Path, trend_store: TrendStore,
        aggregation_hints: Dict[str, Tuple[EntityAggregationType, str]],
        created_entity_aggregations: Set[str]):
    """
    Generate all standard aggregations for the specified trend store
    """
    print(f"Loaded trend store: {trend_store}")

    try:
        aggregations = STANDARD_AGGREGATIONS[trend_store.granularity]
    except KeyError:
        print(f"No standard aggregation defined for granularity {trend_store.granularity}")
        return

    instance = MinervaInstance(instance_root)

    entity_relations = defaultdict(list)

    for relation in instance.load_relations():
        entity_relations[relation.source_entity_type].append(relation)

    relations = entity_relations.get(trend_store.entity_type, [])

    for relation in relations:
        created_entity_aggregations = generate_entity_aggregation(aggregation_hints, instance, relation, source_path, trend_store, created_entity_aggregations)

    for source_granularity, target_granularity in aggregations:
        file_path, definition = generate_time_aggregation(
            instance_root, source_path, trend_store, source_granularity,
            target_granularity
        )

        aggregation_context = TimeAggregationContext(
            instance, definition['time_aggregation'], file_path
        )

        target_trend_store = compile_time_aggregation(aggregation_context)

        for relation in relations:
            created_entity_aggregations = generate_entity_aggregation(aggregation_hints, instance, relation, file_path, target_trend_store, created_entity_aggregations)

    return created_entity_aggregations

def generate_entity_aggregation(
        aggregation_hints, instance: MinervaInstance, relation: Relation, source_path: Path, trend_store: TrendStore,
        created_entity_aggregations: Set[str]
):
    aggregation_type = aggregation_hints.get(relation.name, (DEFAULT_AGGREGATION_TYPE, ''))[0]
    aggregation_prefix = aggregation_hints.get(relation.name, (DEFAULT_AGGREGATION_TYPE, ''))[1]
    if aggregation_prefix:
        # if aggregation_prefix is empty, do not add a prefix,
        # but if you do add a prefix, put a dash between the prefix and the rest
        aggregation_prefix += '_'

    file_path, definition = generate_entity_aggregation_yaml(
        instance.root, source_path, trend_store, relation,
        aggregation_type, aggregation_prefix
    )
    aggregation_context = EntityAggregationContext(
        instance, definition['entity_aggregation'], file_path
    )

    basename = definition['entity_aggregation']['basename']

    compile_entity_aggregation(aggregation_context, combined_aggregation=(basename in created_entity_aggregations))
    created_entity_aggregations.add(basename)

    return created_entity_aggregations


def generate_entity_aggregation_yaml(
        instance_root: Path, source_path: Path, trend_store: TrendStore,
        relation: Relation, aggregation_type: EntityAggregationType,
        aggregation_prefix: str) -> Tuple[Path, dict]:
    """
    Generate the YAML based aggregation definition and write it to a file.
    :param instance_root:
    :param source_path: Source trend store file path
    :param trend_store: Source trend store
    :param relation: Relation to map source entity to target entity
    :param aggregation_type: Type of aggregation (view, view materialization)
    :return:
    """
    print(f'generate entity aggregation for {trend_store} using {relation}')
    name = f"{trend_store.data_source}_{aggregation_prefix}{relation.target_entity_type}_{trend_store.granularity}"
    base_name = f"{trend_store.data_source}_{relation.target_entity_type}_{trend_store.granularity}"
    file_name = f"{name}.yaml"
    aggregation_file_path = Path(instance_root, "aggregation", file_name)

    parts = [
        OrderedDict([
            ("name", translate_entity_aggregation_part_name(part.name, relation.target_entity_type, aggregation_prefix)),
            ("source", part.name)
        ])
        for part in trend_store.parts
    ]

    data = {
        "entity_aggregation": OrderedDict([
            ("source", source_path.stem),
            ("name", name),
            ("basename", base_name),
            ("data_source", trend_store.data_source),
            ("entity_type", relation.target_entity_type),
            ("relation", relation.name),
            ("aggregation_type", ENTITY_AGGREGATION_TYPE_MAP_REVERSE[aggregation_type]),
            ("parts", parts)
        ])
    }

    with aggregation_file_path.open("w") as out_file:
        ordered_yaml_dump(data, out_file)

    return aggregation_file_path, data


DEFAULT_AGGREGATION_TYPE = EntityAggregationType.VIEW


def generate_time_aggregation(
        instance_root: Path, source_path: Path, trend_store: TrendStore,
        source_granularity: str, target_granularity
):
    """
    Generate an aggregation definition, based on the provided trend store, but
    not necessarily for the same source granularity as the trend store to the
    provided target granularity. The provided source granularity will be used
    to translate the original trend store and trend store parts names.
    """
    name = f"{trend_store.data_source}_{trend_store.entity_type}_{target_granularity}"  # noqa: E501
    file_name = f"{name}.yaml"

    aggregation_file_path = Path(instance_root, "aggregation", file_name)

    print(f"Generating aggregation {source_granularity} -> {target_granularity}")  # noqa: E501

    parts = [
        OrderedDict(
            [
                ("name", translate_time_aggregation_part_name(part.name, target_granularity)),
                ("source", translate_time_aggregation_part_name(part.name, source_granularity)),
            ]
        )
        for part in trend_store.parts
    ]

    source_name = translate_time_aggregation_part_name(
        str(source_path.with_suffix('')), source_granularity
    )

    mapping_function = (
        f"trend.mapping_{source_granularity}->{target_granularity}"  # noqa: E501
    )

    target_name = str(Path(source_path.parent, name))

    data = {
        "time_aggregation": OrderedDict([
            ("source", source_name),
            ("name", target_name),
            ("data_source", trend_store.data_source),
            ("granularity", target_granularity),
            ("mapping_function", mapping_function),
            ("parts", parts),
        ]),
    }

    with aggregation_file_path.open("w") as out_file:
        ordered_yaml_dump(data, out_file)

    return aggregation_file_path, data


def translate_time_aggregation_part_name(name: str, target_granularity: str) -> str:
    """
    Translate a part name with standard naming convention
    <data_source>_<entity_type>_<granularity> to
    <data_source_<entity_type>_<target_granularity>.
    """
    m = re.match("^(.*)_[^_]+$", name)

    if m is None:
        raise ValueError(f"Could not translate part name {name}")

    entity_type_and_data_source = m.group(1)

    return f"{entity_type_and_data_source}_{target_granularity}"
