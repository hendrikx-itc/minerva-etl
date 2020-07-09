#!/usr/bin/env python3
"""
A tool to automatically generate time aggregations for all raw data trend
stores (containing the word 'raw' in the trend store title).
"""
import re
from pathlib import Path
from collections import OrderedDict

from minerva.instance import MinervaInstance, TrendStore
from minerva.commands import ordered_yaml_dump


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
    ]
}


ENTITY_RELATIONS = {
    'Cell': {
        'target_name': 'v-cell',
        'relation_name': 'Cell->v-cell'
    }
}


def generate_standard_aggregations():
    instance = MinervaInstance.load()

    instance_root = instance.root

    trend_path = Path(instance_root, 'trend')

    for file_path in trend_path.rglob('*.yaml'):
        relative_path = file_path.relative_to(trend_path)
        print(relative_path)
        trend_store = MinervaInstance.load_trend_store_from_file(file_path)

        if trend_store.title and "raw" in trend_store.title.lower():
            generate_aggregations(
                instance_root, relative_path, trend_store
            )


def generate_aggregations(
        instance_root: Path, source_path: Path, trend_store: TrendStore):
    """
    Generate all standard aggregations for the specified trend store
    """
    print(f"Loaded trend store: {trend_store}")

    try:
        aggregations = STANDARD_AGGREGATIONS[trend_store.granularity]
    except KeyError:
        print(f"No standard aggregation defined for granularity {trend_store.granularity}")
        return

    for source_granularity, target_granularity in aggregations:
        target_name = generate_time_aggregation(
            instance_root, source_path, trend_store, source_granularity,
            target_granularity
        )

        relation = ENTITY_RELATIONS.get(trend_store.entity_type)

        if relation is None:
            print(f"No relation for entity type {trend_store.entity_type}")
        else:
            generate_entity_aggregation(instance_root, target_name)


def generate_entity_aggregation(instance_root: Path, source_name):
    print(f'generate entity aggregation for {source_name}')


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

    print(aggregation_file_path)

    print(f"Generating aggregation {source_granularity} -> {target_granularity}")  # noqa: E501

    parts = [
        OrderedDict(
            [
                ("name", translate_part_name(part.name, target_granularity)),
                ("source", translate_part_name(part.name, source_granularity)),
            ]
        )
        for part in trend_store.parts
    ]

    source_name = translate_part_name(
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

    return target_name


def translate_part_name(name: str, target_granularity: str) -> str:
    """
    Translate a part name with standard naming convention
    <data_source>_<entity_type>_<granularity> to
    <data_source_<entity_type>_<target_granularity>.
    """
    m = re.match("^(.*)_[^_]+$", name)

    entity_type_and_data_source = m.group(1)

    return f"{entity_type_and_data_source}_{target_granularity}"
