from pathlib import Path
from typing import List, Tuple
import json

import yaml

from minerva.instance import MinervaInstance
from minerva.instance.aggregation_compilation import EntityAggregationContext, compile_entity_aggregation, \
    TimeAggregationContext, compile_time_aggregation, time_aggregation_key_fn
from minerva.instance.aggregation_generation import generate_standard_aggregations, generate_standard_aggregations_for
from minerva.util.yaml import ordered_yaml_load


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'aggregation', help='commands for defining aggregations'
    )

    cmd_subparsers = cmd.add_subparsers()

    setup_generate_parser(cmd_subparsers)
    setup_compile_parser(cmd_subparsers)
    setup_compile_all_parser(cmd_subparsers)


def setup_generate_parser(subparsers):
    cmd = subparsers.add_parser(
        'generate', help='generate standard aggregations'
    )

    cmd.add_argument(
        'trend_store', nargs='*',
        help='trend stores to generate aggregations for'
    )

    cmd.set_defaults(cmd=generate_standard_aggregations_cmd)


def generate_standard_aggregations_cmd(args):
    instance = MinervaInstance.load()

    if args.trend_store:
        for file_path in args.trend_store:
            generate_standard_aggregations_for(instance, Path(file_path))
    else:
        generate_standard_aggregations(instance)


def setup_compile_parser(subparsers):
    cmd = subparsers.add_parser(
        'compile', help='compile an aggregation into materializations'
    )

    cmd.add_argument(
        '--format', choices=['yaml', 'json'], default='yaml',
        help='format of definition'
    )

    cmd.add_argument(
        'definition',
        help='Aggregations definition file'
    )

    cmd.set_defaults(cmd=compile_aggregation)


def setup_compile_all_parser(subparsers):
    cmd = subparsers.add_parser(
        'compile-all', help='compile all defined aggregations in the Minerva instance'
    )

    cmd.set_defaults(cmd=compile_all_aggregations)


def load_aggregations(instance_root: Path) -> List[Tuple[Path, dict]]:
    return [
        (file_path, yaml.load(file_path.open(), Loader=yaml.SafeLoader))
        for file_path in instance_root.glob('aggregation/*.yaml')
    ]


def compile_all_aggregations(_args):
    instance = MinervaInstance.load()

    instance_root = Path(instance.root)

    aggregation_definitions = load_aggregations(instance_root)

    print("Loading time aggregations")

    time_aggregation_definitions = [
        (file_path, TimeAggregationContext(instance, d['time_aggregation'], file_path))
        for file_path, d in aggregation_definitions
        if 'time_aggregation' in d
    ]

    print("Loading entity aggregations")

    entity_aggregation_definitions = [
        (file_path, EntityAggregationContext(instance, d['entity_aggregation'], file_path))
        for file_path, d in aggregation_definitions
        if 'entity_aggregation' in d
    ]

    sorted_time_aggregation_definitions = sorted(time_aggregation_definitions, key=time_aggregation_key_fn)

    for file_path, aggregation_context in sorted_time_aggregation_definitions:
        title = str(file_path)
        print('#-{}'.format(len(title)*'-'))
        print('# {}'.format(title))
        print('#-{}'.format(len(title)*'-'))
        compile_time_aggregation(aggregation_context)

    trend_stores_done = set()
    for file_path, aggregation_context in entity_aggregation_definitions:
        compile_entity_aggregation(aggregation_context, combined_aggregation=aggregation_context.definition['basename'] in trend_stores_done)
        trend_stores_done.add(aggregation_context.definition['basename'])


def compile_aggregation(args):
    with open(args.definition) as definition_file:
        if args.format == 'json':
            definition = json.load(definition_file)
        elif args.format == 'yaml':
            definition = ordered_yaml_load(definition_file, Loader=yaml.SafeLoader)

    instance = MinervaInstance.load()

    if 'entity_aggregation' in definition:
        aggregation_context = EntityAggregationContext(
            instance, definition['entity_aggregation'], args.definition
        )
        compile_entity_aggregation(aggregation_context, combined_aggregation=False)
    elif 'time_aggregation' in definition:
        aggregation_context = TimeAggregationContext(
            instance, definition['time_aggregation'], args.definition
        )

        compile_time_aggregation(aggregation_context)
