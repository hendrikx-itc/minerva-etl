"""Provide functionality to deduce trend stores from data."""
import os
import traceback

from minerva.harvest.error import DataError
from minerva.harvest.plugin_api_trend import HarvestParserTrend


def deduce_config(file_path, parser: HarvestParserTrend):
    """
    Process a single file with specified plugin.
    """
    if not os.path.exists(file_path):
        raise Exception(f"Could not find file '{file_path}'")

    _directory, filename = os.path.split(file_path)

    trend_descriptors = {}

    with open(file_path) as data_file:
        try:
            for package in parser.load_packages(data_file, filename):
                for trend_descriptor in package.trend_descriptors:
                    if trend_descriptor.name not in trend_descriptors:
                        trend_descriptors[trend_descriptor.name] = trend_descriptor
        except DataError as exc:
            raise exc
        except Exception:
            stack_trace = traceback.format_exc()
            position = -1  # data_file.tell()
            raise Exception(f"{stack_trace} at position {position:d}")

    return {
        "data_source": "DATASOURCE",
        "entity_type": "ENTITYTYPE",
        "granularity": "GRANULARITY",
        "partition_size": "PARTITIONSIZE",
        "parts": [
            {
                "name": "PART",
                "trends": [
                    trend_descriptor.to_dict()
                    for name, trend_descriptor in trend_descriptors.items()
                ],
            }
        ],
    }
