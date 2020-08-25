from operator import itemgetter
import csv
import datetime

from minerva.directory.entityref import entity_name_ref_class
from minerva.error import ConfigurationError
from minerva.harvest.plugin_api_trend import HarvestParserTrend
from minerva.storage.trend.trend import Trend
from minerva.storage.trend.datapackage import DataPackage, DataPackageType
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.datatype import registry


DEFAULT_CONFIG = {
    "timestamp": {
        "is": "timestamp"
    },
    "identifier": {
        "is": "entity"
    }
}


class Parser(HarvestParserTrend):
    def __init__(self, config):
        if config is None:
            self.config = DEFAULT_CONFIG
        else:
            self.config = config

    def load_packages(self, stream, name):
        csv_reader = csv.reader(stream, delimiter=',')

        header = next(csv_reader)

        timestamp_provider = is_timestamp_provider(header, self.config['timestamp']['is'])

        identifier_provider = is_identifier_provider(header, self.config['identifier']['is'])

        value_parsers = [
            (
                itemgetter(header.index(column['name'])),
                registry[column['data_type']].string_parser(),
            )
            for column in self.config['columns']
        ]

        trend_descriptors = [
            Trend.Descriptor(column['name'], registry['text'], '')
            for column in self.config['columns']
        ]

        rows = [
            (
                identifier_provider(row),
                timestamp_provider(row),
                tuple(
                    value_parser(get_value(row))
                    for get_value, value_parser in value_parsers
                )
            )
            for row in csv_reader
        ]

        entity_type_name = self.config['entity_type']

        granularity = create_granularity(self.config['granularity'])

        entity_ref_type = entity_name_ref_class(entity_type_name)

        def get_entity_type_name(data_package):
            return entity_type_name

        data_package_type = DataPackageType(
            entity_type_name, entity_ref_type, get_entity_type_name
        )

        yield DataPackage(
            data_package_type, granularity,
            trend_descriptors, rows
        )


def is_timestamp_provider(header, name):
    if name == 'current_timestamp':
        timestamp = datetime.datetime.now()

        def f(*args):
            return timestamp

        return f
    else:
        if name not in header:
            raise ConfigurationError(f"No column named '{name}' specified in header")

        column_index = header.index(name)
        timestamp_format = "%Y-%m-%dT%H:%M:%SZ"

        def f(row):
            value = row[column_index]

            timestamp = datetime.datetime.strptime(value, timestamp_format)

            return timestamp

        return f


def is_identifier_provider(header, name):
    if name not in header:
        raise ConfigurationError(f"No column named '{name}' specified in header")
    else:
        return itemgetter(header.index(name))


class AliasRef:
    def map_to_entity_ids(self, aliases):
        def map_to(cursor):
            return range(len(aliases))

        return map_to


def fixed_type(name):
    def get_type(*args, **kwargs):
        return name

    return get_type
