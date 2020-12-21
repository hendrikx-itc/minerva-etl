from operator import itemgetter
import csv
import datetime
from itertools import chain, islice

import dateutil.parser

from minerva.directory.entityref import entity_name_ref_class
from minerva.error import ConfigurationError
from minerva.harvest.plugin_api_trend import HarvestParserTrend
from minerva.storage.trend.trend import Trend
from minerva.storage.trend.datapackage import DataPackage, DataPackageType
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.datatype import registry

DEFAULT_CHUNK_SIZE = 5000

DEFAULT_CONFIG = {
    "timestamp": "timestamp",
    "identifier": "entity",
    "delimiter": ",",
    "chunk_size": DEFAULT_CHUNK_SIZE
}


class Parser(HarvestParserTrend):
    def __init__(self, config):
        if config is None:
            self.config = DEFAULT_CONFIG
        else:
            self.config = config

    def load_packages(self, stream, name):
        csv_reader = csv.reader(stream, delimiter=self.config['delimiter'])

        header = next(csv_reader)

        timestamp_provider = is_timestamp_provider(header, self.config['timestamp'])

        identifier_provider = is_identifier_provider(header, self.config['identifier'])

        value_parsers = [
            (
                itemgetter(header.index(column['name'])),
                registry[column['data_type']].string_parser(column.get('parser_config', {"null_value": ""})),
            )
            for column in self.config['columns']
        ]

        trend_descriptors = [
            Trend.Descriptor(column['name'], registry['text'], '')
            for column in self.config['columns']
        ]

        entity_type_name = self.config['entity_type']

        granularity = create_granularity(self.config['granularity'])

        entity_ref_type = entity_name_ref_class(entity_type_name)

        def get_entity_type_name(data_package):
            return entity_type_name

        data_package_type = DataPackageType(
            entity_type_name, entity_ref_type, get_entity_type_name
        )

        rows = (
            (
                identifier_provider(row),
                timestamp_provider(row),
                tuple(
                    parse_value(value_parser, get_value, row)
                    for get_value, value_parser in value_parsers
                )
            )
            for row in csv_reader
        )

        chunk_size = self.config.get('chunk_size', DEFAULT_CHUNK_SIZE)

        for chunk in chunked(rows, chunk_size):
            yield DataPackage(
                data_package_type, granularity,
                trend_descriptors, chunk
            )


def chunked(iterable, size: int):
    """
    Return a generator of chunks (lists) of length size until
    :param iterable: the iterable that will be chunked
    :param size: the chunk size
    :return:
    """
    iterator = iter(iterable)
    for first in iterator:
        yield list(chain([first], islice(iterator, size - 1)))


class ParseError(Exception):
    pass


def parse_value(value_parser, get_value, row):
    """
    Parse a value from a row and provide context if an error occurs.
    :param value_parser:
    :param get_value:
    :param row:
    :return:
    """
    raw_value = get_value(row)

    try:
        value = value_parser(raw_value)
    except Exception as exc:
        raise ParseError(f"Error parsing value '{raw_value}': {exc}")

    return value


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

        def f(row):
            value = row[column_index]

            timestamp = dateutil.parser.parse(value)

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
