from operator import itemgetter
import csv
import datetime

from minerva.harvest.plugin_api_trend import HarvestParserTrend
from minerva.storage.trend.trend import Trend
from minerva.storage.trend.datapackage import DataPackage, DataPackageType
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.datatype import registry


class Parser(HarvestParserTrend):
    def __init__(self, config):
        self.config = config

    def load_packages(self, stream, name):
        csv_reader = csv.reader(stream, delimiter=',')

        header = next(csv_reader)

        timestamp_provider = is_timestamp_provider(self.config['timestamp']['is'])

        def get_identifier(row):
            return row[0]

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
                get_identifier(row),
                timestamp_provider(row),
                tuple(
                    value_parser(get_value(row))
                    for get_value, value_parser in value_parsers
                )
            )
            for row in csv_reader
        ]

        entity_type_name = self.config['entity_type']

        classic_type = DataPackageType(AliasRef(), fixed_type(entity_type_name))

        yield DataPackage(
            classic_type, create_granularity('1 day'),
            trend_descriptors, rows
        )


def is_timestamp_provider(func_name):
    if func_name == 'current_timestamp':
        timestamp = datetime.datetime.now()

        def f(*args):
            return timestamp

        return f
    else:
        raise NotImplementedError()


class AliasRef:
    def map_to_entity_ids(self, aliases):
        def map_to(cursor):
            return range(len(aliases))

        return map_to


def fixed_type(name):
    def get_type(*args, **kwargs):
        return name

    return get_type
