import os
from typing import List
from collections import OrderedDict
from pathlib import Path

from psycopg2.extensions import adapt, register_adapter, AsIs, QuotedString
from psycopg2.extras import Json

import yaml


INSTANCE_ROOT_VARIABLE = 'MINERVA_INSTANCE_ROOT'


class Trend:
    def __init__(self, name, data_type, description, time_aggregation, entity_aggregation, extra_data):
        self.name = name
        self.data_type = data_type
        self.description = description
        self.time_aggregation = time_aggregation
        self.entity_aggregation = entity_aggregation
        self.extra_data = extra_data

    @staticmethod
    def from_json(data):
        return Trend(
            data['name'],
            data['data_type'],
            data.get('description', ''),
            data.get('time_aggregation', 'SUM'),
            data.get('entity_aggregation', 'SUM'),
            data.get('extra_data', {})
        )

    def to_json(self):
        return OrderedDict([
            ('name', self.name),
            ('data_type', self.data_type),
            ('description', self.description),
            ('time_aggregation', self.time_aggregation),
            ('entity_aggregation', self.entity_aggregation),
            ('extra_data', self.extra_data)
        ])

    @staticmethod
    def adapt(trend):
        if trend.extra_data is None:
            extra_data = 'NULL'
        else:
            extra_data = Json(trend.extra_data)

        return AsIs(
            "({}, {}, {}, {}, {}, {})".format(
                QuotedString(trend.name),
                QuotedString(trend.data_type),
                QuotedString(trend.description),
                QuotedString(trend.time_aggregation),
                QuotedString(trend.entity_aggregation),
                extra_data
            )
        )


class GeneratedTrend:
    name: str
    data_type: str
    description: str
    expression: str
    extra_data: dict

    def __init__(self, name, data_type, description, expression, extra_data):
        self.name = name
        self.data_type = data_type
        self.description = description
        self.expression = expression
        self.extra_data = extra_data

    @staticmethod
    def from_json(data):
        return GeneratedTrend(
            data['name'],
            data['data_type'],
            data.get('description', ''),
            data['expression'],
            data.get('extra_data', {})
        )

    def to_json(self):
        items = [
            ('name', self.name),
            ('data_type', self.data_type)
        ]

        if self.description is not None:
            items.append(('description', self.description))

        items.append(('expression', self.expression))

        if self.extra_data is not None:
            items.append(('extra_data', self.extra_data))

        return OrderedDict(items)

    @staticmethod
    def adapt(generated_trend):
        if generated_trend.extra_data is None:
            extra_data = 'NULL'
        else:
            extra_data = Json(generated_trend.extra_data)

        if generated_trend.description is None:
            description = 'NULL'
        else:
            description = QuotedString(generated_trend.description)

        return AsIs(
            "({}, {}, {}, {}, {})".format(
                QuotedString(generated_trend.name),
                QuotedString(generated_trend.data_type),
                description,
                QuotedString(str(generated_trend.expression)),
                extra_data
            )
        )


class TrendStorePart:
    name: str
    trends: List[Trend]
    generated_trends: List[Trend]

    def __init__(self, name: str, trends: List[Trend], generated_trends: List[GeneratedTrend]):
        self.name = name
        self.trends = trends
        self.generated_trends = generated_trends

    def __str__(self):
        return str(TrendStorePart.adapt(self))

    @staticmethod
    def from_json(data):
        return TrendStorePart(
            data['name'],
            [
                Trend.from_json(trend)
                for trend in data['trends']
            ],
            [
                GeneratedTrend.from_json(generated_trend)
                for generated_trend in data.get('generated_trends', [])
            ]
        )

    def to_json(self):
        return OrderedDict([
            ('name', self.name),
            ('trends', [trend.to_json() for trend in self.trends]),
            ('generated_trends', [generated_trend.to_json() for generated_trend in self.generated_trends])
        ])

    @staticmethod
    def adapt(trend_store_part):
        return AsIs(
            "({}, {}::trend_directory.trend_descr[], {}::trend_directory.generated_trend_descr[])".format(
                QuotedString(trend_store_part.name),
                adapt(trend_store_part.trends),
                adapt(trend_store_part.generated_trends)
            )
        )


class TrendStore:
    title: str
    data_source: str
    entity_type: str
    granularity: str
    partition_size: str
    parts: List[TrendStorePart]

    def __init__(self, data_source, entity_type, granularity, partition_size, parts):
        self.title = None
        self.data_source = data_source
        self.entity_type = entity_type
        self.granularity = granularity
        self.partition_size = partition_size
        self.parts = parts

    def __str__(self):
        return f'{self.data_source} - {self.entity_type} - {self.granularity}'

    @staticmethod
    def from_json(data):
        trend_store = TrendStore(
            data['data_source'],
            data['entity_type'],
            data['granularity'],
            data['partition_size'],
            [TrendStorePart.from_json(p) for p in data['parts']]
        )

        trend_store.title = data.get('title')

        return trend_store

    def to_json(self):
        return OrderedDict([
            ('title', self.title),
            ('data_source', self.data_source),
            ('entity_type', self.entity_type),
            ('granularity', self.granularity),
            ('partition_size', self.partition_size),
            ('parts', [part.to_json() for part in self.parts])
        ])


register_adapter(TrendStorePart, TrendStorePart.adapt)
register_adapter(GeneratedTrend, GeneratedTrend.adapt)
register_adapter(Trend, Trend.adapt)


class MinervaInstance:
    root: str

    def __init__(self, root: str):
        self.root = root

    @staticmethod
    def load(instance_root=None):
        """
        Load a Minerva instance with the specified root, or using the path in
        default environment variable, or the current working directory.
        """
        return MinervaInstance(
            instance_root or os.environ.get(INSTANCE_ROOT_VARIABLE) or os.getcwd()
        )

    def materialization_file_path(self, name: str):
        """
        Return a full file path from the provided materialization name.
        """
        return os.path.join(
            self.root, 'materialization', f'{name}.yaml'
        )

    def trend_store_file_path(self, name: str):
        base_name, ext = os.path.splitext(name)

        if not ext:
            file_name = f'{name}.yaml'
        else:
            file_name = name

        return os.path.join(
            self.root, 'trend', file_name
        )

    def make_relative(self, path: str):
        return os.path.relpath(path, self.root)

    def load_trend_store(self, name: str) -> TrendStore:
        file_path = self.trend_store_file_path(name)

        with open(file_path) as definition_file:
            definition = yaml.load(definition_file, Loader=yaml.SafeLoader)

        return TrendStore.from_json(definition)

    def list_trend_stores(self):
        trend_store_dir = Path(self.root, 'trend')

        trend_store_files = [path.relative_to(trend_store_dir) for path in trend_store_dir.rglob('*.yaml')]

        return trend_store_files

    def load_trend_stores(self):
        return [
            self.load_trend_store(name)
            for name in self.list_trend_stores()
        ]
