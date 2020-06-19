import os
from io import TextIOBase
from typing import List, Generator, Union, Tuple
from collections import OrderedDict
from pathlib import Path

from minerva.commands import ordered_yaml_dump, ConfigurationError
from psycopg2.extensions import adapt, register_adapter, AsIs, QuotedString
from psycopg2.extras import Json

import yaml


INSTANCE_ROOT_VARIABLE = 'MINERVA_INSTANCE_ROOT'


class DefinitionError(Exception):
    pass


class Trend:
    def __init__(self, name, data_type, description, time_aggregation, entity_aggregation, extra_data):
        self.name = name
        self.data_type = data_type
        self.description = description
        self.time_aggregation = time_aggregation
        self.entity_aggregation = entity_aggregation
        self.extra_data = extra_data

    @staticmethod
    def from_dict(data: dict):
        return Trend(
            data['name'],
            data['data_type'],
            data.get('description', ''),
            data.get('time_aggregation', 'SUM'),
            data.get('entity_aggregation', 'SUM'),
            data.get('extra_data', {})
        )

    def to_dict(self) -> OrderedDict:
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
    def from_dict(data: dict):
        return GeneratedTrend(
            data['name'],
            data['data_type'],
            data.get('description', ''),
            data['expression'],
            data.get('extra_data', {})
        )

    def to_dict(self) -> OrderedDict:
        items: List[Tuple[str, Union[str, dict]]] = [
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
    generated_trends: List[GeneratedTrend]

    def __init__(self, name: str, trends: List[Trend], generated_trends: List[GeneratedTrend]):
        self.name = name
        self.trends = trends
        self.generated_trends = generated_trends

    def __str__(self):
        return str(TrendStorePart.adapt(self))

    @staticmethod
    def from_dict(data: dict):
        return TrendStorePart(
            data['name'],
            [
                Trend.from_dict(trend)
                for trend in data['trends']
            ],
            [
                GeneratedTrend.from_dict(generated_trend)
                for generated_trend in data.get('generated_trends', [])
            ]
        )

    def to_dict(self) -> OrderedDict:
        return OrderedDict([
            ('name', self.name),
            ('trends', [trend.to_dict() for trend in self.trends]),
            ('generated_trends', [generated_trend.to_dict() for generated_trend in self.generated_trends])
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
    title: Union[str, None]
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
    def verify_data(data: dict):
        """
        Verify if data contains a valid trend store definition and raise exception if it doesn't

        :param data:
        :return:
        """
        if data is None:
            raise DefinitionError('None is not a valid trend store definition')

        required_attributes = [
            'data_source', 'entity_type', 'granularity', 'partition_size', 'parts'
        ]

        for attribute_name in required_attributes:
            if attribute_name not in data:
                raise DefinitionError("Attribute '{attribute_name}' missing from trend store definition")

    @staticmethod
    def from_dict(data: dict):
        TrendStore.verify_data(data)

        trend_store = TrendStore(
            data['data_source'],
            data['entity_type'],
            data['granularity'],
            data['partition_size'],
            [TrendStorePart.from_dict(p) for p in data['parts']]
        )

        trend_store.title = data.get('title')

        return trend_store

    def to_dict(self) -> OrderedDict:
        return OrderedDict([
            ('title', self.title),
            ('data_source', self.data_source),
            ('entity_type', self.entity_type),
            ('granularity', self.granularity),
            ('partition_size', self.partition_size),
            ('parts', [part.to_dict() for part in self.parts])
        ])


register_adapter(TrendStorePart, TrendStorePart.adapt)
register_adapter(GeneratedTrend, GeneratedTrend.adapt)
register_adapter(Trend, Trend.adapt)


class Attribute:
    name: str
    data_type: str
    unit: str
    description: str
    extra_data: dict

    def __init__(self, name, data_type, unit, description, extra_data):
        self.name = name
        self.data_type = data_type
        self.unit = unit
        self.description = description
        self.extra_data = extra_data

    @staticmethod
    def from_dict(data: dict):
        attribute = Attribute(
            data['name'],
            data['data_type'],
            data.get('unit'),
            data.get('description'),
            data.get('extra_data')
        )

        return attribute

    def to_dict(self) -> OrderedDict:
        return OrderedDict([
            ('name', self.name),
            ('data_type', self.data_type),
            ('unit', self.unit),
            ('description', self.description),
            ('extra_data', self.extra_data)
        ])


class AttributeStore:
    data_source: str
    entity_type: str
    attributes: List[Attribute]

    def __init__(self, data_source, entity_type, attributes):
        self.data_source = data_source
        self.entity_type = entity_type
        self.attributes = attributes

    def __str__(self):
        return f'{self.data_source}_{self.entity_type}'

    @staticmethod
    def from_dict(data: dict):
        attribute_store = AttributeStore(
            data['data_source'],
            data['entity_type'],
            [Attribute.from_dict(a) for a in data['attributes']]
        )

        return attribute_store

    def to_dict(self) -> OrderedDict:
        return OrderedDict([
            ('data_source', self.data_source),
            ('entity_type', self.entity_type),
            ('attributes', [attribute.to_dict() for attribute in self.attributes])
        ])


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

    def trend_store_file_path(self, name: str) -> Path:
        base_name, ext = os.path.splitext(name)

        if not ext:
            file_name = f'{name}.yaml'
        else:
            file_name = name

        return Path(self.root, 'trend', file_name)

    def attribute_store_file_path(self, name: str):
        base_name, ext = os.path.splitext(name)

        if not ext:
            file_name = f'{name}.yaml'
        else:
            file_name = name

        return Path(
            self.root, 'attribute', file_name
        )

    def make_relative(self, path: str):
        return os.path.relpath(path, self.root)

    def load_trend_store_by_name(self, name: str) -> TrendStore:
        file_path = self.trend_store_file_path(name)

        return self.load_trend_store_from_file(file_path)

    @staticmethod
    def load_trend_store_from_file(file: Union[Path, TextIOBase]) -> TrendStore:
        """
        Load and return trend store from the provided path
        """
        if isinstance(file, Path):
            with file.open() as definition_file:
                definition = yaml.load(definition_file, Loader=yaml.SafeLoader)
        elif isinstance(file, TextIOBase):
            definition = yaml.load(file, Loader=yaml.SafeLoader)
        else:
            raise Exception('Unsupported argument type for file')

        return TrendStore.from_dict(definition)

    def load_attribute_store(self, name: str) -> AttributeStore:
        file_path = self.attribute_store_file_path(name)

        try:
            return self.load_attribute_store_from_file(file_path)
        except Exception as e:
            raise ConfigurationError(f'Error loading attribute store {name}: {e}')

    def save_attribute_store(self, attribute_store: AttributeStore) -> Path:
        """
        Save the attribute store in the instance directory and return the path
        of the saved file.
        :param attribute_store: attribute store to save
        :return: path of the saved file
        """
        out_file_path = self.attribute_store_file_path(str(attribute_store))

        with open(out_file_path, 'w') as out_file:
            ordered_yaml_dump(attribute_store.to_dict(), out_file)

        return out_file_path

    @staticmethod
    def load_attribute_store_from_file(file: Union[Path, TextIOBase]) -> AttributeStore:
        if isinstance(file, Path):
            with file.open() as definition_file:
                definition = yaml.load(definition_file, Loader=yaml.SafeLoader)
        elif isinstance(file, TextIOBase):
            definition = yaml.load(file, Loader=yaml.SafeLoader)
        else:
            raise Exception('Unsupported argument type for file')

        return AttributeStore.from_dict(definition)

    def list_trend_stores(self) -> List[Path]:
        """
        Return list of trend store file paths in the instance trend directory
        """
        return list(Path(self.root, 'trend').rglob('*.yaml'))

    def list_attribute_stores(self) -> List[Path]:
        """
        Return list of attribute store file paths in the instance attribute directory
        """
        return list(Path(self.root, 'attribute').glob('*.yaml'))

    def load_trend_stores(self) -> Generator[TrendStore, None, None]:
        """
        Return generator that loads and returns all trend stores
        """
        return (
            self.load_trend_store_from_file(path)
            for path in self.list_trend_stores()
        )

    def load_attribute_stores(self) -> Generator[AttributeStore, None, None]:
        """
        Return generator that loads and returns all attribute stores
        """
        return (
            self.load_attribute_store_from_file(path)
            for path in self.list_attribute_stores()
        )
