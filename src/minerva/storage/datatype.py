# -*- coding: utf-8 -*-
"""
Defines the data types recognized by Minerva.
"""
import re
from datetime import datetime, tzinfo
import decimal
from functools import partial, reduce
import operator
from typing import Callable, Optional, Any, Set, Dict, List, Iterable

import pytz

from minerva.util import merge_dicts


class ParseError(Exception):
    pass


class DataType:
    name: str

    def __init__(self, name: str):
        self.name = name

    def string_parser(self, config: dict=None) -> Callable[[str], Any]:
        raise NotImplementedError()

    def string_serializer(self, config: dict=None) -> Callable[[Any], str]:
        raise NotImplementedError()

    def deduce_parser_config(self, value: str) -> Optional[dict]:
        """
        Returns a configuration that can be used to parse the provided value
        and values like it or None if the value can not be parsed.

        :param value: A string containing a value of this type
        :return: configuration dictionary
        """
        raise NotImplementedError()


class Boolean(DataType):
    true_set: Set[str] = {"1", "True", "true"}
    false_set: Set[str] = {"0", "False", "false"}
    bool_set: Set[str] = true_set | false_set

    default_parser_config: Dict[str, str] = {
        "null_value": "\\N",
        "true_value": "true",
        "false_value": "false"
    }

    default_serializer_config: Dict[str, str] = {
        "null_value": "\\N",
        "true_value": "true",
        "false_value": "false"
    }

    def __init__(self):
        DataType.__init__(self, 'boolean')

    def string_parser_config(self, config: Optional[dict]) -> dict:
        if config is None:
            return self.default_parser_config
        else:
            return merge_dicts(self.default_parser_config, config)

    def string_parser(self, config=None) -> Callable[[str], Optional[bool]]:
        config = self.string_parser_config(config)

        null_value = config["null_value"]
        true_value = config["true_value"]
        false_value = config["false_value"]

        if hasattr(true_value, '__iter__'):
            is_true = partial(operator.contains, true_value)
        elif isinstance(true_value, str):
            is_true = partial(operator.eq, true_value)

        if hasattr(false_value, '__iter__'):
            is_false = partial(operator.contains, false_value)
        elif isinstance(false_value, str):
            is_false = partial(operator.eq, false_value)

        def parse(value: str) -> Optional[bool]:
            if value == null_value:
                return None
            elif is_true(value):
                return True
            elif is_false(value):
                return False
            else:
                raise ParseError(
                    'invalid literal for data type boolean: {}'.format(value)
                )

        return parse

    def string_serializer_config(self, config: Optional[dict]) -> dict:
        if config is not None:
            return merge_dicts(self.default_serializer_config, config)
        else:
            return self.default_serializer_config

    def string_serializer(self, config: Optional[dict] = None) -> Callable[[Optional[bool]], str]:
        merged_config = self.string_serializer_config(config)

        def serialize(value) -> str:
            if value is None:
                return merged_config['null_value']
            elif value is True:
                return merged_config['true_value']
            else:
                return merged_config['false_value']

        return serialize

    def deduce_parser_config(self, value: str) -> Optional[dict]:
        if value in self.bool_set:
            return merge_dicts(
                self.default_parser_config,
                {
                    "true_value": self.true_set,
                    "false_value": self.false_set
                }
            )
        else:
            return None


def assure_tzinfo(tz):
    if isinstance(tz, tzinfo):
        return tz
    else:
        return pytz.timezone(tz)


class TimestampWithTimeZone(DataType):
    default_parser_config = {
        "null_value": "\\N",
        "timezone": "UTC",
        "format": "%Y-%m-%dT%H:%M:%S"
    }

    default_serializer_config = {
        "null_value": "\\N",
        "format": "%Y-%m-%dT%H:%M:%S"
    }

    def __init__(self):
        DataType.__init__(self, 'timestamp with time zone')

    def string_parser_config(self, config: Optional[dict]) -> dict:
        if config is None:
            return self.default_parser_config
        else:
            return merge_dicts(self.default_parser_config, config)

    def string_parser(self, config: dict=None) -> Callable[[str], Optional[datetime]]:
        """
        Return function that can parse a string representation of a
        TimestampWithTimeZone value.

        :param config: a dictionary with the form {"timezone", <tzinfo>,
        "format", <format_string>}
        :return: a function (str_value) -> value
        """
        config = self.string_parser_config(config)

        null_value = config["null_value"]
        tz = assure_tzinfo(config["timezone"])
        format_str = config["format"]

        def parse(value: str) -> Optional[datetime]:
            if value == null_value:
                return None
            else:
                return tz.localize(datetime.strptime(value, format_str))

        return parse

    def string_serializer_config(self, config: Optional[dict]) -> dict:
        if config is None:
            return self.default_serializer_config
        else:
            return merge_dicts(self.default_serializer_config, config)

    def string_serializer(self, config: Optional[dict] = None) -> Callable[[datetime], str]:
        config = self.string_parser_config(config)

        null_value = config['null_value']
        format_str = config['format']

        def serialize(value: datetime) -> str:
            if value is None:
                return null_value
            else:
                return value.strftime(format_str)

        return serialize

    def deduce_parser_config(self, value: str) -> dict:
        if value is None:
            return self.default_parser_config
        else:
            raise NotImplementedError


class Timestamp(DataType):
    default_parser_config = {
        "null_value": "\\N",
        "format": "%Y-%m-%dT%H:%M:%S"
    }

    default_serializer_config = {
        "null_value": "\\N",
        "format": "%Y-%m-%dT%H:%M:%S"
    }

    known_formats = [
        (
            re.compile(
                "^([0-9]{4})-([0-9]{2})-([0-9]{2})T"
                "([0-9]{2}):([0-9]{2}):([0-9]{2})$"
            ),
            "%Y-%m-%dT%H:%M:%S"
        ),
        (
            re.compile(
                "^([0-9]{4})-([0-9]{2})-([0-9]{2}) "
                "([0-9]{2}):([0-9]{2}):([0-9]{2})$"
            ),
            "%Y-%m-%d %H:%M:%S"
        )
    ]

    def __init__(self):
        DataType.__init__(self, 'timestamp')

    def string_parser_config(self, config: Optional[dict]=None) -> dict:
        if config is None:
            return self.default_parser_config
        else:
            return merge_dicts(self.default_parser_config, config)

    def string_parser(self, config=None):
        config = self.string_parser_config(config)

        def parse(value: str) -> Optional[datetime]:
            if value == config["null_value"]:
                return None
            else:
                return datetime.strptime(value, config["format"])

        return parse

    def string_serializer_config(self, config: dict) -> dict:
        if config is None:
            return self.default_serializer_config
        else:
            return merge_dicts(self.default_serializer_config, config)

    def string_serializer(self, config=None):
        config = self.string_serializer_config(config)

        datetime_format = config["format"]

        def serialize(value: datetime) -> str:
            return value.strftime(datetime_format)

        return serialize

    def deduce_parser_config(self, value) -> Optional[dict]:
        if not isinstance(value, str):
            return None

        for regex, datetime_format in self.known_formats:
            match = regex.match(value)

            if match is not None:
                return merge_dicts(
                    self.default_parser_config,
                    {'format': datetime_format}
                )

        return None


class SmallInt(DataType):
    min = int(-pow(2, 15))
    max = int(pow(2, 15) - 1)

    default_parser_config = {
        "null_value": "\\N"
    }

    default_serializer_config = {
        "null_value": "\\N"
    }

    def __init__(self):
        DataType.__init__(self, 'smallint')

    def string_parser_config(self, config):
        if config is None:
            return self.default_parser_config
        else:
            return merge_dicts(self.default_parser_config, config)

    def string_parser(self, config=None):
        config = self.string_parser_config(config)

        null_value = config["null_value"]

        def parse(value: str) -> Optional[int]:
            if value == null_value:
                return None
            else:
                return self._parse(value)

        return parse

    regex = re.compile("^-?[1-9][0-9]*$")

    def deduce_parser_config(self, value: Any) -> Optional[dict]:
        if not isinstance(value, str):
            return None

        if value == "":
            return merge_dicts(
                self.default_parser_config,
                {'null_value': ''}
            )

        if not self.regex.match(value):
            return None

        try:
            int_val = int(value)
        except ValueError:
            return None
        except TypeError:
            return None
        else:
            if self.min <= int_val <= self.max:
                return self.default_parser_config
            else:
                return None

    def string_serializer_config(self, config):
        if config is None:
            return self.default_serializer_config
        else:
            return merge_dicts(self.default_serializer_config, config)

    def string_serializer(self, config=None):
        config = self.string_serializer_config(config)

        null_value = config['null_value']

        def serialize(value: int) -> str:
            if value is None:
                return null_value
            else:
                return str(value)

        return serialize

    def _parse(self, value: str) -> Optional[int]:
        if not value:
            return None

        try:
            int_val = int(value)
        except ValueError as exc:
            raise ParseError(str(exc))

        if not (self.min <= int_val <= self.max):
            raise ParseError(
                "{0:d} is not in range {1:d} - {2:d}".format(
                    int_val, self.min, self.max
                )
            )

        return int_val


class Integer(DataType):
    min = int(-pow(2, 31))
    max = int(pow(2, 31) - 1)

    default_parser_config = {
        "null_value": "\\N"
    }

    default_serializer_config = {
        'null_value': '\\N'
    }

    def __init__(self):
        DataType.__init__(self, 'integer')

    def string_parser_config(self, config):
        if config is None:
            return self.default_parser_config
        else:
            return merge_dicts(self.default_parser_config, config)

    @staticmethod
    def _string_serializer_config(config):
        if config is None:
            return Integer.default_serializer_config
        else:
            return merge_dicts(
                Integer.default_serializer_config, config
            )

    def string_parser(self, config=None):
        config = self.string_parser_config(config)

        def parse(value):
            if value == config["null_value"]:
                return None
            else:
                return self._parse(value)

        return parse

    def string_serializer(self, config=None):
        config = self._string_serializer_config(config)

        null_value = config['null_value']

        def serialize(value):
            if value is None:
                return null_value
            else:
                return str(value)

        return serialize

    def deduce_parser_config(self, value):
        if not isinstance(value, str):
            return None

        try:
            int_val = int(value)
        except ValueError:
            return None
        except TypeError:
            return None
        else:
            if self.min <= int_val <= self.max:
                return self.default_parser_config

    def _parse(self, value):
        if not value:
            return None

        try:
            int_val = int(value)
        except ValueError as exc:
            raise ParseError(str(exc))

        if not (self.min <= int_val <= self.max):
            raise ParseError(
                "{0:d} is not in range {1:d} - {2:d}".format(
                    int_val, self.min, self.max
                )
            )

        return int_val


class Bigint(DataType):
    min = int(-pow(2, 63))
    max = int(pow(2, 63) - 1)

    default_parser_config: Dict[str, str] = {
        "null_value": "\\N"
    }

    default_serializer_config: Dict[str, str] = {
        "null_value": "\\n"
    }

    def __init__(self):
        DataType.__init__(self, 'bigint')

    @staticmethod
    def string_parser_config(config):
        if config is None:
            return Bigint.default_parser_config
        else:
            return merge_dicts(Bigint.default_parser_config, config)

    @staticmethod
    def string_serializer_config(config):
        if config is None:
            return Bigint.default_serializer_config
        else:
            return merge_dicts(Bigint.default_serializer_config, config)

    def string_parser(self, config=None):
        config = self.string_parser_config(config)

        null_value = config["null_value"]

        def parse(value):
            if value == null_value:
                return None
            else:
                return Bigint.parse(value)

        return parse

    def string_serializer(self, config=None):
        config = self.string_serializer_config(config)

        null_value = config['null_value']

        def serialize(value):
            if value is None:
                return null_value
            else:
                return str(value)

        return serialize

    def deduce_parser_config(self, value):
        if not isinstance(value, str):
            return None

        try:
            int_val = int(value)
        except (TypeError, ValueError):
            return None
        else:
            if self.min <= int_val <= self.max:
                return self.default_parser_config

    @classmethod
    def parse(cls, value):
        if not value:
            return None

        try:
            int_val = int(value)
        except ValueError as exc:
            raise ParseError(str(exc))

        if not (cls.min <= int_val <= cls.max):
            raise ParseError("{0:d} is not in range {1:d} - {2:d}".format(
                    int_val, cls.min, cls.max
                )
            )

        return int_val


class Real(DataType):
    default_parser_config: Dict[str, str] = {
        "null_value": "\\N"
    }

    default_serializer_config: Dict[str, str] = {
        "null_value": "\\n"
    }

    def __init__(self):
        DataType.__init__(self, 'real')

    @staticmethod
    def string_parser_config(config):
        if config is None:
            return Real.default_parser_config
        else:
            return merge_dicts(Real.default_parser_config, config)

    @staticmethod
    def string_serializer_config(config):
        if config is None:
            return Integer.default_serializer_config
        else:
            return merge_dicts(
                Integer.default_serializer_config, config
            )

    def string_parser(self, config=None):
        config = self.string_parser_config(config)

        def parse(value):
            """
            Parse value and return float value. If value is empty ('') or None,
            None is returned.
            :param value: string representation of a real value, e.g.;
            '34.00034', '343', ''
            :return: float value
            """
            if value == config["null_value"]:
                return None
            else:
                return float(value)

        return parse

    def string_serializer(self, config=None):
        config = self.string_serializer_config(config)

        null_value = config['null_value']

        def serialize(value):
            if value is None:
                return null_value
            else:
                return str(value)

        return serialize

    def deduce_parser_config(self, value):
        if not isinstance(value, str):
            return None

        try:
            float(value)
        except ValueError:
            return None
        except TypeError:
            return None
        else:
            return self.default_parser_config


class DoublePrecision(DataType):
    default_parser_config: Dict[str, str] = {
        "null_value": "\\N"
    }

    default_serializer_config: Dict[str, str] = {
        "null_value": "\\n"
    }

    def __init__(self):
        DataType.__init__(self, 'double precision')

    @staticmethod
    def string_parser_config(config):
        if config is None:
            return DoublePrecision.default_parser_config
        else:
            return merge_dicts(DoublePrecision.default_parser_config, config)

    @staticmethod
    def string_serializer_config(config):
        if config is None:
            return DoublePrecision.default_serializer_config
        else:
            return merge_dicts(
                DoublePrecision.default_serializer_config, config
            )

    def string_parser(self, config=None):
        config = self.string_parser_config(config)

        null_value = config['null_value']

        def parse(value: str) -> Optional[float]:
            if value == null_value:
                return None
            else:
                return float(value)

        return parse

    def deduce_parser_config(self, value):
        if not isinstance(value, str):
            return None

        try:
            float(value)
        except ValueError:
            return None
        except TypeError:
            return None
        else:
            return self.default_parser_config

    def string_serializer(self, config=None):
        config = self.string_serializer_config(config)

        null_value = config['null_value']

        def serialize(value):
            if value is None:
                return null_value
            else:
                return str(value)

        return serialize


class Numeric(DataType):
    default_parser_config: Dict[str, str] = {
        "null_value": "\\N"
    }

    default_serializer_config: Dict[str, str] = {
        "null_value": "\\n"
    }

    def __init__(self):
        DataType.__init__(self, 'numeric')

    @staticmethod
    def string_parser_config(config):
        if config is None:
            return Numeric.default_parser_config
        else:
            return merge_dicts(Numeric.default_parser_config, config)

    @staticmethod
    def string_serializer_config(config):
        if config is None:
            return Numeric.default_serializer_config
        else:
            return merge_dicts(Numeric.default_serializer_config, config)

    def string_parser(self, config=None):
        config = self.string_parser_config(config)

        is_null = partial(operator.eq, config["null_value"])

        def parse(value):
            if is_null(value):
                return None
            else:
                try:
                    return decimal.Decimal(value)
                except decimal.InvalidOperation as exc:
                    raise ParseError(str(exc))

        return parse

    def string_serializer(self, config=None):
        config = self.string_serializer_config(config)

        null_value = config['null_value']

        def serialize(value):
            if value is None:
                return null_value
            else:
                return str(value)

        return serialize

    def deduce_parser_config(self, value):
        try:
            decimal.Decimal(value)
        except decimal.InvalidOperation:
            return None
        except ValueError:
            return None
        except TypeError:
            return None
        else:
            return self.default_parser_config


class Text(DataType):
    default_parser_config = {
        "null_value": "\\N"
    }

    default_serializer_config = {
        "null_value": "\\N",
        "prefix": "",
        "postfix": ""
    }

    def __init__(self):
        DataType.__init__(self, 'text')

    @staticmethod
    def string_parser_config(config):
        if config is None:
            return Text.default_parser_config
        else:
            return merge_dicts(Text.default_parser_config, config)

    def string_parser(self, config=None):
        config = self.string_parser_config(config)

        null_value = config["null_value"]

        def parse(value):
            if value == null_value:
                return None
            else:
                return value

        return parse

    @staticmethod
    def string_serializer_config(config):
        if config is None:
            return Text.default_serializer_config
        else:
            return merge_dicts(Text.default_serializer_config, config)

    def string_serializer(self, config=None):
        config = self.string_serializer_config(config)

        null_value = config['null_value']
        prefix = config['prefix']
        postfix = config['postfix']

        format_str = '{}{{}}{}'.format(prefix, postfix)

        def serialize(value):
            if value is None:
                return null_value
            else:
                return format_str.format(value)

        return serialize

    def deduce_parser_config(self, value) -> dict:
        return self.default_parser_config


class ArrayType(DataType):
    default_string_parser_config = {
        'null_value': '\\n',
        'separator': ',',
        'prefix': '[',
        'postfix': ']'
    }

    default_string_serializer_config = {
        'null_value': '\\N',
        'separator': ',',
        'prefix': '[',
        'postfix': ']',
        'base_type_config': None
    }

    def __init__(self, base_type: DataType):
        self.base_type = base_type

        type_name = '{}[]'.format(base_type.name)

        DataType.__init__(self, type_name)

    @staticmethod
    def string_parser_config(config: Optional[dict]) -> dict:
        if config is None:
            config = ArrayType.default_string_parser_config

        return merge_dicts(
            ArrayType.default_string_parser_config,
            config
        )

    @staticmethod
    def string_serializer_config(config: Optional[dict]) -> dict:
        if config is None:
            config = ArrayType.default_string_serializer_config

        return merge_dicts(
            ArrayType.default_string_serializer_config,
            config
        )

    def string_parser(self, config: Optional[dict]=None):
        config = self.string_parser_config(config)

        base_type_parser = self.base_type.string_parser(
            config.get('base_type_config')
        )
        separator = config['separator']

        lbracket = config['prefix']
        rbracket = config['postfix']

        values_part = bracket_stripper(lbracket, rbracket)

        def parse(str_value):
            return [
                base_type_parser(part)
                for part in values_part(str_value).split(separator)
            ]

        return parse

    def string_serializer(self, config=None):
        config = self.string_serializer_config(config)

        base_type_serializer = self.base_type.string_serializer(
            config['base_type_config']
        )
        separator = config['separator']

        prefix = config['prefix']

        postfix = config['postfix']

        def serialize(arr_value):
            if arr_value is None:
                return config['null_value']
            else:
                return prefix + separator.join(
                    base_type_serializer(part) for part in arr_value
                ) + postfix

        return serialize

    def deduce_parser_config(self, value):
        raise NotImplementedError


def bracket_stripper(lbracket: str, rbracket: str) -> str:
    def strip_brackets(str_value):
        return str_value.lstrip(lbracket).rstrip(rbracket)

    return strip_brackets


registry: Dict[str, DataType] = {}


def register_type(data_type: DataType):
    registry[data_type.name] = data_type


register_type(Bigint())
register_type(Boolean())
register_type(Timestamp())
register_type(TimestampWithTimeZone())
register_type(Integer())
register_type(SmallInt())
register_type(Real())
register_type(DoublePrecision())
register_type(Numeric())
register_type(Text())
register_type(ArrayType(registry['bigint']))
register_type(ArrayType(registry['boolean']))
register_type(ArrayType(registry['timestamp']))
register_type(ArrayType(registry['timestamp with time zone']))
register_type(ArrayType(registry['integer']))
register_type(ArrayType(registry['smallint']))
register_type(ArrayType(registry['real']))
register_type(ArrayType(registry['double precision']))
register_type(ArrayType(registry['numeric']))
register_type(ArrayType(registry['text']))


# The set of types that are integer
INTEGER_TYPES: Set[DataType] = {
    registry['bigint'],
    registry['integer'],
    registry['smallint']
}

TYPE_ORDER: List[DataType] = [
    registry['smallint'],
    registry['integer'],
    registry['bigint'],
    registry['real'],
    registry['double precision'],
    registry['numeric'],
    registry['timestamp'],
    registry['text']
]


TYPE_ORDER_RANKS: Dict[DataType, int] = dict(
    (data_type, i)
    for i, data_type in enumerate(TYPE_ORDER)
)


def max_data_type(left: DataType, right: DataType) -> DataType:
    if TYPE_ORDER_RANKS[right] > TYPE_ORDER_RANKS[left]:
        return right
    else:
        return left


def max_data_types(current_data_types: Iterable[DataType], new_data_types: Iterable[DataType]) -> List[DataType]:
    return [
        max_data_type(current_data_type, new_data_type)
        for current_data_type, new_data_type
        in zip(current_data_types, new_data_types)
    ]


class ParserDescriptor:
    def __init__(self, data_type, parser_config):
        self.data_type = data_type
        self.parser_config = parser_config

    def parser(self):
        return self.data_type.string_parser(self.parser_config)


def parser_descriptor_from_string(value):
    for data_type in TYPE_ORDER:
        parse_config = data_type.deduce_parser_config(value)

        if parse_config is not None:
            return ParserDescriptor(data_type, parse_config)

    raise ValueError("Unable to determine data type of: {0}".format(value))


def deduce_data_types(rows):
    """
    Return a list of the minimal required data types to store the values, in
    the same order as the values and thus matching the order of
    attribute_names.

    :param rows:
    :rtype: collections.iterable[DataType]
    """
    return reduce(
        max_data_types,
        [
            [
                parser_descriptor_from_string(value).data_type
                for value in row
            ]
            for row in rows
        ]
    )


def load_data_format(format_config):
    data_type_name = format_config["data_type"]

    try:
        data_type = registry[data_type_name]
    except KeyError:
        raise Exception("No such data type: {}".format(data_type_name))
    else:
        config = data_type.string_parser_config(
            format_config["string_format"]
        )

        return data_type, data_type.string_parser(config)


copy_from_serializer_base_type_config = {
    registry['bigint']: {
        'null_value': '\\N'
    },
    registry['boolean']: {
        'null_value': '\\N'
    },
    registry['timestamp']: {
        'null_value': '\\N'
    },
    registry['timestamp with time zone']: {
        'null_value': '\\N'
    },
    registry['integer']: {
        'null_value': '\\N'
    },
    registry['smallint']: {
        'null_value': '\\N'
    },
    registry['real']: {
        'null_value': '\\N'
    },
    registry['double precision']: {
        'null_value': '\\N'
    },
    registry['numeric']: {
        'null_value': '\\N'
    },
    registry['text']: {
        'null_value': '\\N'
    }
}


def copy_from_serializer_config(data_type: DataType) -> dict:
    if isinstance(data_type, ArrayType):
        return {
            'separator': ',',
            'prefix': '{',
            'postfix': '}',
            'base_type_config': copy_from_serializer_base_type_config[
                data_type.base_type
            ]
        }
    else:
        return copy_from_serializer_base_type_config[data_type]
