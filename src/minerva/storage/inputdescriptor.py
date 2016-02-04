# -*- coding: utf-8 -*-
from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage import datatype


class InputDescriptor:
    """
    Combines a value descriptor with configuration for parsing values.
    """
    def __init__(
            self, value_descriptor: ValueDescriptor, parser_config: dict=None):
        self.value_descriptor = value_descriptor
        self.parser_config = parser_config
        self.parse = value_descriptor.data_type.string_parser(parser_config)

    @staticmethod
    def load(config):
        return InputDescriptor(
            ValueDescriptor(
                config['name'],
                datatype.registry[config['data_type']]
            ),
            config.get('parser_config')
        )

    def to_dict(self):
        return {
            'name': self.value_descriptor.name,
            'data_type': self.value_descriptor.data_type.name,
            'parser_config': self.parser_config
        }
