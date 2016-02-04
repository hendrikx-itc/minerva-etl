# -*- coding: utf-8 -*-
from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage import datatype


class OutputDescriptor:
    """
    Combines a value descriptor with configuration for serializing values.
    """
    def __init__(
            self, value_descriptor: ValueDescriptor,
            serializer_config: dict=None):
        self.value_descriptor = value_descriptor
        self.serializer_config = serializer_config
        self.serialize = value_descriptor.data_type.string_serializer(
            serializer_config
        )

    @staticmethod
    def load(config):
        return OutputDescriptor(
            ValueDescriptor(
                config['name'],
                datatype.registry[config['data_type']]
            ),
            config.get('serializer_config')
        )

    def to_dict(self):
        return {
            'name': self.value_descriptor.name,
            'data_type': self.value_descriptor.data_type.name,
            'serializer_config': self.serializer_config
        }
