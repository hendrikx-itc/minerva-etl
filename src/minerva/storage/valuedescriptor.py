# -*- coding: utf-8 -*-
from minerva.storage.datatype import DataType


class ValueDescriptor:
    """
    A combination of value name and type.
    """
    name: str
    data_type: DataType

    def __init__(
            self, name: str, data_type: DataType):
        self.name = name
        self.data_type = data_type

    def __eq__(self, other: 'ValueDescriptor'):
        return (
            self.name == other.name and
            self.data_type == other.data_type
        )
