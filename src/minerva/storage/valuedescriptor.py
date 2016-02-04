# -*- coding: utf-8 -*-
from minerva.storage.datatype import DataType


class ValueDescriptor:
    """
    A combination of value name and type.
    """
    def __init__(
            self, name: str, data_type: DataType):
        self.name = name
        self.data_type = data_type

    def __eq__(self, other):
        return (
            self.name == other.name and
            self.data_type == other.data_type
        )
