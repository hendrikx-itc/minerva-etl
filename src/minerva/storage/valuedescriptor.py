from minerva.storage import datatype


class ValueDescriptor:
    def __init__(
            self, name, data_type, parser_config=None, serializer_config=None):
        self.name = name
        self.data_type = data_type
        self.parser_config = parser_config
        self.serializer_config = serializer_config

        self.parse_string = data_type.string_parser(parser_config)

        self.serialize_to_string = data_type.string_serializer(
            serializer_config
        )

    def __eq__(self, other):
        return (
            self.name == other.name and
            self.data_type == other.data_type
        )

    def parse(self, value):
        return self.parse_string(value)

    def serialize(self, value):
        return self.serialize_to_string(value)

    def to_config(self):
        return {
            'name': self.name,
            'data_type': self.data_type.name,
            'parser_config': self.parser_config,
            'serializer_config': self.serializer_config
        }

    @staticmethod
    def from_config(config):
        return ValueDescriptor(
            config['name'],
            datatype.registry[config['data_type']],
            config.get('parser_config'),
            config.get('serializer_config')
        )
