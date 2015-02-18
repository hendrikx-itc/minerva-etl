class ValueDescriptor():
    def __init__(
            self, name, data_type, parser_config=None, serializer_config=None):
        self.name = name
        self.data_type = data_type
        self.parser_config = parser_config
        self.serializer_config = serializer_config

        self.parse_string = data_type.string_parser(
            data_type.string_parser_config(parser_config)
        )

        self.serialize_to_string = data_type.string_serializer(
            serializer_config
        )

    def parse(self, value):
        return self.parse_string(value)

    def serialize(self, value):
        return self.serialize_to_string(value)
