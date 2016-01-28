from psycopg2.extensions import adapt, register_adapter


class AttributeDescriptor:
    def __init__(self, name, data_type, description):
        self.name = name
        self.data_type = data_type
        self.description = description


class Attribute:
    """Describes the attribute of a specific NotificationStore."""

    def __init__(
            self, id_, notification_store_id, name, data_type, description):
        self.id = id_
        self.notification_store_id = notification_store_id
        self.name = name
        self.data_type = data_type
        self.description = description

    def __str__(self):
        return self.name


def adapt_trend_descriptor(attribute_descriptor):
    """Return psycopg2 compatible representation of `attribute`."""
    return adapt((
        attribute_descriptor.name,
        attribute_descriptor.data_type.name,
        attribute_descriptor.description
    ))


register_adapter(AttributeDescriptor, adapt_trend_descriptor)
