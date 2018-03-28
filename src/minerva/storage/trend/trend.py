# -*- coding: utf-8 -*-
from minerva.storage import datatype
from psycopg2.extensions import adapt, register_adapter, ISQLQuote


class NoSuchTrendError(Exception):
    pass


class Trend:
    class Descriptor:
        name: str
        data_type: datatype.DataType
        description: str

        def __init__(self, name, data_type, description):
            """

            :param name: str
            :param data_type: DataType
            :param description: str
            :return: TrendDescriptor
            """
            self.name = name
            self.data_type = data_type
            self.description = description

        def to_dict(self):
            return {
                'name': self.name,
                'data_type': self.data_type,
                'description': self.description
            }

    def __init__(self, id_, name, data_type, trend_store_part_id, description):
        self.id = id_
        self.name = name
        self.data_type = data_type
        self.trend_store_part_id = trend_store_part_id
        self.description = description

    @staticmethod
    def create(trend_store_id, descriptor):
        def f(cursor):
            query = (
                "INSERT INTO trend_directory.trend ("
                "name, data_type, trend_store_id, description"
                ") "
                "VALUES (%s, %s, %s, %s) "
                "RETURNING *"
            )

            args = (
                descriptor.name, descriptor.data_type, trend_store_id,
                descriptor.description
            )

            cursor.execute(query, args)

            return Trend(*cursor.fetchone())

        return f


def adapt_trend_descriptor(trend_descriptor):
    """Return psycopg2 compatible representation of `attribute`."""
    return adapt((
        trend_descriptor.name,
        trend_descriptor.data_type.name,
        trend_descriptor.description
    ))


register_adapter(Trend.Descriptor, adapt_trend_descriptor)
