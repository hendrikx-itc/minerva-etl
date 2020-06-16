# -*- coding: utf-8 -*-
from typing import Optional

from minerva.storage import datatype
from psycopg2.extras import Json
from psycopg2.extensions import adapt, register_adapter


class NoSuchTrendError(Exception):
    pass


class Trend:
    class Descriptor:
        name: str
        data_type: datatype.DataType
        description: str
        time_aggregation: str
        entity_aggregation: str
        extra_data: dict

        def __init__(self, name: str, data_type: datatype.DataType, description: str, extra_data: Optional[dict]=None):
            self.name = name
            self.data_type = data_type
            self.description = description
            self.time_aggregation = 'SUM'
            self.entity_aggregation = 'SUM'

            if extra_data is None:
                self.extra_data = {}
            else:
                self.extra_data = extra_data

        def to_dict(self):
            return {
                'name': self.name,
                'data_type': self.data_type.name,
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


def adapt_trend_descriptor(trend_descriptor: Trend.Descriptor):
    """Return psycopg2 compatible representation of `trend_descriptor`."""
    if trend_descriptor.extra_data is None:
        extra_data = None
    else:
        extra_data = Json(trend_descriptor.extra_data)

    return adapt((
        trend_descriptor.name,
        trend_descriptor.data_type.name,
        trend_descriptor.description,
        trend_descriptor.time_aggregation,
        trend_descriptor.entity_aggregation,
        extra_data,
    ))


register_adapter(Trend.Descriptor, adapt_trend_descriptor)
