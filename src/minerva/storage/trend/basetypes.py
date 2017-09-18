# -*- coding: utf-8 -*-
from minerva.storage.trend.granularity import ensure_granularity


class Trend(object):
    def __init__(
            self, id, name, description, data_source_id, entity_type_id,
            granularity):
        self.id = id
        self.name = name
        self.description = description
        self.data_source_id = data_source_id
        self.entity_type_id = entity_type_id
        self.granularity = ensure_granularity(granularity)

    def __repr__(self):
        return "<Trend({0}/{1}/{2}/{3})>".format(
            self.name, self.data_source_id, self.entity_type_id,
            self.granularity
        )

    def __str__(self):
        return self.name


class TrendTag(object):
    def __init__(self, id, name):
        self.id = id
        self.name = name

    def __repr__(self):
        return "<TrendTag({0})>".format(self.name)

    def __str__(self):
        return self.name
