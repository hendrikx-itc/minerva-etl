from minerva.storage.trend.trendstore import TrendStore


class ViewTrendStoreDescriptor():
    def __init__(
            self, data_source, entity_type, granularity, query):
        self.data_source = data_source
        self.entity_type = entity_type
        self.granularity = granularity
        self.query = query


class ViewTrendStore(TrendStore):
    pass