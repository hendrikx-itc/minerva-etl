from minerva.db.query import Table, Column, Eq, ands
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.partition import Partition
from minerva.storage.trend.partitioning import Partitioning
from minerva.storage.trend.tables import DATA_TABLE_POSTFIXES
from minerva.directory import DataSource, EntityType
from minerva.storage.trend.granularity import create_granularity


class TableTrendStoreDescriptor():
    def __init__(
            self, data_source, entity_type, granularity, trend_descriptors,
            partition_size):
        self.data_source = data_source
        self.entity_type = entity_type
        self.granularity = granularity
        self.trend_descriptors = trend_descriptors
        self.partition_size = partition_size


class TableTrendStore(TrendStore):
    def __init__(
            self, id, data_source, entity_type, granularity, trends,
            partition_size):
        super().__init__(id, data_source, entity_type, granularity, trends)
        self.partition_size = partition_size
        self.partitioning = Partitioning(partition_size)

    def __str__(self):
        return self.base_table_name()

    def base_table_name(self):
        granularity_str = str(self.granularity)

        postfix = DATA_TABLE_POSTFIXES.get(
            granularity_str, granularity_str
        )

        return "{}_{}_{}".format(
            self.data_source.name, self.entity_type.name, postfix
        )

    def partition_table_name(self, timestamp):
        return "{}_{}".format(
            self.base_table_name(),
            self.partitioning.index(timestamp)
        )

    def base_table(self):
        return Table("trend", self.base_table_name())

    def partition(self, timestamp):
        index = self.partitioning.index(timestamp)

        return Partition(index, self)

    def index_to_interval(self, partition_index):
        return self.partitioning.index_to_interval(partition_index)

    def check_trends_exist(self, trend_descriptors):
        query = (
            "SELECT trend_directory.assure_trends_exist("
            "trend_store, %s::trend_directory.trend_descr[]"
            ") "
            "FROM trend_directory.trend_store "
            "WHERE id = %s"
        )

        args = trend_descriptors, self.id

        def f(cursor):
            cursor.execute(query, args)

        return f

    def ensure_data_types(self, trend_descriptors):
        """
        Check if database column types match trend data type and correct it if
        necessary.
        """
        query = (
            "SELECT trend_directory.assure_data_types("
            "trend_store, %s::trend_directory.trend_descr[]"
            ") "
            "FROM trend_directory.trend_store "
            "WHERE id = %s"
        )

        args = trend_descriptors, self.id

        def f(cursor):
            cursor.execute(query, args)

        return f

    @staticmethod
    def create(descriptor):
        def f(cursor):
            args = (
                descriptor.data_source.name,
                descriptor.entity_type.name,
                str(descriptor.granularity),
                descriptor.trend_descriptors,
                descriptor.partition_size
            )

            query = (
                "SELECT * FROM trend_directory.create_table_trend_store("
                "%s, %s, %s, %s::trend_directory.trend_descr[], %s"
                ")"
            )

            cursor.execute(query, args)

            (
                trend_store_id, entity_type_id, data_source_id, granularity_str,
                partition_size, store_type, retention_period
            ) = cursor.fetchone()

            entity_type = EntityType.get(entity_type_id)(cursor)
            data_source = DataSource.get(data_source_id)(cursor)

            trends = TableTrendStore.get_trends(cursor, trend_store_id)

            return TableTrendStore(
                trend_store_id, data_source, entity_type,
                create_granularity(granularity_str), trends, partition_size,
                store_type
            )

        return f
