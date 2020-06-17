"""
Provides access and a location for storage class logic like 'trend',
'attribute', etc..
"""
from minerva.storage.trend.datapackage import DataPackage


class StoreCmd:
    def __call__(self, data_source):
        """
        Return function that actually stores the data using the information
        stored in this object and the data_source provided.

        :param data_source: The data source the data came from
        :rtype: (conn) -> None
        """
        raise NotImplementedError()


class Engine:
    """
    The Engine class provides an interface that separates the database-aware
    code from the database-unaware logic.
    """
    @staticmethod
    def store_cmd(package: DataPackage, description: dict) -> StoreCmd:
        """
        Return a StoreCmd-like object.

        Usage:

        engine = SomeEngineSubclass()

        cmd = engine.store_cmd(package)

        cmd(data_source)(conn)
        """
        raise NotImplementedError()
