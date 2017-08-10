"""
Provides access and a location for storage class logic like 'trend',
'attribute', etc..
"""


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
    def store_cmd(package):
        """
        Return a StoreCmd-like object.

        Usage:

        engine = SomeEngineSubclass()

        cmd = engine.store(package)

        cmd(data_source)(conn)

        :param package: :class:`DataPackageBase
        <minerva.storage.trend.datapackage.DataPackageBase>` sub-class instance
        :return: :class:`StoreCmd`-like object
        """
        raise NotImplementedError()
