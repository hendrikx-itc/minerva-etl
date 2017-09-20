# -*- coding: utf-8 -*-
from typing import Iterable, BinaryIO

from minerva.storage.trend.datapackage import DataPackage
from minerva.storage.trend.engine import TrendEngine


class HarvestParserTrend:
    @staticmethod
    def store_command():
        engine = TrendEngine()

        return engine.store_cmd

    def load_packages(self, stream: BinaryIO, name: str) -> Iterable[DataPackage]:
        """
        Return iterable of DataPackage objects.

        :param stream: A file-like object to read the data from
        :param name: Name of the stream (for files this should be the file path)
        :return: An iterable of data packages
        """
        raise NotImplementedError()


class HarvestPluginTrend:
    @staticmethod
    def create_parser(config: dict) -> HarvestParserTrend:
        """
        Create and return new parser instance.

        A parser instance is a callable object that returns an iterator of
        data packages.

        :returns: A new parser object
        """
        raise NotImplementedError()
