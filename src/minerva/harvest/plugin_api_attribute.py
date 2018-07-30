# -*- coding: utf-8 -*-
from typing import Iterable, BinaryIO

from minerva.storage.attribute.datapackage import DataPackage


class HarvestParserAttribute:
    @staticmethod
    def load_packages(stream: BinaryIO, name: str) -> Iterable[DataPackage]:
        """
        Return iterable of DataPackage objects.

        :param stream: A file-like object to read the data from
        :param name: Name of the stream (for files this should be the file path)
        :return: An iterable of data packages
        """
        raise NotImplementedError()


class HarvestPluginAttribute:
    @staticmethod
    def create_parser(config: dict) -> HarvestParserAttribute:
        """
        Create and return new parser instance.

        A parser instance is a callable object that returns an iterator of
        data packages.

        :returns: A new parser object
        """
        raise NotImplementedError()
