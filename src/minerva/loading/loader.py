# -*- coding: utf-8 -*-
import logging
from contextlib import contextmanager
from contextlib import closing
from operator import itemgetter
from functools import partial
import re
from pathlib import Path

from minerva.storage.trend.trendstore import NoSuchTrendStore
from minerva.util import compose, k
from minerva.directory import DataSource
import minerva.storage.trend.datapackage
from minerva.storage.trend.datapackage import DataPackage
from minerva.directory.entitytype import NoSuchEntityType, EntityType
from minerva.harvest.fileprocessor import process_file
from minerva.db import connect, connect_logging
from minerva.harvest.plugins import get_plugin


class ConfigurationError(Exception):
    pass


class Loader:
    statistics: bool
    pretend: bool
    debug: bool
    data_source: str
    show_progress: bool
    merge_packages: bool
    stop_on_missing_entity_type: bool

    def __init__(self):
        self.statistics = False
        self.pretend = True
        self.debug = False
        self.data_source = 'loader'
        self.show_progress = False
        self.merge_packages = True
        self.stop_on_missing_entity_type = False

    def load_data(self, file_type: str, config: dict, file_path: Path):
        """
        Load the data in the file specified by `file_path` of type
        `file_type`. The parser will be configured with `config`.
        :param file_type: The type of file
        :param config: The parser configuration
        :param file_path: The file to process
        :return:
        """
        statistics = Statistics()

        plugin = get_plugin(file_type)

        parser = plugin.create_parser(config)

        if self.pretend:
            storage_provider = store_dummy
        else:
            if self.debug:
                connect_to_db = partial(
                    connect_logging, logging.getLogger('psycopg2')
                )
            else:
                connect_to_db = connect

            storage_provider = create_store_db_context(
                self.data_source, parser.store_command(), connect_to_db,
            )

        try:
            with storage_provider() as store:
                def handle_package(package, action: dict):
                    if self.debug:
                        print(package.render_table())

                    store(package, action)

                logging.info(
                    "Start processing file {0} of type {1}"
                    " and config {2}".format(
                        file_path, file_type, config
                    )
                )

                action = {
                    'type': 'load-data',
                    'file_type': file_type,
                    'uri': str(file_path)
                }

                packages_generator = process_file(
                    file_path, parser, self.show_progress
                )

                if self.merge_packages:
                    packages = DataPackage.merge_packages(packages_generator)
                else:
                    packages = packages_generator

                for package in packages:
                    try:
                        handle_package(package, action)
                    except NoSuchEntityType as exc:
                        if self.stop_on_missing_entity_type:
                            raise ConfigurationError(
                                'No such entity type \'{entity_type}\'\n'
                                'Create a data source using e.g.\n'
                                '\n'
                                '    minerva entity-type create {entity_type}\n'.format(
                                    entity_type=exc.entity_type_name
                                )
                            )
                        else:
                            logging.warning(exc)

        except ConfigurationError as err:
            print('fatal: {}'.format(err))

        if self.statistics:
            for line in statistics.report():
                logging.info(line)


def create_regex_filter(x):
    if x:
        return re.compile(x).match
    else:
        return k(True)


class Statistics(object):
    def __init__(self):
        self.package_count = 0

    def extract_statistics(self, package):
        self.package_count += 1

        return package

    def report(self):
        return [
            "{} packages".format(self.package_count)
        ]


def filter_trend_package(entity_filter, trend_filter, package: DataPackage):
    filtered_trend_names = list(filter(trend_filter, package.trend_descriptors))

    trend_filter_map = map(trend_filter, package.trend_descriptors)

    filter_rows = compose(entity_filter, itemgetter(0))

    entity_filtered_rows = filter(filter_rows, package.rows)

    filtered_rows = []

    for row in entity_filtered_rows:
        values = row[-1]

        trend_filtered_values = [
            v
            for include, v in
            zip(trend_filter_map, values)
            if include
        ]

        trend_filtered_row = tuple(row[:-1]) + (trend_filtered_values,)

        filtered_rows.append(trend_filtered_row)

    return minerva.storage.trend.datapackage.DataPackage(
        package.data_package_type, package.granularity, filtered_trend_names,
        filtered_rows
    )


def filter_attribute_package(entity_filter, attribute_filter, package):
    filtered_attribute_names = filter(attribute_filter, package.attribute_names)

    attribute_filter_map = map(attribute_filter, package.attribute_names)

    filter_rows = compose(entity_filter, itemgetter(0))

    entity_filtered_rows = filter(filter_rows, package.rows)

    filtered_rows = []

    for row in entity_filtered_rows:
        values = row[-1]

        attribute_filtered_values = [
            v
            for include, v in
            zip(attribute_filter_map, values)
            if include
        ]

        attribute_filtered_row = row[:-1] + (attribute_filtered_values,)

        filtered_rows.append(attribute_filtered_row)

    return minerva.storage.attribute.datapackage.DataPackage(
        filtered_attribute_names,
        filtered_rows)


package_data_filters = {
    "trend": filter_trend_package,
    "attribute": filter_attribute_package
}


def tee(fn):
    def wrapper(x):
        fn(x)

        return x

    return wrapper


def create_store_db_context(
        data_source_name: str, store_cmd, connect_to_db,
        stop_on_missing_trend_store=False
):
    @contextmanager
    def store_db_context():
        with closing(connect_to_db()) as conn:
            with closing(conn.cursor()) as cursor:
                data_source = DataSource.get_by_name(data_source_name)(cursor)

            if data_source is None:
                raise no_such_data_source_error(data_source_name)

            def store_package(package, action):
                try:
                    store_cmd(package, action)(data_source)(conn)
                except NoSuchTrendStore as exc:
                    if stop_on_missing_trend_store:
                        raise no_such_trend_store_error(
                            exc.data_source, exc.entity_type, str(exc.granularity)
                        )
                    else:
                        logging.warning(str(exc))

            yield store_package

    return store_db_context


def no_such_data_source_error(data_source_name: str) -> ConfigurationError:
    return ConfigurationError(
        'No such data source \'{data_source}\'\n'
        'Create a data source using e.g.\n'
        '\n'
        '    minerva data-source create {data_source}\n'.format(
            data_source=data_source_name
        )
    )


def no_such_trend_store_error(data_source: DataSource, entity_type: EntityType, granularity: str) -> ConfigurationError:
    return ConfigurationError(
        'No table trend store found for the combination (data source: '
        '{data_source}, entity type: {entity_type}, granularity: {granularity}'
        ')\n'
        'Create a table trend store using e.g.\n'
        '\n'
        '    minerva trend-store create --from-json'.format(
            data_source=data_source.name,
            entity_type=entity_type.name,
            granularity=granularity
        )
    )


@contextmanager
def store_dummy():
    yield no_op


def no_op(*args, **kwargs):
    pass
