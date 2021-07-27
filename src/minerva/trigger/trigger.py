from contextlib import closing
from typing import List

from minerva.commands import ConfigurationError
from minerva.db import error
from psycopg2 import sql


class Trigger:
    name: str
    kpi_data: List
    kpi_function: str
    thresholds: List
    condition: str
    weight: int
    notification: str
    tags: List[str]
    fingerprint: str
    notification_store: str
    trend_store_links: List
    mapping_functions: List
    granularity: str

    def __init__(self, name, kpi_data, kpi_function, thresholds, condition,
                 weight, notification, tags, fingerprint, notification_store,
                 trend_store_links, mapping_functions, granularity):
        self.name = name
        self.kpi_data = kpi_data
        self.kpi_function = kpi_function
        self.thresholds = thresholds
        self.condition = condition
        self.weight = weight
        self.notification = notification
        self.tags = tags
        self.fingerprint = fingerprint
        self.notification_store = notification_store
        self.trend_store_links = trend_store_links
        self.mapping_functions = mapping_functions
        self.granularity = granularity

    @staticmethod
    def from_dict(data: dict):
        return Trigger(
            data["name"],
            data["kpi_data"],
            data["kpi_function"],
            data["thresholds"],
            data["condition"],
            data["weight"],
            data["notification"],
            data["tags"],
            data["fingerprint"],
            data["notification_store"],
            data["trend_store_links"],
            data["mapping_functions"],
            data["granularity"]
        )

    def create(self, conn):
        yield " - creating KPI type"

        try:
            self.create_kpi_type(conn)
        except error.DuplicateObject:
            pass

        yield " - creating KPI function"

        try:
            self.create_kpi_function(conn)
        except error.DuplicateFunction:
            pass

        # set_fingerprint(conn, config)

        yield " - creating rule"

        self.create_rule(conn)

        yield " - setting weight"

        self.set_weight(conn)

        yield " - setting thresholds"

        self.set_thresholds(conn)

        yield " - setting condition"

        self.set_condition(conn)

        yield " - defining notification"

        self.define_notification_message(conn)

        yield " - creating mapping functions"

        self.create_mapping_functions(conn)

        yield " - link trend stores"

        self.link_trend_stores(conn)

    @staticmethod
    def delete(conn, name: str):
        query = 'SELECT trigger.delete_rule(%s)'
        query_args = (name,)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)
            return cursor.fetchone()[0]

    @staticmethod
    def set_enabled(conn, name: str, enabled: bool):
        query = 'UPDATE trigger.rule SET enabled = %s WHERE name = %s RETURNING enabled'
        query_args = (enabled, name)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)
            return cursor.fetchone()

    def set_weight(self, conn):
        Trigger.set_weight_by_name(conn, self.name, self.weight)

    @staticmethod
    def execute(conn, name, timestamp=None):
        if timestamp is None:
            query = "SELECT * FROM trigger.create_notifications(%s::name)"
            query_args = (name,)
        else:
            query = "SELECT * FROM trigger.create_notifications(%s::name, %s)"
            query_args = (name, timestamp)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            notification_count, = cursor.fetchone()

            return notification_count

    def create_kpi_type(self, conn):
        type_name = '{}_kpi'.format(self.name)

        column_specs = [
            ('entity_id', 'integer'),
            ('timestamp', 'timestamp with time zone')
        ]

        column_specs.extend(
            (kpi_column['name'], kpi_column['data_type'])
            for kpi_column in self.kpi_data
        )

        columns = [
            sql.SQL('{{}} {}'.format(data_type)).format(sql.Identifier(name))
            for name, data_type in column_specs
        ]

        columns_part = sql.SQL(', ').join(columns)

        query_parts = [
            sql.SQL(
                "CREATE TYPE trigger_rule.{} AS ("
            ).format(sql.Identifier(type_name)),
            columns_part,
            sql.SQL(')')
        ]

        query = sql.SQL('').join(query_parts)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

    def create_kpi_function(self, conn, or_replace=False):
        function_name = '{}_kpi'.format(self.name)
        type_name = '{}_kpi'.format(self.name)

        if or_replace:
            create_function_part = sql.SQL("CREATE OR REPLACE FUNCTION")
        else:
            create_function_part = sql.SQL("CREATE FUNCTION")

        query_parts = [
            sql.SQL(
                '{} trigger_rule.{}(timestamp with time zone)\n'
                'RETURNS SETOF trigger_rule.{}\n'
                'AS $trigger$'
            ).format(create_function_part, sql.Identifier(function_name), sql.Identifier(type_name)),
            sql.SQL(self.kpi_function),
            sql.SQL(
                '$trigger$ LANGUAGE plpgsql STABLE;'
            )
        ]

        query = sql.SQL('').join(query_parts)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

    def define_notification_message(self, conn):
        query = 'SELECT trigger.define_notification_message(%s, %s)'

        query_args = (
            self.name,
            self.notification
        )

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

    @staticmethod
    def create_mapping_function_query(definition):
        return (
            'CREATE FUNCTION trend."{}"(timestamp with time zone) '
            'RETURNS SETOF timestamp with time zone '
            'AS $$ {} $$ LANGUAGE sql STABLE;'
        ).format(definition['name'], definition['source'])

    def create_mapping_functions(self, conn):
        queries = [
            self.create_mapping_function_query(mapping_function)
            for mapping_function in self.mapping_functions
        ]

        with closing(conn.cursor()) as cursor:
            for query in queries:
                try:
                    cursor.execute(query)
                except Exception:
                    # Function already exists
                    pass

    def link_trend_stores(self, conn):
        query = (
            'INSERT INTO trigger.rule_trend_store_link(rule_id, trend_store_part_id, timestamp_mapping_func) '
            "SELECT rule.id, trend_store_part.id, '{}(timestamp with time zone)'::regprocedure "
            'FROM trigger.rule, trend_directory.trend_store_part '
            'WHERE rule.name = %s AND trend_store_part.name = %s'
        )

        with closing(conn.cursor()) as cursor:
            for trend_store_link in self.trend_store_links:
                mapping_function = 'trend.{}'.format(trend_store_link['mapping_function'])
                formatted_query = query.format(mapping_function)

                query_args = (self.name, trend_store_link['part_name'])

                cursor.execute(formatted_query, query_args)

    def set_fingerprint(self, conn):
        query = sql.SQL('SELECT trigger.set_fingerprint({}, {});').format(
            sql.Literal(self.name),
            sql.Literal(self.fingerprint)
        )

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

    def create_rule(self, conn):
        create_query = (
            "SELECT * "
            "FROM trigger.create_rule('{}', array[{}]::trigger.threshold_def[]);"
        ).format(
            self.name,
            ','.join(
                "('{}', '{}')".format(threshold['name'], threshold['data_type'])
                for threshold in self.thresholds
            )
        )

        get_notification_store_query = (
            "SELECT * FROM notification_directory.notification_store "
            "WHERE notification_store::text = %s"
        )

        set_properties_query = (
            "UPDATE trigger.rule "
            "SET notification_store_id = notification_store.id, "
            "granularity = %s "
            "FROM notification_directory.notification_store "
            "JOIN directory.data_source "
            "ON data_source.id = notification_store.data_source_id "
            "WHERE rule.id = %s AND data_source.name = %s"
        )

        with closing(conn.cursor()) as cursor:
            cursor.execute(get_notification_store_query,
                           (self.notification_store,))

            if cursor.rowcount == 0:
                raise ConfigurationError(f'No such notification store: \
                    {self.notification_store}')

            cursor.execute(create_query)

            row = cursor.fetchone()

            rule_id, _, _, _, _, _ = row

            cursor.execute(
                set_properties_query,
                (self.granularity, rule_id, self.notification_store)
            )

    def set_thresholds(self, conn):
        function_name = '{}_set_thresholds'.format(self.name)

        query = 'SELECT trigger_rule."{}"({})'.format(
            function_name,
            ','.join(len(self.thresholds) * ['%s'])
        )

        query_args = tuple(threshold['value'] for threshold in self.thresholds)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

    def set_condition(self, conn):
        query = (
            'SELECT trigger.set_condition(rule, %s) '
            'FROM trigger.rule WHERE name = %s'
        )

        query_args = (self.condition, self.name)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

    @staticmethod
    def set_weight_by_name(conn, name: str, weight: int):
        query = (
            'SELECT trigger.set_weight(%s::name, %s::text)'
        )

        query_args = (name, str(weight))

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)
