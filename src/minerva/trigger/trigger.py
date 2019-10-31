from contextlib import closing

from psycopg2 import sql
import psycopg2


class Trigger:
    def __init__(self, name):
        self.name = name
        self.config = None

    @staticmethod
    def from_config(config):
        trigger = Trigger(config['name'])
        trigger.config = config

        return trigger

    def create(self, conn):
        yield " - creating KPI type"

        try:
            create_kpi_type(conn, self.config)
        except psycopg2.errors.DuplicateObject as exc:
            # Type already exists
            yield 'Type exists already'

        yield " - creating KPI function"

        try:
            create_kpi_function(conn, self.config)
        except psycopg2.errors.DuplicateFunction as exc:
            # Function already exists
            yield 'Function exists already'

        #set_fingerprint(conn, config)

        yield " - creating rule"

        create_rule(conn, self.config)

        yield " - setting thresholds"

        set_thresholds(conn, self.config)

        yield " - setting condition"

        set_condition(conn, self.config)

        yield " - defining notification"

        define_notification(conn, self.config)

    def delete(self, conn):
        query = 'SELECT trigger.delete_rule(%s)'
        query_args = (self.name,)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

    def execute(self, conn, timestamp):
        query = "SELECT * FROM trigger.create_notifications(%s, %s)"
        query_args = (self.name, timestamp)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            row = cursor.fetchone()

            return row


def create_kpi_type(conn, config):
    type_name = '{}_kpi'.format(config['name'])

    column_specs = [
        ('entity_id', 'integer'),
        ('timestamp', 'timestamp with time zone')
    ]

    column_specs.extend(
        (kpi_column['name'], kpi_column['data_type'])
        for kpi_column in config['kpi_data']
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


def create_kpi_function(conn, config):
    function_name = '{}_kpi'.format(config['name'])
    type_name = '{}_kpi'.format(config['name'])

    query_parts = [
        sql.SQL(
            'CREATE FUNCTION trigger_rule.{}(timestamp with time zone)\n'
            'RETURNS SETOF trigger_rule.{}\n'
            'AS $trigger$'
        ).format(sql.Identifier(function_name), sql.Identifier(type_name)),
        sql.SQL(config['kpi_function']),
        sql.SQL(
            '$trigger$ LANGUAGE plpgsql STABLE;'
        )
    ]

    query = sql.SQL('').join(query_parts)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)


def define_notification(conn, config):
    query = 'SELECT trigger.define_notification(%s, %s)'

    query_args = (
        config['name'],
        config['notification']
    )

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, query_args)


def set_fingerprint(conn, config):
    query = sql.SQL('SELECT trigger.set_fingerprint({}, {});').format(
        sql.Literal(config['name']),
        sql.Literal(config['fingerprint'])
    )

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)


def create_rule(conn, config):
    create_query = (
        "SELECT * "
        "FROM trigger.create_rule('{}', array[{}]::trigger.threshold_def[]);"
    ).format(
        config['name'],
        ','.join(
            "('{}', '{}')".format(threshold['name'], threshold['data_type'])
            for threshold in config['thresholds']
        )
    )

    set_notification_store_query = (
        "UPDATE trigger.rule "
        "SET notification_store_id = notification_store.id "
        "FROM notification_directory.notification_store "
        "JOIN directory.data_source "
        "ON data_source.id = notification_store.data_source_id "
        "WHERE rule.id = %s AND data_source.name = %s"
    )

    with closing(conn.cursor()) as cursor:
        cursor.execute(create_query)

        row = cursor.fetchone()

        rule_id, _, _, _, _, _ = row

        cursor.execute(
            set_notification_store_query,
            (rule_id, config['notification_store'])
        )


def set_thresholds(conn, config):
    function_name = '{}_set_thresholds'.format(config['name'])

    query = 'SELECT trigger_rule."{}"({})'.format(
        function_name,
        ','.join(len(config['thresholds']) * ['%s'])
    )

    query_args = tuple(threshold['value'] for threshold in config['thresholds'])

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, query_args)


def set_condition(conn, config):
    query = (
        'SELECT trigger.set_condition(rule, %s) '
        'FROM trigger.rule WHERE name = %s'
    )

    query_args = (config['condition'], config['name'])

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, query_args)
