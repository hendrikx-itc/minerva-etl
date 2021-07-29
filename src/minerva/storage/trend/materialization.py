from minerva.commands import ConfigurationError
from psycopg2 import sql


def from_config(config):
    if 'view' in config:
        return ViewMaterialization.from_config(config)
    elif 'function' in config:
        return FunctionMaterialization.from_config(config)


class Materialization:
    def __init__(self, target_trend_store_part: str):
        self.target_trend_store_part = target_trend_store_part
        self.enabled = False
        self.processing_delay = None
        self.stability_delay = None
        self.reprocessing_period = None
        self.sources = None
        self.fingerprint_function = None

    def create(self, conn):
        raise NotImplementedError()

    def update(self, conn):
        raise NotImplementedError()

    def drop(self, conn):
        undefine_materialization_query = (
            "DELETE FROM trend_directory.materialization WHERE materialization::text = %s"
        )

        undefine_materialization_args = (self.target_trend_store_part,)

        with conn.cursor() as cursor:
            cursor.execute(undefine_materialization_query, undefine_materialization_args)

            return cursor.rowcount

    def link_sources(self, conn):
        link_trend_store_query = (
            "INSERT INTO trend_directory.materialization_trend_store_link("
            "materialization_id, trend_store_part_id, timestamp_mapping_func) "
            "SELECT m.id, tsp.id, %s::regprocedure "
            "FROM trend_directory.materialization m, trend_directory.trend_store_part tsp "
            "WHERE m::text = %s and tsp.name = %s "
            "RETURNING *"
        )

        with conn.cursor() as cursor:
            for link in self.sources:
                link_trend_store_args = (
                    '{}(timestamp with time zone)'.format(link['mapping_function']),
                    self.target_trend_store_part,
                    link['trend_store_part']
                )

                cursor.execute(link_trend_store_query, link_trend_store_args)

                if cursor.rowcount == 0:
                    raise ConfigurationError(f"Could not link source trend store part '{link['trend_store_part']}'")

    def unlink_sources(self, conn):
        unlink_trend_store_query = (
            "DELETE FROM trend_directory.materialization_trend_store_link mtsl "
            "USING trend_directory.materialization m "
            "WHERE m::text = %s and m.id = mtsl.dst_trend_store_part_id"
        )

        unlink_trend_store_args = (
            self.target_trend_store_part,
        )

        with conn.cursor() as cursor:
            cursor.execute(unlink_trend_store_query, unlink_trend_store_args)

    def fingerprint_function_name(self) -> str:
        return f'{self.target_trend_store_part}_fingerprint'

    def create_fingerprint_function(self, conn):
        create_fingerprint_function_query = sql.SQL(
            'CREATE FUNCTION {}(timestamp with time zone) RETURNS trend_directory.fingerprint AS $$'
            '{}'
            '$$ LANGUAGE sql STABLE'
        ).format(
            sql.Identifier('trend', self.fingerprint_function_name()),
            sql.SQL(self.fingerprint_function)
        )

        with conn.cursor() as cursor:
            cursor.execute(create_fingerprint_function_query)

    def drop_fingerprint_function(self, conn):
        drop_fingerprint_function_query = sql.SQL(
            'DROP FUNCTION {}(timestamp with time zone)'
        ).format(
            sql.Identifier('trend', self.fingerprint_function_name())
        )

        with conn.cursor() as cursor:
            cursor.execute(drop_fingerprint_function_query)

    def set_attributes(self, conn):
        """
        Set attributes processing_delay, stability_delay, reprocessing_period
        and enabled in database to configured values.

        :param conn:
        :return:
        """
        set_enabled_query = (
            'UPDATE trend_directory.materialization '
            'SET processing_delay = %s, stability_delay = %s, reprocessing_period = %s, enabled = %s '
            'WHERE materialization::text = %s'
        )

        set_enabled_args = (
            self.processing_delay, self.stability_delay,
            self.reprocessing_period, self.enabled,
            self.target_trend_store_part
        )

        with conn.cursor() as cursor:
            cursor.execute(set_enabled_query, set_enabled_args)


class ViewMaterialization(Materialization):
    def __init__(self, target_trend_store_part: str):
        Materialization.__init__(self, target_trend_store_part)

        self.view = None

    def view_name(self) -> str:
        return f'_{self.target_trend_store_part}'

    @staticmethod
    def from_config(config: dict):
        materialization = ViewMaterialization(config['target_trend_store_part'])
        materialization.enabled = config['enabled']
        materialization.processing_delay = config['processing_delay']
        materialization.stability_delay = config['stability_delay']
        materialization.reprocessing_period = config['reprocessing_period']
        materialization.sources = config['sources']
        materialization.fingerprint_function = config['fingerprint_function']
        materialization.view = config['view']

        return materialization

    def create(self, conn):
        self.create_view(conn)
        self.define_view_materialization(conn)
        self.link_sources(conn)
        self.create_fingerprint_function(conn)
        self.set_attributes(conn)

    def update(self, conn):
        # Remove old
        self.drop_fingerprint_function(conn)
        self.drop_view(conn)

        # Create new
        self.create_view(conn)
        self.create_fingerprint_function(conn)

        self.set_attributes(conn)

    def create_view(self, conn):
        create_view_query = sql.SQL('CREATE VIEW {} AS {}').format(
            sql.Identifier('trend', self.view_name()),
            sql.SQL(self.view)
        )

        with conn.cursor() as cursor:
            cursor.execute(create_view_query)

    def drop_view(self, conn):
        create_view_query = sql.SQL(
            'DROP VIEW IF EXISTS {}'
        ).format(
            sql.Identifier('trend', self.view_name())
        )

        with conn.cursor() as cursor:
            cursor.execute(create_view_query)

    def define_view_materialization(self, conn):
        define_view_materialization_query = (
            "SELECT trend_directory.define_view_materialization("
            "id, %s::interval, %s::interval, %s::interval, %s::regclass"
            ") "
            "FROM trend_directory.trend_store_part WHERE name = %s"
        )

        define_view_materialization_args = (
            self.processing_delay,
            self.stability_delay,
            self.reprocessing_period,
            f'trend."{self.view_name()}"',
            self.target_trend_store_part
        )

        with conn.cursor() as cursor:
            cursor.execute(define_view_materialization_query, define_view_materialization_args)


DEFAULT_FUNCTION_LANGUAGE = 'plpgsql'


class FunctionMaterialization(Materialization):
    def __init__(self, target_trend_store_part: str):
        Materialization.__init__(self, target_trend_store_part)

        self.function = None

    @staticmethod
    def from_config(config: dict):
        materialization = FunctionMaterialization(config['target_trend_store_part'])
        materialization.enabled = config['enabled']
        materialization.processing_delay = config['processing_delay']
        materialization.stability_delay = config['stability_delay']
        materialization.reprocessing_period = config['reprocessing_period']
        materialization.sources = config['sources']
        materialization.fingerprint_function = config['fingerprint_function']
        materialization.function = config['function']

        return materialization

    def create(self, conn):
        self.create_function(conn)
        self.define_function_materialization(conn)
        self.link_sources(conn)
        self.create_fingerprint_function(conn)
        self.set_attributes(conn)

    def update(self, conn):
        # Remove old
        self.drop_fingerprint_function(conn)
        self.drop_function(conn)

        # Create new
        self.create_function(conn)
        self.create_fingerprint_function(conn)

        self.set_attributes(conn)

    def create_function(self, conn):
        create_function_query = sql.SQL(
            'CREATE FUNCTION {}(timestamp with time zone) RETURNS {} AS $$ '
            '{}'
            '$$ LANGUAGE {} STABLE'
        ).format(
            sql.Identifier('trend', self.target_trend_store_part),
            sql.SQL(self.function['return_type']),
            sql.SQL(self.function['src']),
            sql.SQL(self.function.get('language', DEFAULT_FUNCTION_LANGUAGE))
        )

        with conn.cursor() as cursor:
            cursor.execute(create_function_query)

    def drop_function(self, conn):
        drop_function_query = sql.SQL(
            'DROP FUNCTION {}(timestamp with time zone)'
        ).format(
            sql.Identifier('trend', self.target_trend_store_part)
        )

        with conn.cursor() as cursor:
            cursor.execute(drop_function_query)

    def define_function_materialization(self, conn):
        define_function_materialization_query = (
            "SELECT trend_directory.define_function_materialization("
            "id, %s::interval, %s::interval, %s::interval, %s::regprocedure"
            ") "
            "FROM trend_directory.trend_store_part WHERE name = %s"
        )

        define_materialization_args = (
            self.processing_delay,
            self.stability_delay,
            self.reprocessing_period,
            f'trend."{self.target_trend_store_part}"(timestamp with time zone)',
            self.target_trend_store_part
        )

        with conn.cursor() as cursor:
            cursor.execute(define_function_materialization_query, define_materialization_args)

            if cursor.rowcount == 0:
                raise ConfigurationError(
                    f"No target trend store part '{self.target_trend_store_part}' for materialization"
                )
