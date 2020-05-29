# -*- coding: utf-8 -*-
from minerva.error import ConfigurationError

import psycopg2.extras


def connect_logging(logger, **kwargs):
    conn = psycopg2.connect(
        dsn='',  # Empty dsn force use of environment variables
        connection_factory=psycopg2.extras.LoggingConnection,
        **kwargs
    )
    conn.initialize(logger)

    return conn


def connect(**kwargs):
    """
    Return new database connection.

    The kwargs are merged with the database configuration of the instance
    and passed directly to the psycopg2 connect function.
    """
    try:
        return psycopg2.connect(
            dsn='',  # Empty dsn force use of environment variables
            **kwargs
        )
    except psycopg2.OperationalError as exc:
        raise ConfigurationError(exc)

