# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import psycopg2.extras


def connect_logging(logger, **kwargs):
    conn = psycopg2.connect(
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
    return psycopg2.connect(**kwargs)
