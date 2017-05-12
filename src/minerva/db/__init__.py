# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""

import urlparse


class OperationalError(Exception):
    pass


def parse_db_url(db_url):
    url = urlparse.urlparse(db_url)

    port = None

    if url.scheme == "postgresql":
        port = 5432
    elif url.scheme == "mysql":
        port = 3306

    if url.port:
        port = url.port

    if url.path:
        database = url.path[1:]
    else:
        database = None

    return (url.scheme, url.username, url.password, url.hostname, port,
            database)


def connect(url, setsession=None):
    """
    Return connection to database specified by `url`.
    """

    scheme, username, password, hostname, port, database = parse_db_url(url)

    if scheme == "mysql":
        db_api2_mod = __import__("MySQLdb")

        try:
            conn = db_api2_mod.connect(db=database, user=username,
                                       passwd=password, host=hostname, port=port)
        except db_api2_mod.OperationalError as exc:
            raise OperationalError(str(exc))
    else:
        psycopg2 = __import__("psycopg2")

        try:
            conn = psycopg2.connect(url)
        except psycopg2.OperationalError as exc:
            raise OperationalError(str(exc))

    if setsession:
        cursor = conn.cursor()
        for sql in setsession:
            cursor.execute(sql)
        cursor.close()

    return conn


def extract_safe_url(url):
    """
    Extract a url that can be written to a log file because it doesn't contain
    the password.
    """
    scheme, username, _password, hostname, port, database = parse_db_url(url)

    return "{0}://{1}@{2}:{3}/{4}".format(scheme, username, hostname, port,
                                          database)
