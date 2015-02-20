# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import logging
from functools import wraps

import psycopg2.errorcodes


class NoSuchTable(Exception):
    pass


class UniqueViolation(Exception):
    """
    Used to raise database independent exceptions for uniqueness constraint
    violations.
    """
    pass


class ExistsError(Exception):
    pass


class DataTypeMismatch(Exception):
    pass


class NoSuchColumnError(Exception):
    pass


class AggregationError(Exception):
    pass


class DuplicateTable(Exception):
    pass


class NoCopyInProgress(Exception):
    pass


postgresql_exception_mapping = {
    psycopg2.errorcodes.UNDEFINED_COLUMN: NoSuchColumnError,
    psycopg2.errorcodes.UNIQUE_VIOLATION: UniqueViolation,
    psycopg2.errorcodes.UNDEFINED_TABLE: NoSuchTable,
    psycopg2.errorcodes.DATATYPE_MISMATCH: DataTypeMismatch,
    psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE: DataTypeMismatch,
    psycopg2.errorcodes.INVALID_TEXT_REPRESENTATION: DataTypeMismatch,
    psycopg2.errorcodes.DUPLICATE_TABLE: DuplicateTable}


def translate_postgresql_exception(exc):
    exc_type = postgresql_exception_mapping.get(exc.pgcode)

    if exc_type is None:
        return exc
    else:
        return exc_type(exc.pgerror)


def translate_postgresql_exceptions(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except psycopg2.DatabaseError as exc:
            logging.debug(str(exc))
            raise translate_postgresql_exception(exc)

    return wrapped
