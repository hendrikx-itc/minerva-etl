# -*- coding: utf-8 -*-
import logging
from functools import wraps
from psycopg2 import errorcodes

import psycopg2.errorcodes
from psycopg2.errorcodes import DEADLOCK_DETECTED, DUPLICATE_FUNCTION, DUPLICATE_SCHEMA, IN_FAILED_SQL_TRANSACTION, LOCK_NOT_AVAILABLE


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


class DuplicateObject(Exception):
    pass


class DuplicateFunction(Exception):
    pass


class DuplicateSchema(Exception):
    pass


class NoCopyInProgress(Exception):
    pass


class InFailedSQLTransaction(Exception):
    pass


class LockNotAvailable(Exception):
    pass


class DeadLockDetected(Exception):
    pass


postgresql_exception_mapping = {
    psycopg2.errorcodes.UNDEFINED_COLUMN: NoSuchColumnError,
    psycopg2.errorcodes.UNIQUE_VIOLATION: UniqueViolation,
    psycopg2.errorcodes.UNDEFINED_TABLE: NoSuchTable,
    psycopg2.errorcodes.DATATYPE_MISMATCH: DataTypeMismatch,
    psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE: DataTypeMismatch,
    psycopg2.errorcodes.INVALID_TEXT_REPRESENTATION: DataTypeMismatch,
    psycopg2.errorcodes.DUPLICATE_TABLE: DuplicateTable,
    psycopg2.errorcodes.DUPLICATE_FUNCTION: DuplicateFunction,
    psycopg2.errorcodes.DUPLICATE_OBJECT: DuplicateObject,
    psycopg2.errorcodes.DUPLICATE_SCHEMA: DuplicateSchema,
    psycopg2.errorcodes.IN_FAILED_SQL_TRANSACTION: InFailedSQLTransaction,
    psycopg2.errorcodes.LOCK_NOT_AVAILABLE: LockNotAvailable,
    psycopg2.errorcodes.DEADLOCK_DETECTED: DeadLockDetected}


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
