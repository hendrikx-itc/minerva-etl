# -*- coding: utf-8 -*-
"""
Helper functions for the directory schema.
"""
from minerva.util import fst

from minerva.db.error import translate_postgresql_exceptions


@translate_postgresql_exceptions
def dns_to_entity_ids(cursor, dns):
    cursor.callproc("directory.dns_to_entity_ids", (dns,))

    return list(map(fst, cursor.fetchall()))
