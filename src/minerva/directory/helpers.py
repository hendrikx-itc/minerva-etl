# -*- coding: utf-8 -*-
"""
Helper functions for the directory schema.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.util import fst

from minerva.db.error import translate_postgresql_exceptions


@translate_postgresql_exceptions
def dns_to_entity_ids(cursor, dns):
    cursor.callproc("directory.dns_to_entity_ids", (dns,))

    return list(map(fst, cursor.fetchall()))
