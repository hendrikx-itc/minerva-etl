# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.directory.helpers_v4 import dns_to_entity_ids
from minerva.storage.datatype import TYPE_ORDER


INITIAL_TYPE = TYPE_ORDER[0]


def refine_data_rows(cursor, rows):
    dns, value_rows = zip(*rows)

    entity_ids = dns_to_entity_ids(cursor, list(dns))

    refined_value_rows = map(refine_values, value_rows)

    return zip(entity_ids, refined_value_rows)


def refine_values(raw_values):
    values = []

    for value in raw_values:
        if type(value) is tuple:
            joined = ",".join(value)

            if len(joined) > 0:
                values.append(joined)
            else:
                values.append(None)
        elif len(value) == 0:
            values.append(None)
        else:
            values.append(value)

    return values
