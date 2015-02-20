# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import logging
from contextlib import closing
from functools import partial

from minerva.test import with_conn
from minerva.directory import EntityType
from minerva.directory.query import compile_sql
from minerva.storage.trend.aggregate import get_aggregate_shard
from minerva.storage.trend.granularity import create_granularity


@with_conn
def test_get_aggregate_shard(conn):
    awacs_query = [{"type": "C", "value": ["dummy_type"]}]

    granularity = create_granularity("900")

    formula = "SUM(Drops)"

    shard_indexes = [15680]

    with closing(conn.cursor()) as cursor:
        entity_type_cell = EntityType.from_name('dummy_type')(cursor)

        sql, args, entity_id_column = compile_sql(awacs_query, None)

        select_statement = "SELECT {} AS id {}".format(
            entity_id_column, sql
        )

        entities_query = cursor.mogrify(select_statement, args)

    get_shard = partial(
        get_aggregate_shard, conn, entities_query, entity_type_cell.id,
        granularity, formula
    )

    shards = list(map(get_shard, shard_indexes))

    for shard in shards:
        logging.debug("{} - {}".format(shard[0], shard[-1]))

    assert len(shards) == len(shard_indexes)
