# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import logging
from contextlib import closing
from functools import partial

from nose.tools import eq_
from minerva.directory.helpers_v4 import name_to_entitytype
from minerva.directory.query import compile_sql
from minerva.storage.trend.aggregate import get_aggregate_shard
from minerva.storage.trend.granularity import create_granularity

from minerva_db import with_data
from data import TestData


class TestAggregate(with_data(TestData)):
    def test_get_aggregate_shard(self):
        awacs_query = [{"type": "C", "value": ["dummy_type"]}]

        granularity = create_granularity("900")

        formula = "SUM(Drops)"

        shard_indexes = [15680]

        with closing(self.conn.cursor()) as cursor:
            entitytype_cell = name_to_entitytype(cursor, 'dummy_type')

            sql, args, entity_id_column = compile_sql(awacs_query, None)

            select_statement = "SELECT {} AS id {}".format(entity_id_column, sql)

            entities_query = cursor.mogrify(select_statement, args)

        get_shard = partial(get_aggregate_shard, self.conn, entities_query,
                entitytype_cell.id, granularity, formula)

        shards = map(get_shard, shard_indexes)

        for shard in shards:
            logging.debug("{} - {}".format(shard[0], shard[-1]))

        eq_(len(shards), len(shard_indexes))
