# -*- coding: utf-8 -*-
import time
import pytz
from datetime import datetime
from contextlib import closing

from nose.tools import assert_not_equal

from minerva.test import with_conn
from minerva.directory.helpers_v4 import name_to_datasource, name_to_entitytype

from .minerva_db import clear_database

from minerva.storage.attribute.datapackage import DataPackage
from minerva.storage.attribute.attributestore import AttributeStore
from minerva.storage.attribute.attribute import Attribute
from minerva.storage.attribute.retrieve import retrieve

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2017 Hendrikx-ITC B.V.
Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


@with_conn(clear_database)
def test_retrieve(conn):
    with closing(conn.cursor()) as cursor:
        time1 = pytz.utc.localize(datetime.utcnow())
        trend_names = ['CellID', 'CCR', 'Drops']
        data_rows = [
            (10023, time1, ('10023', '0.9919', '17')),
            (10047, time1, ('10047', '0.9963', '18'))
        ]
        datapackage = DataPackage(trend_names, data_rows)

        entitytype = name_to_entitytype(cursor, "UtranCell")
        datasource = name_to_datasource(cursor, "integration-test")

        data_types = ["text", "real", "smallint"]

        attributes = [Attribute(name, datatype) for name, datatype in
                      zip(trend_names, data_types)]

        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.create(cursor)

        attributestore.store_txn(datapackage).run(conn)
        time.sleep(5)

        time2 = pytz.utc.localize(datetime.utcnow())
        update_data_rows = [(10023, time2, ('10023', '0.9919', '18'))]
        update_datapackage = DataPackage(trend_names, update_data_rows)
        attributestore.store_txn(update_datapackage).run(conn)
        conn.commit()

        data = retrieve(conn, attributestore.table, trend_names, [10023])
        assert_not_equal(data, None)
