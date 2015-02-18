# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from threading import Thread
from contextlib import closing
from functools import partial

from nose.tools import raises
from minerva.test import with_conn, clear_database
from minerva.db.error import UniqueViolation
from minerva.directory import helpers_v4, DataSource, Entity


@with_conn(clear_database)
def test_dns_to_entity_ids(conn):
    dns = [
        "Network=TL,Node=001",
        "Network=TL,Node=002",
        "Network=TL,Node=003"]

    with closing(conn.cursor()) as cursor:
        entity_ids = helpers_v4.dns_to_entity_ids(cursor, dns)

    assert len(entity_ids) == 3


@with_conn()
def run_dns_to_entity_ids(conn, amount=100):
    dns = ["Network=TL,Node={}".format(i) for i in range(amount)]

    with closing(conn.cursor()) as cursor:
        entity_ids = helpers_v4.dns_to_entity_ids(cursor, dns)

    conn.commit()

    assert len(entity_ids) == amount


def test_dns_to_entity_ids_concurrent():
    """
    Concurrent execution of dns_to_entity_ids for the same previously
    non-existing Distinguished Names should result in a UniqueViolation
    exception.
    """
    tasks = [
        partial(run_dns_to_entity_ids, 10),
        partial(raises(UniqueViolation)(run_dns_to_entity_ids), 10)]

    threads = [Thread(target=task) for task in tasks]

    for t in threads:
        t.start()

    for t in threads:
        t.join()


@with_conn(clear_database)
def test_dn_to_entity(conn):
    dn = "Network=TL,Node=001"

    with closing(conn.cursor()) as cursor:
        entity = Entity.create_from_dn(cursor, dn)

    assert entity.id is not None
    assert entity.entitytype_id is not None
    assert entity.parent_id is not None
    assert entity.name == "001"


@with_conn()
def test_create_entity(conn):
    dn = "Network=TL,Node=001"

    with closing(conn.cursor()) as cursor:
        entity = Entity.create_from_dn(cursor, dn)

    assert entity is not None
    assert entity.name == "001"


@with_conn()
def test_create_data_source(conn):
    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create(
            "test-create-datasource", "short description",
        )(cursor)

    assert data_source.id is not None
    assert data_source.name == "test-create-datasource"
    assert data_source.description == "short description"


@with_conn()
def test_name_to_data_source(conn):
    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test_name_to_datasource")(cursor)

    assert data_source.id is not None

    assert data_source.name == "test_name_to_datasource"
