# -*- coding: utf-8 -*-
from threading import Thread
from contextlib import closing
from functools import partial

from nose.tools import raises
from minerva.test import with_conn
from minerva.db.error import UniqueViolation
from minerva.directory import helpers_v4

from minerva_db import clear_database

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2017 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


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
    non-existing
    Distinguished Names should result in a UniqueViolation exception.
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
        entity = helpers_v4.dn_to_entity(cursor, dn)

    assert entity.id is not None
    assert entity.entitytype_id is not None
    assert entity.parent_id is not None
    assert entity.name == "001"


@with_conn()
def test_create_entity(conn):
    dn = "Network=TL,Node=001"

    with closing(conn.cursor()) as cursor:
        entity = helpers_v4.create_entity(cursor, dn)

    assert entity is not None
    assert entity.name == "001"


@with_conn()
def test_create_datasource(conn):
    with closing(conn.cursor()) as cursor:
        datasource = helpers_v4.create_datasource(
            cursor,
            "test-create-datasource",
            "short description",
            "Europe/Amsterdam")

    assert datasource.id is not None


@with_conn()
def test_name_to_datasource(conn):
    with closing(conn.cursor()) as cursor:
        datasource = helpers_v4.name_to_datasource(
            cursor, "test_name_to_datasource")

    assert datasource.id is not None


@with_conn()
def test_create_entitytype(conn):
    with closing(conn.cursor()) as cursor:
        entitytype = helpers_v4.create_entitytype(
            cursor,
            "test_create_entitytype",
            "short description of type")

    assert entitytype.id is not None
    assert entitytype.name == "test_create_entitytype"


@with_conn()
def test_get_entitytype_by_id(conn):
    with closing(conn.cursor()) as cursor:
        new_entitytype = helpers_v4.create_entitytype(
            cursor,
            "test_get_entitytype_by_id",
            "short description of type")

        entitytype = helpers_v4.get_entitytype_by_id(cursor, new_entitytype.id)

    assert entitytype.id == new_entitytype.id
    assert entitytype.name == "test_get_entitytype_by_id"


@with_conn()
def test_get_entitytype(conn):
    with closing(conn.cursor()) as cursor:
        new_entitytype = helpers_v4.create_entitytype(
            cursor,
            "test_get_entitytype",
            "short description of type")

        entitytype = helpers_v4.get_entitytype(cursor, "test_get_entitytype")

    assert entitytype.id == new_entitytype.id
    assert entitytype.name == "test_get_entitytype"


@with_conn()
def test_name_to_entitytype(conn):
    with closing(conn.cursor()) as cursor:
        entitytype = helpers_v4.name_to_entitytype(
            cursor,
            "test_name_to_entitytype")

    assert entitytype is not None
    assert entitytype.name == "test_name_to_entitytype"
