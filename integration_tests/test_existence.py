from contextlib import closing
from nose.tools import eq_, ok_
from datetime import datetime
import pytz

from minerva.test import with_conn, clear_database

from minerva.directory import Entity
from minerva.directory import existence


def prepare_database(conn):
    with closing(conn.cursor()) as cursor:
        cursor.execute("DELETE FROM directory.entity")
        cursor.execute("DELETE FROM directory.existence")

    clear_database(conn)


@with_conn(prepare_database)
def test_existence_simple(conn):
    dn = "network=1,cell=1"

    dt = pytz.utc.localize(datetime(2014, 1, 1, 14, 0, 0))

    with closing(conn.cursor()) as cursor:
        Entity.create_from_dn(dn)(cursor)

    ex = existence.Existence(conn)
    ex.mark_existing([dn])
    ex.flush(dt)

    check_existence(conn, [(dn, dt, True)])


@with_conn(prepare_database)
def test_existence_onoff(conn):
    dn1 = "network=1,cell=1"
    dn2 = "network=1,cell=2"

    dt1 = pytz.utc.localize(datetime(2014, 1, 1, 14, 0, 0))
    dt2 = pytz.utc.localize(datetime(2014, 2, 1, 14, 0, 0))

    with closing(conn.cursor()) as cursor:
        Entity.create_from_dn(dn1)(cursor)
        Entity.create_from_dn(dn2)(cursor)

    ex = existence.Existence(conn)

    ex.mark_existing([dn1])
    ex.flush(dt1)

    ex.mark_existing([dn2])
    ex.flush(dt2)

    check_existence(conn, [
        (dn1, dt1, True),
        (dn2, dt1, False),
        (dn1, dt2, False),
        (dn2, dt2, True)
    ])


@with_conn(prepare_database)
def test_existence_history_on(conn):
    dn1 = "network=1,cell=1"
    dn2 = "network=1,cell=2"

    dt1 = pytz.utc.localize(datetime(2014, 1, 1, 14, 0, 0))
    dt2 = pytz.utc.localize(datetime(2014, 2, 1, 14, 0, 0))

    ex = existence.Existence(conn)

    with closing(conn.cursor()) as cursor:
        Entity.create_from_dn(dn1)(cursor)
        Entity.create_from_dn(dn2)(cursor)

    ex.mark_existing([dn1])
    ex.flush(dt2)

    ex.mark_existing([dn2])
    ex.flush(dt1)

    check_existence(conn, [
        (dn1, dt1, False),
        (dn2, dt1, True),
        (dn1, dt2, True)
    ])


@with_conn(prepare_database)
def test_existence_history_off(conn):
    dn1 = "network=1,cell=1"
    dn2 = "network=1,cell=2"

    dt1 = pytz.utc.localize(datetime(2014, 1, 1, 14, 0, 0))
    dt2 = pytz.utc.localize(datetime(2014, 2, 1, 14, 0, 0))
    dt3 = pytz.utc.localize(datetime(2014, 3, 1, 14, 0, 0))

    ex = existence.Existence(conn)

    with closing(conn.cursor()) as cursor:
        Entity.create_from_dn(dn1)(cursor)
        Entity.create_from_dn(dn2)(cursor)

    ex.mark_existing([dn1])
    ex.flush(dt1)

    ex.mark_existing([dn2])
    ex.flush(dt2)

    ex.mark_existing([dn1, dn2])
    ex.flush(dt3)

    check_existence(conn, [
        (dn1, dt1, True),
        (dn2, dt1, False),
        (dn1, dt2, False),
        (dn2, dt2, True),
        (dn1, dt3, True)
    ])


@with_conn(prepare_database)
def test_existence_duplicate(conn):
    dn = "network=1,cell=1"
    dt = pytz.utc.localize(datetime(2014, 1, 1, 14, 0, 0))

    with closing(conn.cursor()) as cursor:
        Entity.create_from_dn(dn)(cursor)

    ex = existence.Existence(conn)

    ex.mark_existing([dn])
    ex.flush(dt)

    ex.mark_existing([dn])
    ex.flush(dt)

    check_existence(conn, [(dn, dt, True)])


@with_conn(prepare_database)
def test_existence_still_on(conn):
    dn = "network=1,cell=1"
    dt1 = pytz.utc.localize(datetime(2014, 1, 1, 14, 0, 0))
    dt2 = pytz.utc.localize(datetime(2014, 2, 1, 14, 0, 0))

    with closing(conn.cursor()) as cursor:
        Entity.create_from_dn(dn)(cursor)

    ex = existence.Existence(conn)

    ex.mark_existing([dn])
    ex.flush(dt1)

    ex.mark_existing([dn])
    ex.flush(dt2)

    check_existence(conn, [(dn, dt1, True)])


@with_conn(prepare_database)
def test_duplicate(conn):
    dn1 = "network=1,cell=1"
    dn2 = "network=1,cell=2"
    dn3 = "network=1,cell=3"

    dt1 = pytz.utc.localize(datetime(2014, 1, 1, 14, 0, 0))
    dt2 = pytz.utc.localize(datetime(2014, 2, 1, 14, 0, 0))
    dt3 = pytz.utc.localize(datetime(2014, 3, 1, 14, 0, 0))
    dt4 = pytz.utc.localize(datetime(2014, 4, 1, 14, 0, 0))
    dt5 = pytz.utc.localize(datetime(2014, 5, 1, 14, 0, 0))

    ex = existence.Existence(conn)

    with closing(conn.cursor()) as cursor:
        Entity.create_from_dn(dn1)(cursor)
        Entity.create_from_dn(dn2)(cursor)
        Entity.create_from_dn(dn3)(cursor)

    ex.mark_existing([dn1])
    ex.flush(dt1)

    ex.mark_existing([dn2])
    ex.flush(dt2)

    ex.mark_existing([dn1])
    ex.flush(dt3)

    ex.mark_existing([dn2])
    ex.flush(dt4)

    ex.mark_existing([dn1])
    ex.flush(dt5)

    expected_rows = [
        (dn1, dt1, True),
        (dn2, dt1, False),
        (dn3, dt1, False),

        (dn1, dt2, False),
        (dn2, dt2, True),

        (dn1, dt3, True),
        (dn2, dt3, False),

        (dn1, dt4, False),
        (dn2, dt4, True),

        (dn1, dt5, True),
        (dn2, dt5, False)
    ]

    check_existence(conn, expected_rows)


def check_existence(conn, expected_rows):
    query = (
        "SELECT entity.dn, e.timestamp, e.exists "
        "FROM directory.existence e "
        "JOIN directory.entity on entity.id = e.entity_id "
        "ORDER BY e.timestamp, entity.id"
    )

    check_query_result(conn, query, expected_rows)


def check_query_result(conn, query, expected_rows):
    with closing(conn.cursor()) as cursor:
        cursor.execute(query)

        rows = cursor.fetchall()

    for expected_row, row in zip(expected_rows, rows):
        eq_(row, expected_row)