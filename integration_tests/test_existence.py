from contextlib import closing
from nose.tools import eq_, ok_
from datetime import datetime
from pytz import timezone

from minerva.test import with_conn

from minerva_db import clear_database

from minerva import directory
from minerva.directory import existence

def prepare_datebase(conn):
    with closing(conn.cursor()) as cursor:
        cursor.execute("DELETE FROM directory.entity")
        cursor.execute("DELETE FROM directory.existence")

    clear_database(conn)


def get_existence_curr(conn):
    with closing(conn.cursor()) as cursor:
        cursor.execute("""
SELECT entity.dn, e.timestamp, e.exists
FROM directory.existence_curr e
JOIN directory.entity on entity.id = e.entity_id
ORDER BY entity.id, e.timestamp""")
        return cursor.fetchall()


def check_existence(conn, rows):
    with closing(conn.cursor()) as cursor:
        cursor.execute("""
SELECT entity.dn, e.timestamp, e.exists
FROM directory.existence e
JOIN directory.entity on entity.id = e.entity_id
ORDER BY e.timestamp, entity.id""")

        data_rows = cursor.fetchall()

    if len(rows) != len(data_rows):
        return False

    for index, row in enumerate(data_rows):
        if row != rows[index]:
            return False

    return True


TIMEZONE = timezone("Europe/Amsterdam")


@with_conn(prepare_datebase)
def test_existence_simple(conn):
    dn = "network=1,cell=1"

    dt = TIMEZONE.localize(datetime(2014, 01, 01, 14, 0, 0))

    directory.create_entity(conn, dn)

    ex = existence.Existence(conn)
    ex.mark_existing([dn])
    ex.flush(dt)

    ok_(check_existence(conn, [(dn,dt,True)]))


@with_conn(prepare_datebase)
def test_existence_onoff(conn):
    with closing(conn.cursor()) as cursor:
        dn1 = "network=1,cell=1"
        dn2 = "network=1,cell=2"

        dt1 = TIMEZONE.localize(datetime(2014, 01, 01, 14, 0, 0))
        dt2 = TIMEZONE.localize(datetime(2014, 02, 01, 14, 0, 0))

        ex = existence.Existence(conn)

        directory.create_entity(conn, dn1)
        directory.create_entity(conn, dn2)

        ex.mark_existing([dn1])
        ex.flush(dt1)

        ex.mark_existing([dn2])
        ex.flush(dt2)

        ok_(check_existence(conn, [(dn1, dt1, True), (dn1, dt2, False), (dn2, dt2, True)]))


@with_conn(prepare_datebase)
def test_existence_history_on(conn):
    with closing(conn.cursor()) as cursor:
        dn1 = "network=1,cell=1"
        dn2 = "network=1,cell=2"

        dt1 = TIMEZONE.localize(datetime(2014, 01, 01, 14, 0, 0))
        dt2 = TIMEZONE.localize(datetime(2014, 02, 01, 14, 0, 0))

        ex = existence.Existence(conn)

        directory.create_entity(conn, dn1)
        directory.create_entity(conn, dn2)

        ex.mark_existing([dn1])
        ex.flush(dt2)

        ex.mark_existing([dn2])
        ex.flush(dt1)

        ok_(check_existence(conn, [(dn2, dt1, True), (dn1, dt2, True)]))


@with_conn(prepare_datebase)
def test_existence_history_off(conn):
    with closing(conn.cursor()) as cursor:
        dn1 = "network=1,cell=1"
        dn2 = "network=1,cell=2"

        dt1 = TIMEZONE.localize(datetime(2014, 01, 01, 14, 0, 0))
        dt2 = TIMEZONE.localize(datetime(2014, 02, 01, 14, 0, 0))
        dt3 = TIMEZONE.localize(datetime(2014, 03, 01, 14, 0, 0))

        ex = existence.Existence(conn)

        directory.create_entity(conn, dn1)
        directory.create_entity(conn, dn2)

        ex.mark_existing([dn1])
        ex.flush(dt1)

        ex.mark_existing([dn2])
        ex.flush(dt2)

        ex.mark_existing([dn1, dn2])
        ex.flush(dt3)

        ok_(check_existence(conn, [(dn1, dt1, True), (dn1, dt2, False), (dn2, dt2, True), (dn1, dt3, True)]))


@with_conn(prepare_datebase)
def test_existence_duplicate(conn):
    dn = "network=1,cell=1"
    dt = TIMEZONE.localize(datetime(2014, 01, 01, 14, 0, 0))

    directory.create_entity(conn, dn)
    ex = existence.Existence(conn)

    ex.mark_existing([dn])
    ex.flush(dt)

    ex.mark_existing([dn])
    ex.flush(dt)

    ok_(check_existence(conn, [(dn,dt,True)]))


@with_conn(prepare_datebase)
def test_existence_still_on(conn):
    dn = "network=1,cell=1"
    dt1 = TIMEZONE.localize(datetime(2014, 01, 01, 14, 0, 0))
    dt2 = TIMEZONE.localize(datetime(2014, 02, 01, 14, 0, 0))

    directory.create_entity(conn, dn)
    ex = existence.Existence(conn)

    ex.mark_existing([dn])
    ex.flush(dt1)

    ex.mark_existing([dn])
    ex.flush(dt2)

    ok_(check_existence(conn, [(dn,dt1,True)]))
