import os

from minerva.db import connect, parse_db_url

from nose.tools import eq_, raises, assert_not_equal


def test_connect():
    db_url = os.getenv("TEST_DB_URL")

    conn = connect(db_url)

    conn.close()


def test_connect_without_database():
    db_url = os.getenv("TEST_DB_URL")

    scheme, user, password, host, port, database = parse_db_url(db_url)

    assert len(scheme) > 0

    assert len(user) > 0

    assert len(password) > 0

    assert len(host) > 0

    assert port > 0

    db_url_without_database = "{}://{}:{}@{}".format(
            scheme, user, password, host)

    print(db_url_without_database)

    conn = connect(db_url_without_database)

    conn.close()
