from contextlib import closing

from minerva.test import connect


def clear_database(conn):
    with closing(conn.cursor()) as cursor:
        cursor.execute("DELETE FROM trend_directory.trend CASCADE")
        cursor.execute("DELETE FROM trend_directory.trendstore CASCADE")
        cursor.execute("DELETE FROM directory.datasource CASCADE")
        cursor.execute("DELETE FROM directory.entitytype CASCADE")


def with_data(test_set):
    def __init__(i):
        i.data = None
        i.conn = None

    def setup(i):
        i.data = test_set()
        i.conn = connect()

        clear_database(i.conn)

        with closing(i.conn.cursor()) as cursor:
            i.data.load(cursor)

        i.conn.commit()

    def teardown(i):
        i.conn.rollback()
        i.conn.close()

    return type('C', (object,), {
        "__init__": __init__,
        "setup": setup,
        "teardown": teardown
    })

