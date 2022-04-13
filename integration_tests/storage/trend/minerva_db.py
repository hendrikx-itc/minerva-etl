from contextlib import closing

from minerva.test import connect, clear_database


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

    return type(
        "C", (object,), {"__init__": __init__, "setup": setup, "teardown": teardown}
    )
