from contextlib import closing, contextmanager

from minerva.test import connect, clear_database


@contextmanager
def with_data_context(conn, test_set):
    data = test_set()

    data.load(conn)

    yield data


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
