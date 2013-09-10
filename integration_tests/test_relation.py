from minerva_db import with_connection

from minerva.directory import relation


@with_connection
def test_create_relationtype(conn):
    relation.create_relationtype(conn, "a->b")

    conn.commit()
