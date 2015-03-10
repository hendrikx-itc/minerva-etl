from contextlib import closing

from minerva.directory import DataSource
from minerva.test import with_conn


@with_conn()
def test_create_data_source(conn):
    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create(
            "TestSource", "short description of data source"
        )(cursor)

    assert data_source.id is not None
    assert data_source.name == "TestSource"
