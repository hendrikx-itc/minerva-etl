from contextlib import closing

from minerva.directory import DataSource


def test_create_data_source(start_db_container):
    conn = start_db_container

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create(
            "TestSource", "short description of data source"
        )(cursor)

    assert data_source.id is not None
    assert data_source.name == "TestSource"
