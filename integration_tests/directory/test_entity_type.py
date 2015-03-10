from contextlib import closing

from minerva.directory import EntityType
from minerva.test import with_conn


@with_conn()
def test_get_entity_type(conn):
    with closing(conn.cursor()) as cursor:
        new_entity_type = EntityType.create(
            "test_get_entity_type", "short description of type"
        )(cursor)

        entity_type = EntityType.get_by_name("test_get_entity_type")(cursor)

    assert entity_type.id == new_entity_type.id
    assert entity_type.name == "test_get_entity_type"


@with_conn()
def test_get_entity_type_by_id(conn):
    with closing(conn.cursor()) as cursor:
        new_entity_type = EntityType.create(
            "test_get_entitytype_by_id", "short description of type"
        )(cursor)

        entity_type = EntityType.get(new_entity_type.id)(cursor)

    assert entity_type.id == new_entity_type.id
    assert entity_type.name == "test_get_entitytype_by_id"


@with_conn()
def test_create_entity_type(conn):
    with closing(conn.cursor()) as cursor:
        entity_type = EntityType.create(
            "test_create_entitytype", "short description of type"
        )(cursor)

    assert entity_type.id is not None
    assert entity_type.name == "test_create_entitytype"


@with_conn()
def test_name_to_entity_type(conn):
    with closing(conn.cursor()) as cursor:
        entity_type = EntityType.from_name("test_name_to_entitytype")(cursor)

    assert entity_type is not None
    assert entity_type.name == "test_name_to_entitytype"