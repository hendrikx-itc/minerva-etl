from contextlib import closing

from minerva.directory import EntityType


def test_get_entity_type(start_db_container):
    conn = start_db_container

    with closing(conn.cursor()) as cursor:
        new_entity_type = EntityType.create(
            "test_get_entity_type", "short description of type"
        )(cursor)

        entity_type = EntityType.get_by_name("test_get_entity_type")(cursor)

    assert entity_type.id == new_entity_type.id
    assert entity_type.name == "test_get_entity_type"


def test_get_entity_type_by_id(start_db_container):
    conn = start_db_container

    with closing(conn.cursor()) as cursor:
        new_entity_type = EntityType.create(
            "test_get_entitytype_by_id", "short description of type"
        )(cursor)

        entity_type = EntityType.get(new_entity_type.id)(cursor)

    assert entity_type.id == new_entity_type.id
    assert entity_type.name == "test_get_entitytype_by_id"


def test_create_entity_type(start_db_container):
    conn = start_db_container

    with closing(conn.cursor()) as cursor:
        entity_type = EntityType.create(
            "test_create_entitytype", "short description of type"
        )(cursor)

    assert entity_type.id is not None
    assert entity_type.name == "test_create_entitytype"


def test_name_to_entity_type(start_db_container):
    conn = start_db_container

    with closing(conn.cursor()) as cursor:
        entity_type = EntityType.from_name("test_name_to_entitytype")(cursor)

    assert entity_type is not None
    assert entity_type.name == "test_name_to_entitytype"
