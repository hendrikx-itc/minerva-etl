from contextlib import closing

from minerva.directory import EntityType
from minerva.directory.entityref import entity_name_ref_class
from minerva.directory.helpers import create_entity_from_name


def test_get_entity_type(start_db_container):
    conn = start_db_container

    CellRef = entity_name_ref_class('Node')

    node_name = 'Network=G1,Node=001'

    with closing(conn.cursor()) as cursor:
        EntityType.create('Node', '')(cursor)
        create_entity_from_name(cursor, 'Node', node_name)

    ref = CellRef(node_name)

    with closing(conn.cursor()) as cursor:
        entity_type = ref.get_entity_type(cursor)

    assert entity_type.name == 'Node'
