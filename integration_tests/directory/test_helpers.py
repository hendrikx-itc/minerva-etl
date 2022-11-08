from contextlib import closing

from minerva.directory import EntityType
from minerva.directory.helpers import create_entity_from_name, names_to_entity_ids


def test_names_to_entity_ids(start_db_container):
    conn = start_db_container

    node_names = [
        'ODD1/Network=G1,Node=001',
        'ODD1/Network=G1,Node=005',
        'ODD1/Network=G1,Node=011',
        'ODD1/Network=G1,Node=012',
        'ODD1/Network=G1,Node=017',
        'ODD1/Network=G1,Node=029',
        'ODD1/Network=G1,Node=030',
    ]

    node_ids = {}

    with closing(conn.cursor()) as cursor:
        EntityType.create('Node', '')(cursor)

        for node_name in node_names:
            node_id = create_entity_from_name(cursor, 'Node', node_name)

            # Register the node Ids for verification
            node_ids[node_name] = node_id

    ordered_names = [
        'ODD1/Network=G1,Node=017',
        'ODD1/Network=G1,Node=011',
        'ODD1/Network=G1,Node=029',
        'ODD1/Network=G1,Node=012',
        'ODD1/Network=G1,Node=001',
    ]

    with closing(conn.cursor()) as cursor:
        entity_ids = names_to_entity_ids(cursor, 'Node', ordered_names)

    for name, entity_id in zip(ordered_names, entity_ids):
        assert node_ids[name] == entity_id
