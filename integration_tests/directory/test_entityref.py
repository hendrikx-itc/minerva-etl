from contextlib import closing

from minerva.directory import Entity
from minerva.directory.entityref import EntityDnRef
from minerva.test import with_conn


@with_conn()
def test_get_entity_type(conn):
    dn = 'Network=G1,Node=001'

    with closing(conn.cursor()) as cursor:
        Entity.create_from_dn(cursor, dn)

    ref = EntityDnRef(dn)

    with closing(conn.cursor()) as cursor:
        entity_type = ref.get_entitytype(cursor)

    assert entity_type.name == 'Node'