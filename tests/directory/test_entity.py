# -*- coding: utf-8 -*-
from datetime import datetime

import pytz

from minerva.directory import Entity, EntityType


def test_entity_a():
    """
    Store entity
    """
    entity_type = EntityType(id_=None, name="DummyTypeA", description="")

    Entity(
        id_=55,
        created=pytz.utc.localize(datetime.utcnow()),
        name="dummy1",
        entity_type_id=entity_type.id,
        dn="element=dummy1"
    )


def test_entity_with_parent():
    """
    Store entity with parent
    """
    entity_type_a = EntityType(id_=68, name="DummyTypeA", description="")

    entity_1 = Entity(
        id_=24,
        created=pytz.utc.localize(datetime.utcnow()),
        name="dummy1",
        entity_type_id=entity_type_a.id,
        dn="DummyTypeA=dummy1"
    )

    entity_type_b = EntityType(id_=69, name="DummyTypeB", description="")

    Entity(
        id_=72,
        created=pytz.utc.localize(datetime.utcnow()),
        name="dummy2",
        entity_type_id=entity_type_b.id,
        dn=entity_1.dn + ",DummyTypeB=dummy2"
    )
