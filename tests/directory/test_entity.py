# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
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
        dn="element=dummy1",
        parent_id=None
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
        dn="DummyTypeA=dummy1",
        parent_id=None
    )

    entity_type_b = EntityType(id_=69, name="DummyTypeB", description="")

    Entity(
        id_=72,
        created=pytz.utc.localize(datetime.utcnow()),
        name="dummy2",
        entity_type_id=entity_type_b.id,
        dn=entity_1.dn + ",DummyTypeB=dummy2",
        parent_id=entity_1.id
    )
