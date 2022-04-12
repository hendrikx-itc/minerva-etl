# -*- coding: utf-8 -*-
from datetime import datetime
import unittest

import pytz

from minerva.directory import Entity, EntityType


class TestEntity(unittest.TestCase):
    def test_entity_a(self):
        """
        Store entity
        """
        entity_type = EntityType(id_=None, name="DummyTypeA", description="")

        Entity(
            id_=55,
            created=pytz.utc.localize(datetime.utcnow()),
            name="dummy1",
            entity_type_id=entity_type.id,
        )

    def test_entity_with_parent(self):
        """
        Store entity with parent
        """
        entity_type_a = EntityType(id_=68, name="DummyTypeA", description="")

        Entity(
            id_=24,
            created=pytz.utc.localize(datetime.utcnow()),
            name="dummy1",
            entity_type_id=entity_type_a.id,
        )
