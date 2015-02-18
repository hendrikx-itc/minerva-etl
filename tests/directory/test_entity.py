# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from nose.tools import with_setup

from minerva.directory import Entity, EntityType


DB_URL = "postgresql://unit_tester:password@localhost/unit_test"
DIRECTORY_SCHEMA = "directory"


def setup_func():
    pass


def teardown_func():
    pass


@with_setup(setup_func, teardown_func)
def test_entity_a():
    """
    Store entity
    """
    entitytype = EntityType(id=None, name="DummyTypeA", description="")
    entity1 = Entity(id=None, name="dummy1", entitytype_id=entitytype.id,
            dn="element=dummy1", parent_id=None)


@with_setup(setup_func, teardown_func)
def test_entity_with_parent():
    """
    Store entity with parent
    """
    entitytype = EntityType(id=None, name="DummyTypeA", description="")

    dn = "DummyTypeA=dummy1"
    entity1 = Entity(id=None, name="dummy1", entitytype_id=entitytype.id,
        dn=dn, parent_id=None)

    dn = dn + "DummyTypeA=dummy2"
    entity2 = Entity(id=None, name="dummy2", entitytype_id=entitytype.id,
        dn=dn, parent_id=entity1.id)
