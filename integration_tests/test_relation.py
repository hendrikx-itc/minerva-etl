# -*- coding: utf-8 -*-
from minerva.test import clear_database

from minerva.directory.relationtype import RelationType


def test_create_relationtype(start_db_container):
    conn = clear_database(start_db_container)

    with conn.cursor() as cursor:
        RelationType.create(
            RelationType.Descriptor("test-relation-type", "one-to-one")
        )(cursor)
