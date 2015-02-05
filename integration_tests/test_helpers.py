# -*- coding: utf-8 -*-
"""
Unit tests for functions provided by the minerva.directory.helpers module.
"""
import mock
import psycopg2
from datetime import datetime
from nose.tools import assert_raises, raises, eq_

from minerva.directory.basetypes import EntityType, Entity
from minerva.directory import helpers
from minerva.db.error import UniqueViolation


def test_create_entitytype():
    entitytype_id = 42
    entitytype_name = "Dummy1"
    entitytype_descr = "Description of Dummy1"

    mock_conn = mock.Mock()
    mock_cursor = mock.Mock()
    mock_cursor.fetchone.return_value = (entitytype_id, )
    mock_conn.cursor.return_value = mock_cursor

    entitytype = helpers.create_entitytype(mock_conn, entitytype_name,
                                           entitytype_descr)

    assert entitytype.id == entitytype_id
    assert entitytype.name == entitytype_name
    assert entitytype.description == entitytype_descr


@raises(Exception)
def test_create_entity_empty_dn():
    dn = ""

    mock_conn = mock.Mock()

    helpers.create_entity(mock_conn, dn)


@mock.patch('minerva.directory.helpers.get_entity')
@mock.patch('minerva.directory.helpers.get_entitytype')
def test_create_entity(mock_get_entitytype, mock_get_entity):
    dn = "Network=One,Element=Two"

    entitytype = EntityType(33, "testtype", "")
    parent_entity = Entity(42, "One", 18, "Network=One", None)

    mock_conn = mock.Mock()
    mock_cursor = mock.Mock()
    mock_conn.cursor.return_value = mock_cursor

    mock_cursor.fetchone.return_value = (
        1,               # id
        datetime.now(),  # first_appearance
        'Two',           # name
        33,              # entitytype_id
        dn,              # dn
        42               # parent_id
    )

    mock_get_entitytype.return_value = entitytype
    mock_get_entity.return_value = parent_entity

    entity = helpers.create_entity(mock_conn, dn)

    assert entity.name == "Two"
    eq_(entity.entitytype_id, entitytype.id)
    assert entity.dn == dn
    assert entity.parent_id == parent_entity.id


def test_create_entitytype_existing():
    """
    When psycopg2 raises an IntegrityError, create_entitytype should in turn
    raise a UniqueViolation exception.
    """
    entitytype_name = "Dummy1"
    entitytype_descr = "Description of Dummy1"

    mock_conn = mock.Mock()
    mock_cursor = mock.Mock()

    exc = psycopg2.Error()
    exc.pgcode = psycopg2.errorcodes.UNIQUE_VIOLATION

    mock_cursor.execute.side_effect = exc
    mock_conn.cursor.return_value = mock_cursor

    assert_raises(UniqueViolation, helpers.create_entitytype, mock_conn,
                  entitytype_name, entitytype_descr)


def test_get_entitytype():
    """
    Check normal functioning of get_entitytype, with an existing entitytype.
    """
    entitytype_id = 42
    entitytype_name = "Dummy1"
    entitytype_descr = "Description of Dummy1"

    mock_conn = mock.Mock()
    mock_cursor = mock.Mock()
    mock_cursor.fetchone.return_value = (entitytype_id, entitytype_name,
                                         entitytype_descr)
    mock_conn.cursor.return_value = mock_cursor

    entitytype = helpers.get_entitytype(mock_conn, entitytype_name)

    assert entitytype.id == entitytype_id
    assert entitytype.name == entitytype_name
    assert entitytype.description == entitytype_descr


def test_get_entitytype_by_id():
    """
    Check normal functioning of get_entitytype_by_id, with an existing
    entitytype.
    """
    entitytype_id = 42
    entitytype_name = "Dummy1"
    entitytype_descr = "Description of Dummy1"

    mock_conn = mock.Mock()
    mock_cursor = mock.Mock()
    mock_cursor.fetchone.return_value = (entitytype_name, entitytype_descr)
    mock_conn.cursor.return_value = mock_cursor

    entitytype = helpers.get_entitytype_by_id(mock_conn, entitytype_id)

    assert entitytype.id == entitytype_id
    assert entitytype.name == entitytype_name
    assert entitytype.description == entitytype_descr
