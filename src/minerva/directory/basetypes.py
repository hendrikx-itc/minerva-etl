# -*- coding: utf-8 -*-
"""Provides classes for base types like EntityType and DataSource."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import datetime

import pytz


class EntityType:
    def __init__(self, id, name, description):
        self.id = id
        self.name = name
        self.description = description

    def __repr__(self):
        return "<EntityType({0})>".format(self.name)

    def __str__(self):
        return self.name

    @staticmethod
    def create(cursor, name, description):
        """Create new entity type and add it to the database."""
        query = (
            "INSERT INTO directory.entitytype (id, name, description) "
            "VALUES (DEFAULT, %s, %s) "
            "RETURNING *")

        args = name, description

        cursor.execute(query, args)

        return EntityType(*cursor.fetchone())

    @staticmethod
    def get(cursor, entitytype_id):
        """Return the entity type matching the specified Id."""
        query = (
            "SELECT id, name, description "
            "FROM directory.entitytype "
            "WHERE id = %s")

        args = (entitytype_id,)

        cursor.execute(query, args)

        if cursor.rowcount > 0:
            return EntityType(*cursor.fetchone())

    @staticmethod
    def get_by_name(cursor, name):
        """Return the entitytype with name `name`."""
        sql = (
            "SELECT id, name, description "
            "FROM directory.entitytype "
            "WHERE lower(name) = lower(%s)")

        args = (name,)

        cursor.execute(sql, args)

        if cursor.rowcount > 0:
            return EntityType(*cursor.fetchone())

    @staticmethod
    def from_name(cursor, name):
        """
        Return new or existing entitytype with name `name`.
        """
        args = (name, )

        cursor.callproc("directory.name_to_entity_type", args)

        if cursor.rowcount > 0:
            return EntityType(*cursor.fetchone())


class Entity:
    """
    All data within the Minerva platform is linked to entities. Entities are
    very minimal objects with only very generic properties such as name,
    type and a few more.
    """
    def __init__(self, id, name, entitytype_id):
        self.id = id
        self.first_appearance = pytz.utc.localize(datetime.datetime.utcnow())
        self.name = name
        self.entitytype_id = entitytype_id

    def __repr__(self):
        return "<Entity('{0:s}')>".format(self.name)

    def __str__(self):
        return self.name

    @staticmethod
    def create_from_dn(cursor, dn):
        """
        :param conn: A cursor an a Minerva Directory database.
        :param dn: The distinguished name of the entity.
        """
        cursor.callproc("directory.create_entity_from_dn", (dn,))

        row = cursor.fetchone()

        _first_appearance, dn, entitytype_id, id = row

        return Entity(id, dn, entitytype_id)

    @staticmethod
    def get(cursor, entity_id):
        """Return entity with specified distinguished name."""
        args = (entity_id,)

        cursor.callproc("directory.get_entity_by_id", args)

        (_first_appearance, name, entitytype_id, id) = cursor.fetchone()

        return Entity(id, name, entitytype_id)

    @staticmethod
    def get_by_dn(cursor, dn):
        """Return entity with specified distinguished name."""
        args = (dn,)

        cursor.callproc("directory.get_entity_by_dn", args)

        if cursor.rowcount == 1:
            (_first_appearance, name, entitytype_id, id) = cursor.fetchone()

            return Entity(id, name, entitytype_id)

    @staticmethod
    def from_dn(cursor, dn):
        cursor.callproc("directory.dn_to_entity", (dn,))

        row = cursor.fetchone()

        _first_appearance, dn, entitytype_id, id = row

        return Entity(id, dn, entitytype_id)


class DataSource:
    """
    A DataSource describes where a certain set of data comes from.
    """
    def __init__(self, id, name, description="", timezone="UTC"):
        self.id = id
        self.name = name
        self.description = description
        self.timezone = timezone

    def __str__(self):
        return self.name

    def get_tzinfo(self):
        return pytz.timezone(self.timezone)

    def set_tzinfo(self, tzinfo):
        self.timezone = tzinfo.zone

    tzinfo = property(get_tzinfo, set_tzinfo)

    @staticmethod
    def create(cursor, name, description, timezone):
        """
        Create new datasource
        :param cursor: cursor instance used to store into the Minerva database.
        :param name: identifying name of data source.
        :param description: A short description.
        :param timezone: Timezone of data originating from data source.
        """
        query = (
            "INSERT INTO directory.datasource "
            "(id, name, description, timezone) "
            "VALUES (DEFAULT, %s, %s, %s) RETURNING *")

        args = name, description, timezone

        cursor.execute(query, args)

        return DataSource(*cursor.fetchone())

    @staticmethod
    def get(cursor, datasource_id):
        """Return the datasource with the specified Id."""
        query = (
            "SELECT id, name, description, timezone "
            "FROM directory.datasource "
            "WHERE id=%s")

        args = (datasource_id,)

        cursor.execute(query, args)

        if cursor.rowcount > 0:
            return DataSource(*cursor.fetchone())

    @staticmethod
    def get_by_name(cursor, name):
        """Return the datasource with the specified name."""
        query = (
            "SELECT id, name, description, timezone "
            "FROM directory.datasource "
            "WHERE lower(name)=lower(%s)")

        args = (name,)

        cursor.execute(query, args)

        if cursor.rowcount > 0:
            return DataSource(*cursor.fetchone())

    @staticmethod
    def from_name(cursor, name):
        """Return new or existing datasource with name `name`."""
        cursor.callproc("directory.name_to_data_source", (name,))

        if cursor.rowcount > 0:
            return DataSource(*cursor.fetchone())


class TagGroup:
    def __init__(self, id, name, complementary):
        self.id = id
        self.name = name
        self.complementary = complementary


class Tag:
    def __init__(self, id, name, group_id, description=""):
        self.id = id
        self.name = name
        self.description = description
        self.group_id = group_id
