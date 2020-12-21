from typing import Callable, Any, Optional


class NoSuchEntityType(Exception):
    def __init__(self, entity_type_name):
        self.entity_type_name = entity_type_name

    def __str__(self):
        return "No such entity type '{}'".format(
            self.entity_type_name
        )


class EntityType:
    def __init__(self, id_: int, name: str, description: str):
        self.id = id_
        self.name = name
        self.description = description

    def __repr__(self) -> str:
        return "<EntityType({0})>".format(self.name)

    def __str__(self) -> str:
        return self.name

    @staticmethod
    def create(name: str, description: str) -> Callable[[Any], Optional["EntityType"]]:
        """Create new entity type and add it to the database."""
        def f(cursor):
            query = (
                "SELECT id, name, description FROM directory.create_entity_type(%s)"
            )

            args = name,

            cursor.execute(query, args)

            row = cursor.fetchone()

            return EntityType(*row)

        return f

    @staticmethod
    def get(entity_type_id: int) -> Callable[[Any], Optional["EntityType"]]:
        """Return the entity type matching the specified Id."""
        def f(cursor):
            query = (
                "SELECT id, name, description "
                "FROM directory.entity_type "
                "WHERE id = %s")

            args = (entity_type_id,)

            cursor.execute(query, args)

            if cursor.rowcount > 0:
                return EntityType(*cursor.fetchone())

        return f

    @staticmethod
    def get_by_name(name: str) -> Callable[[Any], Optional["EntityType"]]:
        """Return the entity type with name `name`."""
        def f(cursor):
            sql = (
                "SELECT id, name, description "
                "FROM directory.entity_type "
                "WHERE lower(name) = lower(%s)"
            )

            args = (name,)

            cursor.execute(sql, args)

            if cursor.rowcount > 0:
                return EntityType(*cursor.fetchone())

        return f

    @staticmethod
    def from_name(name: str) -> Callable[[Any], Optional["EntityType"]]:
        """
        Return new or existing entity type with name `name`.
        """
        def f(cursor):
            args = (name, )

            cursor.callproc("directory.name_to_entity_type", args)

            if cursor.rowcount > 0:
                return EntityType(*cursor.fetchone())

        return f
