from contextlib import closing
from functools import partial

from minerva.db.util import is_unique


class RelationType:
    class Descriptor:
        def __init__(self, name, cardinality):
            self.name = name
            self.cardinality = cardinality

    def __init__(self, id_, name, cardinality="one-to-one"):
        self.id = id_
        self.name = name
        self.cardinality = cardinality

    @staticmethod
    def create(descriptor):
        def f(cursor):
            pass

        return f

    @staticmethod
    def get_by_name(name):
        def f(cursor):
            """Return the relation type with the specified name."""
            query = (
                "SELECT id, name, cardinality "
                "FROM relation_directory.type "
                "WHERE lower(name) = lower(%s)"
            )

            args = (name,)

            cursor.execute(query, args)

            if cursor.rowcount > 0:
                return RelationType(*cursor.fetchone())

        return f

    @staticmethod
    def create_relation_type(conn, name, cardinality=None):
        if cardinality is not None:
            query = (
                "INSERT INTO relation_directory.type (name, cardinality) "
                "VALUES (%s, %s) "
                "RETURNING id"
            )
            args = (name, cardinality)
        else:
            query = (
                "INSERT INTO relation_directory.type (name) VALUES (%s) RETURNING id"
            )
            args = (name,)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, args)

            (relation_type_id,) = cursor.fetchone()

        return relation_type_id

    def is_one_to_one(self, conn):
        """
        Returns True when relation type is one to one, otherwise False.
        """
        unique = partial(is_unique, conn, "relation", self.name)
        source_unique = unique("source_id")
        target_unique = unique("target_id")

        return source_unique and target_unique

    def is_one_to_many(self, conn):
        """
        Returns True when relation type is one to many, otherwise False
        """
        return is_unique(conn, "relation", self.name, "source_id")

    def is_many_to_one(self, conn):
        """
        Returns True when relation type is many to one, otherwise False
        """
        return is_unique(conn, "relation", self.name, "target_id")
