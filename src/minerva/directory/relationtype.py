from io import StringIO
from contextlib import closing
from functools import partial

from minerva.db.util import is_unique


class RelationType:
    class Descriptor:
        def __init__(self, name, cardinality):
            self.name = name
            self.cardinality = cardinality
            
    def __init__(self, id_, name, cardinality='one-to-one'):
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

    def add_relations(self, conn, relations):
        _f = StringIO()

        for source_id, target_id in relations:
            _f.write("{0}\t{1}\t{2}\n".format(
                source_id, target_id, self.id
            ))

        _f.seek(0)

        tmp_table = "tmp_relation"

        with closing(conn.cursor()) as cursor:

            query = (
                "CREATE TEMPORARY TABLE \"{0}\" "
                "(LIKE relation.all)".format(tmp_table)
            )

            cursor.execute(query)

            query = (
                "COPY \"{0}\" (source_id, target_id, type_id) "
                "FROM STDIN".format(tmp_table))

            cursor.copy_expert(query, _f)

            query = (
                "INSERT INTO relation.\"{0}\" (source_id, target_id, type_id) "
                "SELECT tmp.source_id, tmp.target_id, tmp.type_id "
                "FROM \"{1}\" tmp "
                "LEFT JOIN relation.\"{0}\" r "
                "ON r.source_id = tmp.source_id "
                "AND r.target_id = tmp.target_id "
                "WHERE r.source_id IS NULL ".format(self.name, tmp_table)
            )

            cursor.execute(query)

            cursor.execute("DROP TABLE {0}".format(tmp_table))

    def create_relation_type(conn, name, cardinality=None):
        if cardinality is not None:
            query = (
                "INSERT INTO relation.type (name, cardinality) "
                "VALUES (%s, %s) "
                "RETURNING id"
            )
            args = (name, cardinality)
        else:
            query = "INSERT INTO relation.type (name) VALUES (%s) RETURNING id"
            args = (name,)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, args)

            relationtype_id, = cursor.fetchone()

        return relationtype_id

    def truncate(self, conn):
        query = "TRUNCATE TABLE relation.\"{}\"".format(self.name)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

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
