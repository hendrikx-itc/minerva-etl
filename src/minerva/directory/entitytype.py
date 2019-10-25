class NoSuchEntityType(Exception):
    def __init__(self, entity_type_name):
        self.entity_type_name = entity_type_name

    def __str__(self):
        return "No such entity type '{}'".format(
            self.entity_type_name
        )


class EntityType:
    def __init__(self, id_, name, description):
        self.id = id_
        self.name = name
        self.description = description

    def __repr__(self):
        return "<EntityType({0})>".format(self.name)

    def __str__(self):
        return self.name

    @staticmethod
    def create(name, description):
        """Create new entity type and add it to the database."""
        def f(cursor):
            query = (
                "INSERT INTO directory.entity_type (id, name, description) "
                "VALUES (DEFAULT, %s, %s) "
                "RETURNING *")

            args = name, description

            cursor.execute(query, args)

            return EntityType(*cursor.fetchone())

        return f

    @staticmethod
    def get(entity_type_id):
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
    def get_by_name(name):
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
    def from_name(name):
        """
        Return new or existing entity type with name `name`.
        """
        def f(cursor):
            args = (name, )

            cursor.callproc("directory.name_to_entity_type", args)

            if cursor.rowcount > 0:
                return EntityType(*cursor.fetchone())

        return f
