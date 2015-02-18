class EntityType():
    def __init__(self, id, name, description):
        self.id = id
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
                "INSERT INTO directory.entitytype (id, name, description) "
                "VALUES (DEFAULT, %s, %s) "
                "RETURNING *")

            args = name, description

            cursor.execute(query, args)

            return EntityType(*cursor.fetchone())

        return f

    @staticmethod
    def get(entitytype_id):
        """Return the entity type matching the specified Id."""
        def f(cursor):
            query = (
                "SELECT id, name, description "
                "FROM directory.entitytype "
                "WHERE id = %s")

            args = (entitytype_id,)

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
                "FROM directory.entitytype "
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
        Return new or existing entitytype with name `name`.
        """
        def f(cursor):
            args = (name, )

            cursor.callproc("directory.name_to_entitytype", args)

            if cursor.rowcount > 0:
                return EntityType(*cursor.fetchone())

        return f