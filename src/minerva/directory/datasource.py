class DataSource:
    """
    A DataSource describes where a certain set of data comes from.
    """
    def __init__(self, id, name, description):
        self.id = id
        self.name = name
        self.description = description

    def __str__(self):
        return self.name

    @staticmethod
    def create(name, description):
        """
        Create new data source
        :param name: identifying name of data source.
        :param description: A short description.
        """
        def f(cursor):
            query = (
                "INSERT INTO directory.data_source "
                "(id, name, description) "
                "VALUES (DEFAULT, %s, %s) RETURNING *"
            )

            args = name, description

            cursor.execute(query, args)

            return DataSource(*cursor.fetchone())

        return f

    @staticmethod
    def get(data_source_id):
        def f(cursor):
            """Return the data source with the specified Id."""
            query = (
                "SELECT id, name, description "
                "FROM directory.data_source "
                "WHERE id = %s"
            )

            args = (data_source_id,)

            cursor.execute(query, args)

            if cursor.rowcount > 0:
                return DataSource(*cursor.fetchone())

        return f

    @staticmethod
    def get_by_name(name):
        def f(cursor):
            """Return the data source with the specified name."""
            query = (
                "SELECT id, name, description "
                "FROM directory.data_source "
                "WHERE lower(name) = lower(%s)"
            )

            args = (name,)

            cursor.execute(query, args)

            if cursor.rowcount > 0:
                return DataSource(*cursor.fetchone())

        return f

    @staticmethod
    def from_name(name):
        def f(cursor):
            """Return new or existing data source with name `name`."""
            cursor.callproc("directory.name_to_data_source", (name,))

            if cursor.rowcount > 0:
                return DataSource(*cursor.fetchone())

        return f
