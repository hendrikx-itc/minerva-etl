"""Provides the DataSource class."""


class DataSource:
    """
    A DataSource describes where a certain set of data comes from.
    """
    def __init__(self, id_, name, description):
        self.id = id_
        self.name = name
        self.description = description

    def __str__(self):
        return self.name

    @staticmethod
    def create(name, description):
        """
        Create a new data source
        :param name: identifying name of data source.
        :param description: A short description.
        """
        def execute(cursor):
            query = (
                "INSERT INTO directory.data_source "
                "(id, name, description) "
                "VALUES (DEFAULT, %s, %s) RETURNING *"
            )

            args = name, description

            cursor.execute(query, args)

            return DataSource(*cursor.fetchone())

        return execute

    @staticmethod
    def get(id_):
        """Return function to get a datasource by Id."""
        def execute(cursor):
            """Return the data source with the specified Id."""
            query = (
                "SELECT id, name, description "
                "FROM directory.data_source "
                "WHERE id = %s"
            )

            args = (id_,)

            cursor.execute(query, args)

            if cursor.rowcount > 0:
                return DataSource(*cursor.fetchone())

            return None

        return execute

    @staticmethod
    def get_by_name(name):
        """Return function to get a datasource by name."""
        def execute(cursor):
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

            return None

        return execute

    @staticmethod
    def from_name(name):
        """Return function to get or create a datasource by name."""
        def execute(cursor):
            """Return new or existing data source with name `name`."""
            cursor.callproc("directory.name_to_data_source", (name,))

            if cursor.rowcount > 0:
                return DataSource(*cursor.fetchone())

            return None

        return execute
