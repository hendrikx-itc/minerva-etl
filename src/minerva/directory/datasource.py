import pytz


class DataSource(object):
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
    def create(name, description, timezone):
        """
        Create new datasource
        :param cursor: cursor instance used to store into the Minerva database.
        :param name: identifying name of data source.
        :param description: A short description.
        :param timezone: Timezone of data originating from data source.
        """
        def f(cursor):
            query = (
                "INSERT INTO directory.datasource "
                "(id, name, description, timezone) "
                "VALUES (DEFAULT, %s, %s, %s) RETURNING *"
            )

            args = name, description, timezone

            cursor.execute(query, args)

            return DataSource(*cursor.fetchone())

        return f

    @staticmethod
    def get(datasource_id):
        def f(cursor):
            """Return the datasource with the specified Id."""
            query = (
                "SELECT id, name, description, timezone "
                "FROM directory.datasource "
                "WHERE id=%s"
            )

            args = (datasource_id,)

            cursor.execute(query, args)

            if cursor.rowcount > 0:
                return DataSource(*cursor.fetchone())

        return f

    @staticmethod
    def get_by_name(name):
        def f(cursor):
            """Return the datasource with the specified name."""
            query = (
                "SELECT id, name, description, timezone "
                "FROM directory.datasource "
                "WHERE lower(name)=lower(%s)"
            )

            args = (name,)

            cursor.execute(query, args)

            if cursor.rowcount > 0:
                return DataSource(*cursor.fetchone())

        return f

    @staticmethod
    def from_name(name):
        def f(cursor):
            """Return new or existing datasource with name `name`."""
            cursor.callproc("directory.name_to_datasource", (name,))

            if cursor.rowcount > 0:
                return DataSource(*cursor.fetchone())

        return f