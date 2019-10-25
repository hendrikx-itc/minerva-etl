class Entity:
    """
    All data within the Minerva platform is linked to entities. Entities are
    very minimal objects with only very generic properties such as name,
    parent, type and a few more.
    """
    def __init__(self, id_, created, name, entity_type_id):
        self.id = id_
        self.created = created
        self.name = name
        self.entity_type_id = entity_type_id

    def __repr__(self):
        return "<Entity('{0:s}')>".format(self.name)

    def __str__(self):
        return self.name

    @staticmethod
    def get(entity_id):
        """Return entity with specified distinguished name"""
        def f(cursor):
            cursor.callproc("directory.get_entity_by_id", (entity_id,))

            if cursor.rowcount == 1:
                return Entity(*cursor.fetchone())

        return f
