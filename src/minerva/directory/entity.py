import datetime
import pytz


class Entity(object):
    """
    All data within the Minerva platform is linked to entities. Entities are
    very minimal objects with only very generic properties such as name,
    parent, type and a few more.
    """
    def __init__(self, id, name, entitytype_id, dn, parent_id):
        self.id = id
        self.first_appearance = pytz.utc.localize(datetime.datetime.utcnow())
        self.name = name
        self.entitytype_id = entitytype_id
        self.dn = dn
        self.parent_id = parent_id

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
        cursor.callproc("directory.create_entity", (dn,))

        row = cursor.fetchone()

        id, _first_appearance, name, entitytype_id, dn, parent_id = row

        return Entity(id, name, entitytype_id, dn, parent_id)

    @staticmethod
    def get(cursor, entity_id):
        """Return entity with specified distinguished name."""
        args = (entity_id,)

        cursor.callproc("directory.getentitybyid", args)

        if cursor.rowcount == 1:
            (dn, entitytype_id, entity_name, parent_id) = cursor.fetchone()

            return Entity(entity_id, entity_name, entitytype_id, dn, parent_id)

    @staticmethod
    def get_by_dn(cursor, dn):
        """Return entity with specified distinguished name."""
        args = (dn,)

        cursor.callproc("directory.getentitybydn", args)

        if cursor.rowcount == 1:
            (entity_id, entitytype_id, entity_name, parent_id) = cursor.fetchone()

            return Entity(entity_id, entity_name, entitytype_id, dn, parent_id)

    @staticmethod
    def from_dn(cursor, dn):
        cursor.callproc("directory.dn_to_entity", (dn,))

        row = cursor.fetchone()

        id, first_appearance, name, entitytype_id, _dn, parent_id = row

        return Entity(id, name, entitytype_id, dn, parent_id)