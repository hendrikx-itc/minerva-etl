from contextlib import closing

from minerva.db.util import create_copy_from_file, create_copy_from_query, \
    exec_sql, create_unique_index

TMP_TABLE_NAME = "tmp_existence"


class Existence(object):
    def __init__(self, conn):
        self.conn = conn
        self.existences = []

    def mark_existing(self, dns):
        self.existences.extend([(dn,) for dn in dns])

    def flush(self, timestamp):
        if len(self.existences) > 0:
            dn_temp_table = "tmp_dn_timestamp"
            columns = ["dn character varying NOT NULL"]
            column_names = ["dn"]

            create_temp_table(self.conn, dn_temp_table, columns)
            with closing(self.conn.cursor()) as cursor:
                cursor.copy_expert(
                    create_copy_from_query(dn_temp_table, column_names),
                    create_copy_from_file(self.existences, ("s",)) )

            create_existence_temp_table(self.conn, TMP_TABLE_NAME)

            mark_existing_sql = (
                "INSERT INTO {} (entity_id, entitytype_id, exists) "
                "( SELECT e.id, e.entitytype_id, True FROM {} dns JOIN directory.entity e ON dns.dn = e.dn )".format(TMP_TABLE_NAME, dn_temp_table))
            exec_sql(self.conn, mark_existing_sql, (timestamp))

            update_existing(self.conn, timestamp)

            self.existences = []


def create_existence_temp_table(conn, name):
    create_temp_table(conn, name,
        ["entity_id integer NOT NULL", "entitytype_id integer NOT NULL", "exists boolean NOT NULL"])
    create_unique_index(conn, name, ["entity_id"])


def mark_entities_existing(conn, tmp_table, timestamp, entities):
    columns = ["entity_id", "entitytype_id", "timestamp"]
    copy_from_query = create_copy_from_query(tmp_table, columns)
    copy_from_file = create_entity_copy_from_file(timestamp, entities)

    with closing(conn.cursor()) as cursor:
        cursor.copy_expert(copy_from_query, copy_from_file)


def create_entity_copy_from_file(timestamp, entities):
    formats = ("d", "d", "%Y-%m-%d %H:%M:%S")
    tuples = ((entity.id, entity.entitytype_id, timestamp) for entity in entities)

    return create_copy_from_file(tuples, formats)


def update_existing(conn, timestamp):
    """
    1) Copy records from existence (With the same entitytype) which are not in tmp_table_new and mark them exists=False

    """
    #tmp_table_intermediate = "tmp_intermediate"

    get_entitytype_ids = "SELECT entitytype_id FROM {} tmp GROUP BY entitytype_id"

    copy_old_to_tmp_query = """
INSERT INTO {} (entity_id, entitytype_id, exists) (
    SELECT e.entity_id, e.entitytype_id, False as Exists
    FROM directory.existence e
    LEFT JOIN {} tmp on tmp.entity_id = e.entity_id
    WHERE tmp.entity_id is null
      AND directory.get_existence('{}', e.entity_id) is True
      AND e.entitytype_id in ({})
    GROUP BY e.entity_id, e.entitytype_id
)"""

    copy_old_to_existence = """
INSERT INTO directory.existence (entity_id, entitytype_id, timestamp, exists) (
    SELECT tmp.entity_id , tmp.entitytype_id, '{}' as timestamp, tmp.exists
    FROM {} tmp
    WHERE directory.get_existence('{}', tmp.entity_id) is not True OR tmp.exists is False
)"""

    with closing(conn.cursor()) as cursor:
        cursor.execute(get_entitytype_ids.format(TMP_TABLE_NAME))
        entitytype_ids = ",".join(map(str, [entitytype_id for entitytype_id, in cursor.fetchall()]))

        cursor.execute(copy_old_to_tmp_query.format(TMP_TABLE_NAME, TMP_TABLE_NAME, timestamp, entitytype_ids))
        cursor.execute(copy_old_to_existence.format(timestamp, TMP_TABLE_NAME, timestamp, timestamp))

    conn.commit()


def create_temp_table(conn, name, columns):
    columns_part = ",".join(columns)

    sql = (
        "CREATE TEMP TABLE {} ({}) "
        "ON COMMIT DROP"
    ).format(name, columns_part)

    exec_sql(conn, sql)
