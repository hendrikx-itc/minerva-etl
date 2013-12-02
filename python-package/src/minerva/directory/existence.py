from contextlib import closing

from minerva.db.util import create_copy_from_file, create_copy_from_query, \
    exec_sql, create_unique_index

TMP_TABLE_NAME = "tmp_existence"


class Existence(object):
    def __init__(self, conn):
        self.conn = conn
        self.existence = []

    def mark_existing(self, dns, timestamp):
        self.existence.extend([(dn, timestamp) for dn in dns])

    def flush(self):
        if len(self.existence) > 0:
            dn_temp_table = "tmp_dn_timestamp"

            columns = [
                "dn character varying NOT NULL",
                "timestamp timestamp with time zone NOT NULL"]

            column_names = ["dn", "timestamp"]

            create_temp_table(self.conn, dn_temp_table, columns)

            copy_from_file = create_copy_from_file(self.existence, ("s", "%Y-%m-%d %H:%M:%S"))

            copy_from_query = create_copy_from_query(dn_temp_table, column_names)

            with closing(self.conn.cursor()) as cursor:
                cursor.copy_expert(copy_from_query, copy_from_file)

            create_existence_temp_table(self.conn, TMP_TABLE_NAME)

            mark_existing_sql = (
                "INSERT INTO {} (entity_id, entitytype_id, timestamp) "
                "("
                "SELECT e.id, e.entitytype_id, dns.timestamp FROM {} dns "
                "JOIN directory.entity e ON dns.dn = e.dn"
                ")".format(TMP_TABLE_NAME, dn_temp_table))

            exec_sql(self.conn, mark_existing_sql)

            update_existing(self.conn, TMP_TABLE_NAME)

            self.existence = []


def create_existence_temp_table(conn, name):
    columns = [
        "timestamp timestamp with time zone NOT NULL",
        "entity_id integer NOT NULL",
        "entitytype_id integer NOT NULL"]

    create_temp_table(conn, name, columns)
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


def update_existing(conn, tmp_table_new):
    """
    1) Copy records now in existence_curr and not in temp table to existence
    twice. Once verbatim and once with 'exists' set to 'False' and a timestamp
    of NOW().

    2) Remove records of step 1 from existence_curr table.

    3) Add new records to existence_curr.
    """
    tmp_table_intermediate = "tmp_intermediate"

    get_entitytype_ids = (
        "SELECT entitytype_id FROM {} tmp "
        "GROUP BY entitytype_id")

    copy_old_to_tmp_query = (
        "INSERT INTO {} (entity_id, entitytype_id, timestamp) "
        "(SELECT ec.entity_id, ec.entitytype_id, ec.timestamp FROM directory.existence_curr ec "
        "LEFT JOIN {} tmp ON ec.entity_id = tmp.entity_id "
        "WHERE tmp.entity_id IS NULL AND ec.entitytype_id IN ({}))")

    remove_old_query = (
        "DELETE FROM directory.existence_curr ec "
        "WHERE ec.entity_id IN (SELECT entity_id FROM {})")

    copy_old_created_query = (
        "INSERT INTO directory.existence (entity_id, entitytype_id, timestamp, exists) "
        "(SELECT entity_id, entitytype_id, timestamp, True FROM {})")

    copy_old_destroyed_query = (
        "INSERT INTO directory.existence (entity_id, entitytype_id, timestamp, exists) "
        "(SELECT entity_id, entitytype_id, NOW(), False FROM {})")

    add_new_query = (
        "INSERT INTO directory.existence_curr (entity_id, entitytype_id, timestamp, exists) "
        "(SELECT tmp.entity_id, tmp.entitytype_id, tmp.timestamp, True FROM {0} tmp "
        "LEFT JOIN directory.existence_curr ec ON ec.entity_id = tmp.entity_id "
        "WHERE ec.entity_id IS NULL)")

    create_existence_temp_table(conn, tmp_table_intermediate)

    with closing(conn.cursor()) as cursor:
        cursor.execute(get_entitytype_ids.format(tmp_table_new))
        entitytype_ids = [entitytype_id for entitytype_id, in cursor.fetchall()]

        cursor.execute(copy_old_to_tmp_query.format(tmp_table_intermediate,
            tmp_table_new, ",".join(map(str, entitytype_ids))))
        cursor.execute(remove_old_query.format(tmp_table_intermediate))
        cursor.execute(copy_old_created_query.format(tmp_table_intermediate))
        cursor.execute(copy_old_destroyed_query.format(tmp_table_intermediate))
        cursor.execute(add_new_query.format(tmp_table_new))

    conn.commit()


def create_temp_table(conn, name, columns):
    columns_part = ",".join(columns)

    sql = (
        "CREATE TEMP TABLE {} ({}) "
        "ON COMMIT DROP"
    ).format(name, columns_part)

    exec_sql(conn, sql)
