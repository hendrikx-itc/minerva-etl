import re
from datetime import datetime
from contextlib import closing

import psycopg2

from minerva.db.postgresql import grant, column_exists
from minerva.storage.generic import create_column, RecoverableError, \
    NonRecoverableError, NoOpFix, create_full_table_name

from generic import do_nothing

def create_parent_notification_table(conn, datasource, attributes):
    """
    Create parent notification table.
    Table name:
    Assumption: number of attributes is static, i.e. doesn't change for a notification type
    """


def check_columns_exist(conn, schema, table, column_names):
    for column_name in column_names:
        create_column(conn, schema, table, column_name, "smallint")




def create_parent_data_table(conn, datasource_name, attributes, data_types):
    """
    Create parent data table. Actual data is stored in inherited tables.
    """
    columns_part = "".join(
        ["\"{0}\" {1}, ".format(name, type) for
            (name, type) in zip(attributes, data_types)])

    query = ("CREATE TABLE {0}.\"{1}\" ("
        "id integer NOT NULL, "
        "\"timestamp\" timestamp with time zone NOT NULL, "
        "\"modified\" timestamp with time zone NOT NULL, "
        "entity_id integer NOT NULL, "
        "{2}"
        ")".format(SCHEMA, datasource_name, columns_part))


    # Add PROCEDURE for inserting data in right child table

def create_data_table(conn, table_name, column_names, data_types):
    """
    :param conn: psycopg2 database connection
    :param schema: name of the database schema to create the table in
    :param table_name: name of table to be created
    """
    columns_part = "".join(
        ["\"{0}\" {1}, ".format(name, type) for
            (name, type) in zip(column_names, data_types)])

    full_table_name = create_full_table_name(schema, table_name)

    query = (
        "CREATE TABLE {0} ( "
        "entity_id integer NOT NULL, "
        '"timestamp" timestamp with time zone NOT NULL, '
        '"modified" timestamp with time zone NOT NULL, '
        "{1}"
        'CONSTRAINT "{2}_pkey" PRIMARY KEY (entity_id, "timestamp"))'.format(
            full_table_name, columns_part, table_name))

    alter_query = (
        "ALTER TABLE {0} ALTER COLUMN modified "
        "SET DEFAULT CURRENT_TIMESTAMP".format(full_table_name))

    index_query_modified = (
        'CREATE INDEX "idx_{0}_modified" ON {1} '
        'USING btree (modified)'.format(table_name, full_table_name))

    index_query_timestamp = (
        'CREATE INDEX "idx_{0}_timestamp" ON {1} '
        'USING btree (timestamp)'.format(table_name, full_table_name))

    trigger_query = (
        "CREATE TRIGGER update_modified_modtime "
        "BEFORE UPDATE "
        "ON {0} FOR EACH ROW EXECUTE PROCEDURE "
        "directory.update_modified_column()".format(full_table_name))

    owner_query = "ALTER TABLE {} OWNER TO minerva_writer".format(full_table_name)

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query)
            cursor.execute(alter_query)
            cursor.execute(index_query_modified)
            cursor.execute(index_query_timestamp)
            cursor.execute(trigger_query)
            cursor.execute(owner_query)
        except psycopg2.IntegrityError as exc:
            raise RecoverableError(str(exc), NoOpFix)
        except psycopg2.ProgrammingError as exc:
            if exc.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
                raise RecoverableError(str(exc), NoOpFix)
            else:
                raise NonRecoverableError(\
                    "ProgrammingError({0}): {1}".format(exc.pgcode, exc.pgerror))
        else:
            grant(conn, "TABLE", "SELECT", full_table_name, "minerva")
            conn.commit()


def create_temp_table_from(conn, schema, table):
    """
    Create a temporary table that inherits from `table` and return the temporary
    table name.
    """
    tmp_table_name = "tmp_{0}".format(table)
    full_table_name = create_full_table_name(schema, table)

    query = """CREATE TEMPORARY TABLE \"{0}\" (LIKE {1})
ON COMMIT DROP""".format(tmp_table_name, full_table_name)

    query_drop_modified_column = \
        "ALTER TABLE \"{0}\" DROP COLUMN modified".format(tmp_table_name)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)

        try:
            cursor.execute(query_drop_modified_column)
        except psycopg2.ProgrammingError as exc:
            if exc.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                # Might happen after database connection loss
                raise RecoverableError(str(exc), do_nothing)
            else:
                raise NonRecoverableError("{0}, {1!s} in query '{2}'".format(
                    exc.pgcode, exc, query_drop_modified_column))

    return tmp_table_name
