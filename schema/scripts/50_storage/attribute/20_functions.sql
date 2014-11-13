SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = attribute_directory, pg_catalog;


CREATE OR REPLACE FUNCTION to_char(attribute_directory.attributestore)
    RETURNS text
AS $$
    SELECT datasource.name || '_' || entitytype.name
    FROM directory.datasource, directory.entitytype
    WHERE datasource.id = $1.datasource_id AND entitytype.id = $1.entitytype_id;
$$ LANGUAGE SQL STABLE STRICT;


CREATE OR REPLACE FUNCTION to_table_name(attribute_directory.attributestore)
    RETURNS name
AS $$
    SELECT (attribute_directory.to_char($1))::name;
$$ LANGUAGE SQL STABLE STRICT;


CREATE TYPE attribute_info AS (
    name name,
    data_type character varying
);


CREATE OR REPLACE FUNCTION datatype_order(datatype character varying)
    RETURNS integer
AS $$
BEGIN
    CASE datatype
        WHEN 'smallint' THEN
            RETURN 1;
        WHEN 'integer' THEN
            RETURN 2;
        WHEN 'bigint' THEN
            RETURN 3;
        WHEN 'real' THEN
            RETURN 4;
        WHEN 'double precision' THEN
            RETURN 5;
        WHEN 'numeric' THEN
            RETURN 6;
        WHEN 'timestamp without time zone' THEN
            RETURN 7;
        WHEN 'smallint[]' THEN
            RETURN 8;
        WHEN 'integer[]' THEN
            RETURN 9;
        WHEN 'text[]' THEN
            RETURN 10;
        WHEN 'text' THEN
            RETURN 11;
        WHEN NULL THEN
            RETURN NULL;
        ELSE
            RAISE EXCEPTION 'Unsupported data type: %', datatype;
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION greatest_datatype(datatype_a character varying, datatype_b character varying)
    RETURNS character varying
AS $$
BEGIN
    IF trend.datatype_order(datatype_b) > trend.datatype_order(datatype_a) THEN
        RETURN datatype_b;
    ELSE
        RETURN datatype_a;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE FUNCTION init(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
BEGIN
    PERFORM attribute_directory.create_main_table($1);

    PERFORM attribute_directory.create_history_table($1);

    PERFORM attribute_directory.create_at_func_ptr($1);
    PERFORM attribute_directory.create_at_func($1);

    PERFORM attribute_directory.create_entity_at_func_ptr($1);
    PERFORM attribute_directory.create_entity_at_func($1);

    PERFORM attribute_directory.create_staging_table($1);

    PERFORM attribute_directory.create_hash_triggers($1);

    PERFORM attribute_directory.create_modified_trigger_function($1);
    PERFORM attribute_directory.create_modified_triggers($1);

    PERFORM attribute_directory.create_changes_view($1);

    PERFORM attribute_directory.create_run_length_view($1);

    PERFORM attribute_directory.create_curr_ptr_table($1);

    PERFORM attribute_directory.create_compacted_tmp_table($1);

    PERFORM attribute_directory.create_dependees($1);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION upgrade_attribute_table(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
BEGIN
    PERFORM attribute_directory.drop_curr_view($1);
    PERFORM attribute_directory.add_first_appearance_to_attribute_table($1);
    PERFORM attribute_directory.create_curr_view($1);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_dependees(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
    SELECT
        attribute_directory.create_compacted_view(
            attribute_directory.create_curr_view(
                attribute_directory.create_curr_ptr_view(
                    attribute_directory.create_staging_modified_view(
                        attribute_directory.create_staging_new_view(
                            attribute_directory.create_hash_function($1)
                        )
                    )
                )
            )
        );
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION drop_dependees(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
    SELECT
        attribute_directory.drop_hash_function(
            attribute_directory.drop_staging_new_view(
                attribute_directory.drop_staging_modified_view(
                    attribute_directory.drop_curr_ptr_view(
                        attribute_directory.drop_curr_view(
                            attribute_directory.drop_compacted_view($1)
                        )
                    )
                )
            )
        );
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION materialize_curr_ptr(attribute_directory.attributestore)
    RETURNS integer
AS $$
DECLARE
    table_name name := attribute_directory.to_table_name($1) || '_curr_ptr';
    view_name name := attribute_directory.to_table_name($1) || '_curr_selection';
    row_count integer;
BEGIN
    IF attribute_directory.requires_compacting($1) THEN
        PERFORM attribute_directory.compact($1);
    END IF;

    EXECUTE format('TRUNCATE attribute_history.%I', table_name);
    EXECUTE format(
        'INSERT INTO attribute_history.%I (entity_id, timestamp) '
        'SELECT entity_id, timestamp '
        'FROM attribute_history.%I', table_name, view_name
    );

    GET DIAGNOSTICS row_count = ROW_COUNT;

    PERFORM attribute_directory.mark_curr_materialized($1.id);

    RETURN row_count;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_changes_view(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name;
    view_name name;
    view_sql text;
BEGIN
    table_name = attribute_directory.to_table_name($1);
    view_name = table_name || '_changes';

    view_sql = format('SELECT entity_id, timestamp, COALESCE(hash <> lag(hash) OVER w, true) AS change FROM attribute_history.%I WINDOW w AS (PARTITION BY entity_id ORDER BY timestamp asc)', table_name);

    EXECUTE format('CREATE VIEW attribute_history.%I AS %s', view_name, view_sql);

    EXECUTE format('ALTER TABLE attribute_history.%I
        OWNER TO minerva_writer', view_name);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_run_length_view(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name;
    view_name name;
    view_sql text;
BEGIN
    table_name = attribute_directory.to_table_name($1);
    view_name = table_name || '_run_length';

    view_sql = format('SELECT public.first(entity_id) AS entity_id, min(timestamp) AS "start", max(timestamp) AS "end", min(first_appearance) AS first_appearance, max(modified) AS modified, count(*) AS run_length
FROM
(
    SELECT entity_id, timestamp, first_appearance, modified, sum(change) OVER w2 AS run
    FROM
    (
        SELECT entity_id, timestamp, first_appearance, modified, CASE WHEN hash <> lag(hash) OVER w THEN 1 ELSE 0 END AS change
        FROM attribute_history.%I
        WINDOW w AS (PARTITION BY entity_id ORDER BY timestamp asc)
    ) t
    WINDOW w2 AS (PARTITION BY entity_id ORDER BY timestamp ASC)
) runs
GROUP BY entity_id, run;', table_name);

    EXECUTE format('CREATE VIEW attribute_history.%I AS %s', view_name, view_sql);

    EXECUTE format('ALTER TABLE attribute_history.%I
        OWNER TO minerva_writer', view_name);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;

COMMENT ON FUNCTION create_run_length_view(attribute_directory.attributestore) IS
'Create a view on an attributestore''s history table that lists the runs of
duplicate attribute data records by their entity Id and start-end. This can
be used as a source for compacting actions.';


CREATE OR REPLACE FUNCTION drop_changes_view(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
BEGIN
    EXECUTE format('DROP VIEW attribute_history.%I', attribute_directory.to_table_name($1) || '_history_changes');

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_curr_view(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name;
    curr_ptr_table_name name;
    view_name name;
    view_sql text;
BEGIN
    table_name = attribute_directory.to_table_name($1);
    curr_ptr_table_name = table_name || '_curr_ptr';
    view_name = table_name;

    view_sql = format('SELECT h.* FROM attribute_history.%I h JOIN attribute_history.%I c ON h.entity_id = c.entity_id AND h.timestamp = c.timestamp', table_name, curr_ptr_table_name);

    EXECUTE format('CREATE VIEW attribute.%I AS %s', view_name, view_sql);

    EXECUTE format('ALTER TABLE attribute.%I
        OWNER TO minerva_writer', view_name);

    EXECUTE format('GRANT SELECT ON TABLE attribute.%I TO minerva', view_name);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION drop_curr_view(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
BEGIN
    EXECUTE format('DROP VIEW attribute.%I', attribute_directory.to_table_name($1));

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_curr_ptr_table(
    attribute_directory.attributestore,
    table_suffix name DEFAULT '_curr_ptr'
)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name;
    curr_ptr_table_name name;
BEGIN
    table_name = attribute_directory.to_table_name($1);
    curr_ptr_table_name = table_name || table_suffix;

    EXECUTE format('CREATE TABLE attribute_history.%I (
entity_id integer NOT NULL,
timestamp timestamp with time zone NOT NULL,
PRIMARY KEY (entity_id, timestamp))', curr_ptr_table_name);

    EXECUTE format('CREATE INDEX ON attribute_history.%I (entity_id, timestamp)', curr_ptr_table_name);

    EXECUTE format('ALTER TABLE attribute_history.%I
        OWNER TO minerva_writer', curr_ptr_table_name);

    EXECUTE format('GRANT SELECT ON TABLE attribute_history.%I TO minerva', curr_ptr_table_name);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_curr_ptr_view(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name := attribute_directory.to_table_name($1);
    view_name name := table_name || '_curr_selection';
    view_sql text;
BEGIN
    view_sql = format(
        'SELECT max(timestamp) AS timestamp, entity_id '
        'FROM attribute_history.%I '
        'GROUP BY entity_id',
        table_name
    );

    EXECUTE format('CREATE OR REPLACE VIEW attribute_history.%I AS %s', view_name, view_sql);

    EXECUTE format(
        'ALTER TABLE attribute_history.%I '
        'OWNER TO minerva_writer',
        view_name
    );

    EXECUTE format('GRANT SELECT ON TABLE attribute_history.%I TO minerva', view_name);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION drop_curr_ptr_view(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
BEGIN
    EXECUTE format('DROP VIEW attribute_history.%I', attribute_directory.to_table_name($1) || '_curr_selection');

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_main_table(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name;
    default_columns text[];
    columns_part text;
BEGIN
    table_name = attribute_directory.to_table_name($1);

    default_columns = ARRAY[
        'entity_id integer NOT NULL',
        '"timestamp" timestamp with time zone NOT NULL'];

    SELECT array_to_string(default_columns || array_agg(format('%I %s', name, datatype)), ', ') INTO columns_part
    FROM attribute_directory.attribute
    WHERE attributestore_id = $1.id;

    EXECUTE format('CREATE TABLE attribute_base.%I (
    %s
    )', table_name, columns_part);

    EXECUTE format('ALTER TABLE attribute_base.%I
        OWNER TO minerva_writer', table_name);

    EXECUTE format('GRANT SELECT ON TABLE attribute_base.%I TO minerva', table_name);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION add_first_appearance_to_attribute_table(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name;
BEGIN
    table_name = attribute_directory.to_table_name($1);

    EXECUTE format('ALTER TABLE attribute_base.%I ADD COLUMN
        first_appearance timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP', table_name);

    EXECUTE format('UPDATE attribute_history.%I SET first_appearance = modified', table_name);

    EXECUTE format('CREATE INDEX ON attribute_history.%I (first_appearance)', table_name);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_history_table(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name;
BEGIN
    table_name = attribute_directory.to_table_name($1);

    EXECUTE format('CREATE TABLE attribute_history.%I (
        first_appearance timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
        modified timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
        hash character varying,
        PRIMARY KEY (entity_id, timestamp)
    ) INHERITS (attribute_base.%I)', table_name, table_name);

    EXECUTE format('CREATE INDEX ON attribute_history.%I (first_appearance)', table_name);

    EXECUTE format('CREATE INDEX ON attribute_history.%I (modified)', table_name);

    EXECUTE format('ALTER TABLE attribute_history.%I
        OWNER TO minerva_writer', table_name);

    EXECUTE format('GRANT SELECT ON TABLE attribute_history.%I TO minerva', table_name);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_staging_table(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name;
    default_columns text[];
    columns_part text;
BEGIN
    table_name = attribute_directory.to_table_name($1);

    default_columns = ARRAY[
        'entity_id integer NOT NULL',
        '"timestamp" timestamp with time zone NOT NULL'];

    SELECT array_to_string(default_columns || array_agg(format('%I %s', name, datatype)), ', ') INTO columns_part
    FROM attribute_directory.attribute
    WHERE attributestore_id = $1.id;

    EXECUTE format('CREATE UNLOGGED TABLE attribute_staging.%I (
    %s
    ) INHERITS (attribute_base.%I)', table_name, columns_part, table_name);

    EXECUTE format('CREATE INDEX ON attribute_staging.%I
        USING btree (entity_id, timestamp)', table_name);

    EXECUTE format('ALTER TABLE attribute_staging.%I
        OWNER TO minerva_writer', table_name);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_staging_new_view(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name;
    view_name name;
    column_expressions text[];
    columns_part character varying;
BEGIN
    table_name = attribute_directory.to_table_name($1);
    view_name = table_name || '_new';

    SELECT
        array_agg(format('public.last(s.%I) AS %I', name, name)) INTO column_expressions
    FROM
        public.column_names('attribute_staging', table_name) name
    WHERE name NOT in ('entity_id', 'timestamp');

    SELECT array_to_string(
        ARRAY['s.entity_id', 's.timestamp'] || column_expressions,
        ', ')
    INTO columns_part;

    EXECUTE format('CREATE VIEW attribute_staging.%I
AS SELECT %s FROM attribute_staging.%I s
LEFT JOIN attribute_history.%I a
    ON a.entity_id = s.entity_id
    AND a.timestamp = s.timestamp
WHERE a.entity_id IS NULL
GROUP BY s.entity_id, s.timestamp', view_name, columns_part, table_name, table_name);

    EXECUTE format('ALTER TABLE attribute_staging.%I
        OWNER TO minerva_writer', view_name);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION drop_staging_new_view(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
BEGIN
    EXECUTE format('DROP VIEW attribute_staging.%I', attribute_directory.to_table_name($1) || '_new');

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_staging_modified_view(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name;
    staging_table_name name;
    view_name name;
BEGIN
    table_name = attribute_directory.to_table_name($1);
    view_name = table_name || '_modified';

    EXECUTE format('CREATE VIEW attribute_staging.%I
AS SELECT s.* FROM attribute_staging.%I s
JOIN attribute_history.%I a ON a.entity_id = s.entity_id AND a.timestamp = s.timestamp', view_name, table_name, table_name);

    EXECUTE format('ALTER TABLE attribute_staging.%I
        OWNER TO minerva_writer', view_name);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION drop_staging_modified_view(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
BEGIN
    EXECUTE format('DROP VIEW attribute_staging.%I', attribute_directory.to_table_name($1) || '_modified');

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_hash_triggers(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name;
BEGIN
    table_name = attribute_directory.to_table_name($1);

    EXECUTE format('CREATE TRIGGER set_hash_on_update
        BEFORE UPDATE ON attribute_history.%I
        FOR EACH ROW EXECUTE PROCEDURE attribute_directory.set_hash()', table_name);

    EXECUTE format('CREATE TRIGGER set_hash_on_insert
        BEFORE INSERT ON attribute_history.%I
        FOR EACH ROW EXECUTE PROCEDURE attribute_directory.set_hash()', table_name);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


-- Curr materialization log functions

CREATE OR REPLACE FUNCTION update_curr_materialized(attributestore_id integer, materialized timestamp with time zone)
    RETURNS attribute_directory.attributestore_curr_materialized
AS $$
    UPDATE attribute_directory.attributestore_curr_materialized
    SET materialized = greatest(materialized, $2)
    WHERE attributestore_id = $1
    RETURNING attributestore_curr_materialized;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION store_curr_materialized(attributestore_id integer, materialized timestamp with time zone)
    RETURNS attribute_directory.attributestore_curr_materialized
AS $$
    INSERT INTO attribute_directory.attributestore_curr_materialized (attributestore_id, materialized)
    VALUES ($1, $2)
    RETURNING attributestore_curr_materialized;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION mark_curr_materialized(attributestore_id integer, materialized timestamp with time zone)
    RETURNS attribute_directory.attributestore_curr_materialized
AS $$
    SELECT COALESCE(attribute_directory.update_curr_materialized($1, $2), attribute_directory.store_curr_materialized($1, $2));
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION mark_curr_materialized(attributestore_id integer)
    RETURNS attribute_directory.attributestore_curr_materialized
AS $$
    SELECT attribute_directory.mark_curr_materialized(attributestore_id, modified)
    FROM attribute_directory.attributestore_modified
    WHERE attributestore_id = $1;
$$ LANGUAGE SQL VOLATILE;


-- Compacting log functions

CREATE OR REPLACE FUNCTION update_compacted(attributestore_id integer, compacted timestamp with time zone)
    RETURNS attribute_directory.attributestore_compacted
AS $$
    UPDATE attribute_directory.attributestore_compacted
    SET compacted = greatest(compacted, $2)
    WHERE attributestore_id = $1
    RETURNING attributestore_compacted;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION store_compacted(attributestore_id integer, compacted timestamp with time zone)
    RETURNS attribute_directory.attributestore_compacted
AS $$
    INSERT INTO attribute_directory.attributestore_compacted (attributestore_id, compacted)
    VALUES ($1, $2)
    RETURNING attributestore_compacted;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION mark_compacted(attributestore_id integer, compacted timestamp with time zone)
    RETURNS attribute_directory.attributestore_compacted
AS $$
    SELECT COALESCE(attribute_directory.update_compacted($1, $2), attribute_directory.store_compacted($1, $2));
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION mark_compacted(attributestore_id integer)
    RETURNS attribute_directory.attributestore_compacted
AS $$
    SELECT attribute_directory.mark_compacted(attributestore_id, modified)
    FROM attribute_directory.attributestore_modified
    WHERE attributestore_id = $1;
$$ LANGUAGE SQL VOLATILE;


-- Modified log functions

CREATE OR REPLACE FUNCTION mark_modified(attributestore_id integer)
    RETURNS attribute_directory.attributestore_modified
AS $$
    SELECT CASE
        WHEN current_setting('minerva.trigger_mark_modified') = 'off' THEN
            (SELECT asm FROM attribute_directory.attributestore_modified asm WHERE asm.attributestore_id = $1)

        ELSE
            attribute_directory.mark_modified($1, now())

        END;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION mark_modified(attributestore_id integer, modified timestamp with time zone)
    RETURNS attribute_directory.attributestore_modified
AS $$
    SELECT COALESCE(attribute_directory.update_modified($1, $2), attribute_directory.store_modified($1, $2));
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION update_modified(attributestore_id integer, modified timestamp with time zone)
    RETURNS attribute_directory.attributestore_modified
AS $$
    UPDATE attribute_directory.attributestore_modified
    SET modified = greatest(modified, $2)
    WHERE attributestore_id = $1
    RETURNING attributestore_modified;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION store_modified(attributestore_id integer, modified timestamp with time zone)
    RETURNS attribute_directory.attributestore_modified
AS $$
    INSERT INTO attribute_directory.attributestore_modified (attributestore_id, modified)
    VALUES ($1, $2)
    RETURNING attributestore_modified;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION create_modified_trigger_function(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $function$
DECLARE
    table_name name;
BEGIN
    table_name = attribute_directory.to_table_name($1);

    EXECUTE format('CREATE FUNCTION attribute_history.mark_modified_%s()
RETURNS TRIGGER
AS $$
BEGIN
    PERFORM attribute_directory.mark_modified(%s);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql', $1.id, $1.id);

    EXECUTE format('ALTER FUNCTION attribute_history.mark_modified_%s()
        OWNER TO minerva_writer', $1.id);

    RETURN $1;
END;
$function$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_modified_triggers(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name;
BEGIN
    table_name = attribute_directory.to_table_name($1);

    EXECUTE format('CREATE TRIGGER mark_modified_on_update
        AFTER UPDATE ON attribute_history.%I
        FOR EACH STATEMENT EXECUTE PROCEDURE attribute_history.mark_modified_%s()', table_name, $1.id);

    EXECUTE format('CREATE TRIGGER mark_modified_on_insert
        AFTER INSERT ON attribute_history.%I
        FOR EACH STATEMENT EXECUTE PROCEDURE attribute_history.mark_modified_%s()', table_name, $1.id);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION render_hash_query(attribute_directory.attributestore)
    RETURNS text
AS $$
    SELECT COALESCE(
        'SELECT md5(' ||
        array_to_string(array_agg(format('COALESCE(($1.%I)::text, '''')', name)), ' || ') ||
        ')',
        'SELECT ''''::text')
    FROM attribute_directory.attribute
    WHERE attributestore_id = $1.id;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION drop_hash_function(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
BEGIN
    EXECUTE format('DROP FUNCTION attribute_history.values_hash(attribute_history.%I)', attribute_directory.to_table_name($1));

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_hash_function(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $function$
BEGIN
    EXECUTE format('CREATE FUNCTION attribute_history.values_hash(attribute_history.%I)
RETURNS text
AS $$
    %s
$$ LANGUAGE SQL STABLE', attribute_directory.to_table_name($1), attribute_directory.render_hash_query($1));

    EXECUTE format('ALTER FUNCTION attribute_history.values_hash(attribute_history.%I)
        OWNER TO minerva_writer', attribute_directory.to_table_name($1));

    RETURN $1;
END;
$function$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION compact(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name;
    run_length_view_name name;
    r record;
BEGIN
    table_name = attribute_directory.to_table_name($1);
    run_length_view_name = table_name || '_run_length';

    FOR r IN EXECUTE format('SELECT entity_id, start, "end", first_appearance, modified FROM attribute_history.%I WHERE run_length > 1', run_length_view_name)
    LOOP
        EXECUTE format('DELETE FROM attribute_history.%I WHERE entity_id = $1 AND timestamp > $2 AND timestamp <= $3', table_name)
        USING r.entity_id, r."start", r."end";

        EXECUTE format('UPDATE attribute_history.%I SET modified = $1, first_appearance = $4 WHERE entity_id = $2 AND timestamp = $3', table_name)
        USING r.modified, r.entity_id, r."start", r.first_appearance;
    END LOOP;

    PERFORM attribute_directory.mark_compacted(attributestore_id, modified)
    FROM attribute_directory.attributestore_modified
    WHERE attributestore_id = $1.id;

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;

COMMENT ON FUNCTION compact(attribute_directory.attributestore) IS
'Remove all subsequent records with duplicate attribute values and update the modified of the first';


CREATE OR REPLACE FUNCTION init(attribute_directory.attribute)
    RETURNS attribute_directory.attribute
AS $$
DECLARE
    table_name name;
BEGIN
    SELECT attribute_directory.to_char(attributestore) INTO table_name
    FROM attribute_directory.attributestore WHERE id = $1.attributestore_id;

    PERFORM dep_recurse.alter(
        dep_recurse.table_ref('attribute_base', table_name),
        ARRAY[
            format('ALTER TABLE attribute_base.%I ADD COLUMN %I %s', table_name, $1.name, $1.datatype)
        ]
    );

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION get_attributestore(datasource_id integer, entitytype_id integer)
    RETURNS attribute_directory.attributestore
AS $$
    SELECT attributestore
    FROM attribute_directory.attributestore
    WHERE datasource_id = $1 AND entitytype_id = $2;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION define_attributestore(datasource_id integer, entitytype_id integer)
    RETURNS attribute_directory.attributestore
AS $$
    INSERT INTO attribute_directory.attributestore(datasource_id, entitytype_id)
    VALUES ($1, $2) RETURNING attributestore;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION define_attributestore(datasource_name text, entitytype_name text)
    RETURNS attribute_directory.attributestore
AS $$
    INSERT INTO attribute_directory.attributestore(datasource_id, entitytype_id)
    VALUES ((directory.name_to_datasource($1)).id, (directory.name_to_entitytype($2)).id)
    RETURNING attributestore;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION to_attributestore(datasource_id integer, entitytype_id integer)
    RETURNS attribute_directory.attributestore
AS $$
    SELECT COALESCE(
        attribute_directory.get_attributestore($1, $2),
        attribute_directory.init(attribute_directory.define_attributestore($1, $2))
    );
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION create_attributestore(datasource_name text, entitytype_name text)
    RETURNS attribute_directory.attributestore
AS $$
    SELECT attribute_directory.init(attribute_directory.define_attributestore($1, $2));
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION add_attributes(attributestore, attributes attribute_descr[])
    RETURNS attribute_directory.attributestore
AS $$
BEGIN
    INSERT INTO attribute_directory.attribute(attributestore_id, name, datatype, description) (
        SELECT $1.id, name, datatype, description
	FROM unnest($2) atts
    );

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_attributestore(datasource_name text, entitytype_name text, attributes attribute_descr[])
    RETURNS attribute_directory.attributestore
AS $$
    SELECT attribute_directory.init(
	attribute_directory.add_attributes(attribute_directory.define_attributestore($1, $2), $3)
    );
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION get_attribute(attributestore, name)
    RETURNS attribute_directory.attribute
AS $$
    SELECT attribute
    FROM attribute_directory.attribute
    WHERE attributestore_id = $1.id AND name = $2;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION define(attribute_directory.attribute)
    RETURNS attribute_directory.attribute
AS $$
    INSERT INTO attribute_directory.attribute(attributestore_id, description, name, datatype)
    VALUES ($1.attributestore_id, $1.description, $1.name, $1.datatype)
    RETURNING attribute;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION to_attribute(attribute_directory.attribute)
    RETURNS attribute_directory.attribute
AS $$
    SELECT COALESCE(
        attribute_directory.get_attribute(attributestore, $1.name),
        attribute_directory.init(attribute_directory.define($1))
    )
    FROM attribute_directory.attributestore WHERE id = $1.attributestore_id;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION check_attributes_exist(attribute_directory.attribute[])
    RETURNS SETOF attribute_directory.attribute
AS $$
    SELECT attribute_directory.to_attribute(n)
    FROM unnest($1) n
    LEFT JOIN attribute_directory.attribute
    ON attribute.attributestore_id = n.attributestore_id AND n.name = attribute.name
    WHERE attribute.name IS NULL;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION check_datatype(attribute_directory.attribute)
    RETURNS attribute_directory.attribute
AS $$
    SELECT attribute_directory.to_table_name(attributestore)
    FROM attribute_directory.attributestore
    WHERE id = $1.attributestore_id;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION check_attribute_types(attribute_directory.attribute[])
    RETURNS SETOF attribute_directory.attribute
AS $$
    UPDATE attribute_directory.attribute SET datatype = n.datatype
    FROM unnest($1) n
    WHERE attribute.name = n.name
    AND attribute.attributestore_id = n.attributestore_id
    AND attribute.datatype <> attribute_directory.greatest_datatype(n.datatype, attribute.datatype)
    RETURNING attribute.*;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION modify_column_type(table_name name, column_name name, datatype varchar)
    RETURNS void
AS $$
BEGIN
    EXECUTE format('ALTER TABLE attribute_base.%I ALTER %I TYPE %s USING CAST(%I AS %s)', table_name, column_name, datatype, column_name, datatype);
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION modify_column_type(attribute_directory.attributestore, column_name name, datatype varchar)
    RETURNS attribute_directory.attributestore
AS $$
BEGIN
    PERFORM attribute_directory.modify_column_type(
        attribute_directory.to_table_name($1), $2, $3
    );

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION modify_datatype(attribute_directory.attribute)
    RETURNS attribute_directory.attribute
AS $$
BEGIN
    PERFORM
        attribute_directory.create_dependees(
            attribute_directory.modify_column_type(
                attribute_directory.drop_dependees(attributestore),
                $1.name,
                $1.datatype
            )
        )
    FROM attribute_directory.attributestore
    WHERE id = $1.attributestore_id;

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION transfer_staged(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name;
    columns_part text;
    set_columns_part text;
    default_columns text[];
BEGIN
    table_name = attribute_directory.to_table_name($1);

    default_columns = ARRAY[
        'entity_id',
        '"timestamp"'];

    SELECT array_to_string(default_columns || array_agg(format('%I', name)), ', ') INTO columns_part
    FROM attribute_directory.attribute
    WHERE attributestore_id = $1.id;

    EXECUTE format('INSERT INTO attribute_history.%I(%s) SELECT %s FROM attribute_staging.%I', table_name, columns_part, columns_part, table_name || '_new');

    SELECT array_to_string(array_agg(format('%I = m.%I', name, name)), ', ') INTO set_columns_part
    FROM attribute_directory.attribute
    WHERE attributestore_id = $1.id;

    EXECUTE format('UPDATE attribute_history.%I a SET modified = now(), %s FROM attribute_staging.%I m WHERE m.entity_id = a.entity_id AND m.timestamp = a.timestamp', table_name, set_columns_part, table_name || '_modified');

    EXECUTE format('TRUNCATE attribute_staging.%I', table_name);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_compacted_tmp_table(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name := attribute_directory.to_table_name($1);
    compacted_tmp_table_name name := table_name || '_compacted_tmp';
BEGIN
    EXECUTE format(
        'CREATE UNLOGGED TABLE attribute_history.%I ('
        '    "end" timestamp with time zone,'
        '    modified timestamp with time zone,'
        '    hash text'
        ') INHERITS (attribute_base.%I)',
        compacted_tmp_table_name, table_name
    );

    EXECUTE format(
        'CREATE INDEX ON attribute_history.%I '
        'USING btree (entity_id, timestamp)',
        compacted_tmp_table_name
    );

    EXECUTE format(
        'ALTER TABLE attribute_history.%I '
        'OWNER TO minerva_writer',
        compacted_tmp_table_name
    );

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION compacted_view_query(attribute_directory.attributestore)
    RETURNS text
AS $$
DECLARE
    table_name name := attribute_directory.to_table_name($1);
    default_columns text[] := ARRAY['rl.entity_id', 'rl.start AS timestamp', 'rl."end"', 'rl.modified', 'history.hash'];
    run_length_view_name name := table_name || '_run_length';
    columns_part text;
BEGIN
    SELECT array_to_string(default_columns || array_agg(quote_ident(name)), ', ') INTO columns_part
    FROM attribute_directory.attribute
    WHERE attributestore_id = $1.id;

    RETURN format(
        'SELECT %s '
        'FROM attribute_history.%I rl '
        'JOIN attribute_history.%I history ON history.entity_id = rl.entity_id AND history.timestamp = rl.start '
        'WHERE run_length > 1',
        columns_part, run_length_view_name, table_name
    );
END;
$$ LANGUAGE plpgsql STABLE;


CREATE OR REPLACE FUNCTION compacted_view_name(attribute_directory.attributestore)
    RETURNS name
AS $$
    SELECT (attribute_directory.to_table_name($1) || '_compacted')::name;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION create_compacted_view(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    view_name name := attribute_directory.compacted_view_name($1);
BEGIN
    EXECUTE format(
        'CREATE OR REPLACE VIEW attribute_history.%I AS %s',
        view_name,
        attribute_directory.compacted_view_query($1)
    );

    EXECUTE format(
        'ALTER TABLE attribute_history.%I OWNER TO minerva_writer',
        view_name
    );

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION drop_compacted_view(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
BEGIN
    EXECUTE format('DROP VIEW attribute_history.%I', attribute_directory.compacted_view_name($1));

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION requires_compacting(attributestore_id integer)
    RETURNS boolean
AS $$
    SELECT modified <> compacted OR compacted IS NULL
    FROM attribute_directory.attributestore_modified mod
    LEFT JOIN attribute_directory.attributestore_compacted cmp ON mod.attributestore_id = cmp.attributestore_id
    WHERE mod.attributestore_id = $1;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION requires_compacting(attribute_directory.attributestore)
    RETURNS boolean
AS $$
    SELECT attribute_directory.requires_compacting($1.id);
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION compact(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $$
DECLARE
    table_name name := attribute_directory.to_table_name($1);
    compacted_tmp_table_name name := table_name || '_compacted_tmp';
    compacted_view_name name := attribute_directory.compacted_view_name($1);
    default_columns text[] := ARRAY['entity_id', 'timestamp', '"end"', 'hash', 'modified'];
    attribute_columns text[];
    columns_part text;
    row_count integer;
BEGIN
    SELECT array_agg(quote_ident(name)) INTO attribute_columns
    FROM attribute_directory.attribute
    WHERE attributestore_id = $1.id;

    columns_part = array_to_string(default_columns || attribute_columns, ',');

    EXECUTE format(
        'TRUNCATE attribute_history.%I',
        compacted_tmp_table_name
    );

    EXECUTE format(
        'INSERT INTO attribute_history.%I(%s) '
        'SELECT %s FROM attribute_history.%I;',
        compacted_tmp_table_name, columns_part,
        columns_part, compacted_view_name
    );

    GET DIAGNOSTICS row_count = ROW_COUNT;

    RAISE NOTICE 'compacted % rows', row_count;

    EXECUTE format(
        'DELETE FROM attribute_history.%I history '
        'USING attribute_history.%I tmp '
        'WHERE '
        '	history.entity_id = tmp.entity_id AND '
        '	history.timestamp >= tmp.timestamp AND '
        '	history.timestamp <= tmp."end";',
        table_name, compacted_tmp_table_name
    );

    columns_part = array_to_string(
        ARRAY['entity_id', 'timestamp', 'modified', 'hash'] || attribute_columns,
        ','
    );

    EXECUTE format(
        'INSERT INTO attribute_history.%I(%s) '
        'SELECT %s '
        'FROM attribute_history.%I',
        table_name, columns_part,
        columns_part,
        compacted_tmp_table_name
    );

    PERFORM attribute_directory.mark_compacted($1.id);

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION direct_dependers(name text)
    RETURNS SETOF name
AS $$
    SELECT dependee.relname AS name
    FROM pg_depend
    JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
    JOIN pg_class as dependee ON pg_rewrite.ev_class = dependee.oid
    JOIN pg_class as dependent ON pg_depend.refobjid = dependent.oid
    JOIN pg_namespace as n ON dependent.relnamespace = n.oid
    JOIN pg_attribute ON
            pg_depend.refobjid = pg_attribute.attrelid
            AND
            pg_depend.refobjsubid = pg_attribute.attnum
    WHERE pg_attribute.attnum > 0 AND dependent.relname = $1;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION dependers(name name, level integer)
    RETURNS TABLE(name name, level integer)
AS $$
-- Stub function to be able to create a recursive one.
    SELECT $1, $2;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION dependers(name name, level integer)
    RETURNS TABLE(name name, level integer)
AS $$
    SELECT (d.dependers).* FROM (
        SELECT dependers(depender, $2 + 1)
        FROM direct_dependers($1) depender 
    ) d
    UNION ALL
    SELECT depender, $2
    FROM direct_dependers($1) depender;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION dependers(name name)
    RETURNS TABLE(name name, level integer)
AS $$
    SELECT * FROM dependers($1, 1);
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION action(attributestore, sql text)
    RETURNS attributestore
AS $$
BEGIN
    EXECUTE sql;

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION at_ptr_function_name(attributestore)
    RETURNS name
AS $$
    SELECT (attribute_directory.to_table_name($1) || '_at_ptr')::name;
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION create_at_func_ptr(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $function$
    SELECT attribute_directory.action(
        $1,
        format(
            'CREATE FUNCTION attribute_history.%I(timestamp with time zone)
RETURNS TABLE(entity_id integer, "timestamp" timestamp with time zone)
AS $$
    SELECT entity_id, max(timestamp)
    FROM
        attribute_history.%I
    WHERE timestamp <= $1
    GROUP BY entity_id;
$$ LANGUAGE SQL STABLE',
            attribute_directory.at_ptr_function_name($1),
            attribute_directory.to_table_name($1)
        )
    );

    SELECT attribute_directory.action(
        $1,
        format(
            'ALTER FUNCTION attribute_history.%I(timestamp with time zone) '
            'OWNER TO minerva_writer',
            attribute_directory.at_ptr_function_name($1)
        )
    );
$function$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION create_entity_at_func_ptr(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $function$
    SELECT attribute_directory.action(
        $1,
        format(
            'CREATE OR REPLACE FUNCTION attribute_history.%I(entity_id integer, timestamp with time zone)
RETURNS timestamp with time zone
AS $$
    SELECT max(timestamp)
    FROM
        attribute_history.%I
    WHERE timestamp <= $2 AND entity_id = $1;
$$ LANGUAGE SQL STABLE',
            attribute_directory.at_ptr_function_name($1),
            attribute_directory.to_table_name($1)
        )
    );

    SELECT attribute_directory.action(
        $1,
        format(
            'ALTER FUNCTION attribute_history.%I(entity_id integer, timestamp with time zone) '
            'OWNER TO minerva_writer',
            attribute_directory.at_ptr_function_name($1)
        )
    );
$function$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION at_function_name(attributestore)
    RETURNS name
AS $$
    SELECT (attribute_directory.to_table_name($1) || '_at')::name;
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION create_at_func(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $function$
    SELECT attribute_directory.action(
        $1,
        format(
            'CREATE OR REPLACE FUNCTION attribute_history.%I(timestamp with time zone)
    RETURNS SETOF attribute_history.%I
AS $$
SELECT a.*
FROM
    attribute_history.%I a
JOIN
    attribute_history.%I($1) at
ON at.entity_id = a.entity_id AND at.timestamp = a.timestamp;
$$ LANGUAGE SQL STABLE;',
            attribute_directory.at_function_name($1),
            attribute_directory.to_table_name($1),
            attribute_directory.to_table_name($1),
            attribute_directory.at_ptr_function_name($1)
        )
    );

    SELECT attribute_directory.action(
        $1,
        format(
            'ALTER FUNCTION attribute_history.%I(timestamp with time zone) '
            'OWNER TO minerva_writer',
            attribute_directory.at_function_name($1)
        )
    );
$function$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION create_entity_at_func(attribute_directory.attributestore)
    RETURNS attribute_directory.attributestore
AS $function$
    SELECT attribute_directory.action(
        $1,
        format(
            'CREATE OR REPLACE FUNCTION attribute_history.%I(entity_id integer, timestamp with time zone)
    RETURNS SETOF attribute_history.%I
AS $$
SELECT *
FROM
    attribute_history.%I
WHERE timestamp = attribute_history.%I($1, $2) AND entity_id = $1;
$$ LANGUAGE SQL STABLE;',
            attribute_directory.at_function_name($1),
            attribute_directory.to_table_name($1),
            attribute_directory.to_table_name($1),
            attribute_directory.at_ptr_function_name($1)
        )
    );

    SELECT attribute_directory.action(
        $1,
        format(
            'ALTER FUNCTION attribute_history.%I(entity_id integer, timestamp with time zone) '
            'OWNER TO minerva_writer',
            attribute_directory.at_function_name($1)
        )
    );
$function$ LANGUAGE SQL VOLATILE;

