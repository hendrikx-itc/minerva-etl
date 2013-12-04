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

	PERFORM attribute_directory.create_staging_table($1);

	PERFORM attribute_directory.create_hash_triggers($1);

	PERFORM attribute_directory.create_modified_trigger_function($1);
	PERFORM attribute_directory.create_modified_triggers($1);

	PERFORM attribute_directory.create_changes_view($1);

	PERFORM attribute_directory.create_run_length_view($1);

    PERFORM attribute_directory.create_curr_ptr_table($1);

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

COMMENT ON FUNCTION upgrade_curr_view(attribute_directory.attributestore) IS
'Function only for the purpose of upgrading the attribute view to use a new
materialization mechanism. Should soon be removed.';


CREATE OR REPLACE FUNCTION create_dependees(attribute_directory.attributestore)
	RETURNS attribute_directory.attributestore
AS $$
	SELECT
		attribute_directory.create_curr_view(
			attribute_directory.create_curr_ptr_view(
				attribute_directory.create_staging_modified_view(
					attribute_directory.create_staging_new_view(
						attribute_directory.create_hash_function($1)
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
						attribute_directory.drop_curr_view($1)
					)
				)
			)
		);
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION materialize_curr_ptr(attribute_directory.attributestore)
    RETURNS integer
AS $$
DECLARE
	table_name name;
	view_name name;
    row_count integer;
BEGIN
	table_name = attribute_directory.to_table_name($1) || '_curr_ptr';
	view_name = attribute_directory.to_table_name($1) || '_curr_selection';

    EXECUTE format('TRUNCATE attribute_history.%I', table_name);

    EXECUTE format('INSERT INTO attribute_history.%I (entity_id, timestamp)
SELECT entity_id, timestamp FROM attribute_history.%I', table_name, view_name);

    GET DIAGNOSTICS row_count = ROW_COUNT;

	PERFORM attribute_directory.mark_curr_materialized(attributestore_id, modified)
	FROM attribute_directory.attributestore_modified
	WHERE attributestore_id = $1.id;

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


CREATE OR REPLACE FUNCTION create_curr_ptr_table(attribute_directory.attributestore)
	RETURNS attribute_directory.attributestore
AS $$
DECLARE
	table_name name;
	curr_ptr_table_name name;
BEGIN
	table_name = attribute_directory.to_table_name($1);
    curr_ptr_table_name = table_name || '_curr_ptr';

    EXECUTE format('CREATE TABLE attribute_history.%I (
entity_id integer NOT NULL,
timestamp timestamp with time zone NOT NULL,
PRIMARY KEY (entity_id, timestamp))', curr_ptr_table_name);

    EXECUTE format('ALTER TABLE ONLY attribute_history.%I
ADD CONSTRAINT %I
FOREIGN KEY (entity_id, timestamp) REFERENCES attribute_history.%I(entity_id, timestamp)
ON DELETE CASCADE;', curr_ptr_table_name, curr_ptr_table_name || '_fk', table_name);

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
	table_name name;
	view_name name;
	view_sql text;
BEGIN
	table_name = attribute_directory.to_table_name($1);
	view_name = table_name || '_curr_selection';

	view_sql = format('
		SELECT DISTINCT ON (entity_id)
		(
			SELECT min(timestamp)
			FROM attribute_history.%I min_query
			WHERE min_query.entity_id = master.entity_id
			AND timestamp > coalesce((
				SELECT max(timestamp)
				FROM attribute_history.%I this
				WHERE this.entity_id = master.entity_id
				AND hash != master.hash
				GROUP BY this.entity_id
			), ''0001-01-01'')
			GROUP BY min_query.entity_id
		) "timestamp", entity_id

		FROM attribute_history.%I master
		ORDER BY master.entity_id desc, master.timestamp desc',
        table_name, table_name, table_name
    );

	EXECUTE format('CREATE VIEW attribute_history.%I AS %s', view_name, view_sql);

	EXECUTE format('ALTER TABLE attribute_history.%I
		OWNER TO minerva_writer', view_name);

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
		array_agg(format('last(s.%I) AS %I', name, name)) INTO column_expressions
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


-- Modified log functions

CREATE OR REPLACE FUNCTION mark_modified(attributestore_id integer)
	RETURNS attribute_directory.attributestore_modified
AS $$
	SELECT attribute_directory.mark_modified($1, now());
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

COMMENT ON FUNCTION create_run_length_view(attribute_directory.attributestore) IS
'Remove all subsequent records with duplicate attribute values and update the modified of the first';

CREATE OR REPLACE FUNCTION init(attribute_directory.attribute)
	RETURNS attribute_directory.attribute
AS $$
DECLARE
	table_name name;
	tmp_attributestore attribute_directory.attributestore;
BEGIN
	SELECT * INTO tmp_attributestore
    FROM attribute_directory.attributestore WHERE id = $1.attributestore_id;

	PERFORM attribute_directory.drop_dependees(tmp_attributestore);

	SELECT attribute_directory.to_char(attributestore) INTO table_name
    FROM attribute_directory.attributestore WHERE id = $1.attributestore_id;

	EXECUTE format('ALTER TABLE attribute_base.%I ADD COLUMN %I %s', table_name, $1.name, $1.datatype);

	PERFORM attribute_directory.create_dependees(tmp_attributestore);

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


CREATE OR REPLACE FUNCTION create_attributestore(datasource_id integer, entitytype_id integer)
	RETURNS attribute_directory.attributestore
AS $$
	INSERT INTO attribute_directory.attributestore(datasource_id, entitytype_id)
    VALUES ($1, $2) RETURNING attributestore;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION to_attributestore(datasource_id integer, entitytype_id integer)
	RETURNS attribute_directory.attributestore
AS $$
	SELECT COALESCE(
        attribute_directory.get_attributestore($1, $2),
        attribute_directory.init(attribute_directory.create_attributestore($1, $2))
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
