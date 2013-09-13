SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = attribute, pg_catalog;


CREATE OR REPLACE FUNCTION to_char(attribute.attributestore)
	RETURNS text
AS $$
	SELECT datasource.name || '_' || entitytype.name
	FROM directory.datasource, directory.entitytype
	WHERE datasource.id = $1.datasource_id AND entitytype.id = $1.entitytype_id;
$$ LANGUAGE SQL STABLE STRICT;


CREATE OR REPLACE FUNCTION to_table_name(attribute.attributestore)
	RETURNS name
AS $$
	SELECT (attribute.to_char($1))::name;
$$ LANGUAGE SQL STABLE STRICT;


CREATE TYPE attribute_info AS (
	name name,
	data_type character varying
);


CREATE OR REPLACE FUNCTION init(attribute.attributestore)
	RETURNS attribute.attributestore
AS $$
BEGIN
	PERFORM attribute.create_main_table($1);

	PERFORM attribute.create_staging_table($1);

	PERFORM attribute.create_changes_view($1);

	PERFORM attribute.create_hash_function($1);

	PERFORM attribute.create_hash_triggers($1);

	RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;



CREATE OR REPLACE FUNCTION create_changes_view(attribute.attributestore)
	RETURNS attribute.attributestore
AS $$
DECLARE
	table_name name;
	view_name name;
	view_sql text;
BEGIN
	table_name = attribute.to_table_name($1);
	view_name = table_name || '_changes';

	view_sql = format('SELECT entity_id, timestamp, COALESCE(hash <> lag(hash) OVER w, true) AS change FROM attribute.%I WINDOW w AS (PARTITION BY entity_id ORDER BY timestamp asc)', table_name);

	EXECUTE format('CREATE VIEW attribute.%I AS %s', view_name, view_sql);

	EXECUTE format('ALTER TABLE attribute.%I
		OWNER TO minerva_admin', view_name);

	RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_main_table(attribute.attributestore)
	RETURNS attribute.attributestore
AS $$
DECLARE
	table_name name;
	default_columns text[];
	columns_part text;
BEGIN
	table_name = attribute.to_table_name($1);

	default_columns = ARRAY[
		'entity_id integer NOT NULL',
		'"timestamp" timestamp with time zone NOT NULL',
		'"modified" timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP',
		'hash character varying'];

	SELECT array_to_string(default_columns || array_agg(format('%I %s', name, datatype)), ', ') INTO columns_part
	FROM attribute.attribute
	WHERE attributestore_id = $1.id;

	EXECUTE format('CREATE TABLE attribute.%I (
	%s,
	PRIMARY KEY (entity_id, timestamp)
	)', table_name, columns_part);

	EXECUTE format('CREATE INDEX ON attribute.%I
		USING btree (modified)', table_name);

	EXECUTE format('CREATE TRIGGER update_modified_modtime
		BEFORE UPDATE ON attribute.%I
		FOR EACH ROW EXECUTE PROCEDURE attribute.update_modified_column()', table_name);

	EXECUTE format('ALTER TABLE attribute.%I
		OWNER TO minerva_admin', table_name);

	EXECUTE format('GRANT SELECT ON TABLE attribute.%I TO minerva', table_name);

	RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_staging_table(attribute.attributestore)
	RETURNS attribute.attributestore
AS $$
DECLARE
	table_name name;
	default_columns text[];
	columns_part text;
BEGIN
	table_name = attribute.to_table_name($1) || '_staging';

	default_columns = ARRAY[
		'entity_id integer NOT NULL',
		'"timestamp" timestamp with time zone NOT NULL'];

	SELECT array_to_string(default_columns || array_agg(format('%I %s', name, datatype)), ', ') INTO columns_part
	FROM attribute.attribute
	WHERE attributestore_id = $1.id;

	EXECUTE format('CREATE UNLOGGED TABLE attribute.%I (
	%s,
	PRIMARY KEY (entity_id, timestamp)
	)', table_name, columns_part);

	EXECUTE format('ALTER TABLE attribute.%I
		OWNER TO minerva_admin', table_name);

	PERFORM attribute.create_staging_new_view($1);

	PERFORM attribute.create_staging_modified_view($1);

	RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_staging_new_view(attribute.attributestore)
	RETURNS attribute.attributestore
AS $$
DECLARE
	table_name name;
	staging_table_name name;
	view_name name;
BEGIN
	table_name = attribute.to_table_name($1);
	staging_table_name = table_name || '_staging';
	view_name = staging_table_name || '_new';

	EXECUTE format('CREATE VIEW attribute.%I
AS SELECT s.* FROM attribute.%I s
LEFT JOIN attribute.%I a ON a.entity_id = s.entity_id AND a.timestamp = s.timestamp
WHERE a.entity_id IS NULL', view_name, staging_table_name, table_name);

	EXECUTE format('ALTER TABLE attribute.%I
		OWNER TO minerva_admin', view_name);

	RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_staging_modified_view(attribute.attributestore)
	RETURNS attribute.attributestore
AS $$
DECLARE
	table_name name;
	staging_table_name name;
	view_name name;
BEGIN
	table_name = attribute.to_table_name($1);
	staging_table_name = table_name || '_staging';
	view_name = staging_table_name || '_modified';

	EXECUTE format('CREATE VIEW attribute.%I
AS SELECT s.* FROM attribute.%I s
JOIN attribute.%I a ON a.entity_id = s.entity_id AND a.timestamp = s.timestamp', view_name, staging_table_name, table_name);

	EXECUTE format('ALTER TABLE attribute.%I
		OWNER TO minerva_admin', view_name);

	RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_hash_triggers(attribute.attributestore)
	RETURNS attribute.attributestore
AS $$
DECLARE
	table_name name;
BEGIN
	table_name = attribute.to_table_name($1);

	EXECUTE format('CREATE TRIGGER set_hash_on_update
		BEFORE UPDATE ON attribute.%I
		FOR EACH ROW EXECUTE PROCEDURE attribute.set_hash()', table_name);

	EXECUTE format('CREATE TRIGGER set_hash_on_insert
		BEFORE INSERT ON attribute.%I
		FOR EACH ROW EXECUTE PROCEDURE attribute.set_hash()', table_name);

	RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION render_hash_query(attribute.attributestore)
	RETURNS text
AS $$
	SELECT
		'SELECT md5(' ||
		array_to_string(array_agg(format('($1.%I)::text', name)), ' || ') ||
		')'
	FROM attribute.attribute
	WHERE attributestore_id = $1.id;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION drop_hash_function(attribute.attributestore)
	RETURNS attribute.attributestore
AS $$
BEGIN
	EXECUTE format('DROP FUNCTION attribute.values_hash(attribute.%I)', attribute.to_table_name($1));

	RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_hash_function(attribute.attributestore)
    RETURNS attribute.attributestore
AS $function$
BEGIN
	EXECUTE format('CREATE FUNCTION attribute.values_hash(attribute.%I)
RETURNS text
AS $$
	%s
$$ LANGUAGE SQL STABLE', attribute.to_table_name($1), attribute.render_hash_query($1));

	RETURN $1;
END;
$function$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION compact(attribute.attributestore)
	RETURNS attribute.attributestore
AS $$
BEGIN
	EXECUTE format(
		'DELETE FROM attribute.%I d
		USING attribute.%I changes
		WHERE changes.entity_id = d.entity_id
			AND changes.timestamp = d.timestamp
			AND changes.change = false', attribute.to_table_name($1), attribute.to_table_name($1) || '_changes');

	RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION init(attribute.attribute)
	RETURNS attribute.attribute
AS $$
DECLARE
	table_name name;
BEGIN
	SELECT attribute.to_char(attributestore) INTO table_name FROM attribute.attributestore WHERE id = $1.attributestore_id;

	EXECUTE format('ALTER TABLE attribute.%I ADD COLUMN %I %s', table_name, $1.name, $1.datatype);

	RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION get_attributestore(datasource_id integer, entitytype_id integer)
	RETURNS attribute.attributestore
AS $$
	SELECT attributestore FROM attribute.attributestore WHERE datasource_id = $1 AND entitytype_id = $2;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION create_attributestore(datasource_id integer, entitytype_id integer)
	RETURNS attribute.attributestore
AS $$
	INSERT INTO attribute.attributestore(datasource_id, entitytype_id) VALUES ($1, $2) RETURNING attributestore;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION to_attributestore(datasource_id integer, entitytype_id integer)
	RETURNS attribute.attributestore
AS $$
	SELECT COALESCE(attribute.get_attributestore($1, $2), attribute.init(attribute.create_attributestore($1, $2)));
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION get_attribute(attributestore, name)
	RETURNS attribute.attribute
AS $$
	SELECT attribute FROM attribute.attribute WHERE attributestore_id = $1.id AND name = $2;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION define(attribute.attribute)
	RETURNS attribute.attribute
AS $$
	INSERT INTO attribute.attribute(attributestore_id, description, name, datatype)
	VALUES ($1.attributestore_id, $1.description, $1.name, $1.datatype)
	RETURNING attribute;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION to_attribute(attribute.attribute)
	RETURNS attribute.attribute
AS $$
	SELECT
		COALESCE(attribute.get_attribute(attributestore, $1.name), attribute.init(attribute.define($1)))
	FROM attribute.attributestore WHERE id = $1.attributestore_id;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION check_attributes_exist(attribute.attribute[])
	RETURNS SETOF attribute.attribute
AS $$
	SELECT attribute.to_attribute(n)
	FROM unnest($1) n
	LEFT JOIN attribute.attribute
	ON attribute.attributestore_id = n.attributestore_id AND n.name = attribute.name
	WHERE attribute.name IS NULL;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION check_datatype(attribute.attribute)
	RETURNS attribute.attribute
AS $$
	SELECT attribute.to_table_name(attributestore)
	FROM attribute.attributestore
	WHERE id = $1.attributestore_id;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION check_attribute_types(attribute.attribute[])
	RETURNS SETOF attribute.attribute
AS $$
	UPDATE attribute.attribute SET datatype = n.datatype
	FROM unnest($1) n
	WHERE attribute.name = n.name
	AND attribute.attributestore_id = n.attributestore_id
	AND attribute.datatype <> n.datatype RETURNING attribute.*;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION modify_column_type(table_name name, column_name name, datatype varchar)
	RETURNS void
AS $$
BEGIN
	EXECUTE format('ALTER TABLE attribute.%I ALTER %I TYPE %s USING CAST(%I AS %s)', table_name, column_name, datatype, column_name, datatype);
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION modify_column_type(attribute.attributestore, column_name name, datatype varchar)
	RETURNS attribute.attributestore
AS $$
BEGIN
	PERFORM attribute.modify_column_type(attribute.to_table_name($1), $2, $3);

	RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION modify_datatype(attribute.attribute)
	RETURNS attribute.attribute
AS $$
BEGIN
	PERFORM
		attribute.create_hash_function(
			attribute.modify_column_type(
				attribute.drop_hash_function(attributestore),
				$1.name,
				$1.datatype
			)
		)
	FROM attribute.attributestore
	WHERE id = $1.attributestore_id;

	RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION store_batch(attribute.attributestore)
	RETURNS attribute.attributestore
AS $$
DECLARE
	table_name name;
	staging_table_name name;
	columns_part text;
	set_columns_part text;
	default_columns text[];
BEGIN
	table_name = attribute.to_table_name($1);
	staging_table_name = attribute.to_table_name($1) || '_staging';

	default_columns = ARRAY[
		'entity_id',
		'"timestamp"'];

	SELECT array_to_string(default_columns || array_agg(format('%I', name)), ', ') INTO columns_part
	FROM attribute.attribute
	WHERE attributestore_id = $1.id;

	EXECUTE format('INSERT INTO attribute.%I(%s) SELECT %s FROM attribute.%I', table_name, columns_part, columns_part, staging_table_name || '_new');

	SELECT array_to_string(array_agg(format('%I = m.%I', name, name)), ', ') INTO set_columns_part
	FROM attribute.attribute
	WHERE attributestore_id = $1.id;

	EXECUTE format('UPDATE attribute.%I a SET %s FROM attribute.%I m WHERE m.entity_id = a.entity_id AND m.timestamp = a.timestamp', table_name, set_columns_part, staging_table_name || '_modified');

	EXECUTE format('TRUNCATE attribute.%I', staging_table_name);

	RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;

