SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = notification, pg_catalog;


CREATE OR REPLACE FUNCTION to_char(notification.notificationstore)
	RETURNS text
AS $$
	SELECT datasource.name
	FROM directory.datasource
	WHERE datasource.id = $1.datasource_id;
$$ LANGUAGE SQL STABLE STRICT;


CREATE OR REPLACE FUNCTION notification.create_table(name name)
	RETURNS void
AS $$
BEGIN
	EXECUTE format(
		'CREATE TABLE %I.%I ('
		'  id serial PRIMARY KEY,'
		'  entity_id integer NOT NULL,'
		'  "timestamp" timestamp with time zone NOT NULL'
		');', 'notification', name);

	EXECUTE format('ALTER TABLE %I.%I OWNER TO minerva_writer;', 'notification',
		name);

	EXECUTE format('GRANT SELECT ON TABLE %I.%I TO minerva;', 'notification',
		name);

	EXECUTE format(
		'GRANT INSERT,DELETE,UPDATE '
		'ON TABLE %I.%I TO minerva_writer;',
		'notification', name);

	EXECUTE format('CREATE INDEX %I ON %I.%I USING btree (timestamp);',
		'idx_notification_' || name || '_timestamp', 'notification', name);
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION table_name(notification.notificationstore)
	RETURNS name
AS $$
	SELECT ds.name::name
		FROM directory.datasource ds
		WHERE
			ds.id = $1.datasource_id;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION table_exists(schema_name name, table_name name)
	RETURNS boolean
AS $$
	SELECT exists(
		SELECT 1
		FROM pg_class
		JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
		WHERE relname=$2 AND relkind = 'r' AND pg_namespace.nspname = $1
	);
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION table_exists(name)
	RETURNS boolean
AS $$
	SELECT notification.table_exists('notification', $1);
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION column_exists(schema_name name, table_name name, column_name name)
	RETURNS boolean
AS $$
	SELECT EXISTS(
		SELECT 1
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3;
	);
$$ LANGUAGE sql STABLE;


CREATE OR REPLACE FUNCTION column_exists(table_name name, column_name name)
	RETURNS boolean
AS $$
	SELECT notification.column_exists('notification', $1, $2);
$$ LANGUAGE sql STABLE;


CREATE OR REPLACE FUNCTION create_notificationstore(datasource_id integer)
	RETURNS notification.notificationstore
AS $$
	INSERT INTO notification.notificationstore(datasource_id, version) VALUES ($1, 1) RETURNING *;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION create_notificationstore(datasource_name text)
	RETURNS notification.notificationstore
AS $$
	SELECT notification.create_notificationstore((directory.name_to_datasource($1)).id);
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION create_attribute(notification.notificationstore, name, name)
	RETURNS SETOF notification.attribute
AS $$
	INSERT INTO notification.attribute(notificationstore_id, name, data_type, description)
	VALUES($1.id, $2, $3, '') RETURNING *;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION create_notificationstore(datasource_id integer, notification.attr_def[])
	RETURNS notification.notificationstore
AS $$
DECLARE
	nstore notification.notificationstore;
BEGIN
	nstore = notification.create_notificationstore($1);

	PERFORM notification.create_attribute(nstore, attr.name, attr.data_type) FROM unnest($2) attr;

	RETURN nstore;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_notificationstore(datasource_name text, notification.attr_def[])
	RETURNS notification.notificationstore
AS $$
	SELECT notification.create_notificationstore((directory.name_to_datasource($1)).id, $2);
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION define_notificationsetstore(name name, notificationstore_id integer)
	RETURNS notification.notificationsetstore
AS $$
	INSERT INTO notification.notificationsetstore(name, notificationstore_id)
	VALUES ($1, $2)
	RETURNING *;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION notificationstore(notification.notificationsetstore)
	RETURNS notification.notificationstore
AS $$
	SELECT notificationstore FROM notification.notificationstore WHERE id = $1.notificationstore_id;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION init_notificationsetstore(notification.notificationsetstore)
	RETURNS notification.notificationsetstore
AS $$
BEGIN
	EXECUTE format(
		'CREATE TABLE notification.%I('
		'  id serial PRIMARY KEY'
		')', $1.name);

	EXECUTE format(
		'CREATE TABLE notification.%I('
		'  notification_id integer REFERENCES notification.%I ON DELETE CASCADE,'
		'  set_id integer REFERENCES notification.%I ON DELETE CASCADE'
		')',
		$1.name || '_link',
		notification.table_name(notification.notificationstore($1)),
		$1.name
	);

	RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_notificationsetstore(name name, notificationstore_id integer)
	RETURNS notification.notificationsetstore
AS $$
	SELECT notification.init_notificationsetstore(
		notification.define_notificationsetstore($1, $2)
	);
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION create_notificationsetstore(name name, notification.notificationstore)
	RETURNS notification.notificationsetstore
AS $$
	SELECT notification.create_notificationsetstore($1, $2.id);
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION get_column_type_name(namespace_name name, table_name name, column_name name)
	RETURNS name
AS $$
	SELECT typname
	FROM pg_type
	JOIN pg_attribute ON pg_attribute.atttypid = pg_type.oid
	JOIN pg_class ON pg_class.oid = pg_attribute.attrelid
	JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
	WHERE nspname = $1 AND relname = $2 AND typname = $3 AND attnum > 0 AND not pg_attribute.attisdropped;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION get_column_type_name(notification.notificationstore, name)
	RETURNS name
AS $$
	SELECT notification.get_column_type_name('notification', notification.table_name($1), $2);
$$ LANGUAGE SQL STABLE;
