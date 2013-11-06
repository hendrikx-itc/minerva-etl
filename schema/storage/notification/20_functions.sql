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
DECLARE
	sql text;
	full_table_name text;
BEGIN
	EXECUTE format('CREATE TABLE %I.%I (
		entity_id integer NOT NULL,
		"timestamp" timestamp with time zone NOT NULL
		);', 'notification', name);

	EXECUTE format('ALTER TABLE %I.%I OWNER TO minerva_writer;', 'notification',
		name);

	EXECUTE format('ALTER TABLE ONLY %I.%I
		ADD CONSTRAINT %I
		PRIMARY KEY (entity_id, "timestamp");', 'notification', name, name || '_pkey');

	EXECUTE format('GRANT SELECT ON TABLE %I.%I TO minerva;', 'notification',
		name);
	EXECUTE format('GRANT INSERT,DELETE,UPDATE ON TABLE %I.%I TO minerva_writer;',
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


CREATE OR REPLACE FUNCTION table_exists(name)
	RETURNS boolean
AS $$
	SELECT exists(
		SELECT 1
		FROM pg_class
		JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
		WHERE relname=$1 AND relkind = 'r' AND pg_namespace.nspname = 'notification'
	);
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION column_exists(table_name name, column_name name)
	RETURNS boolean
AS $$
	SELECT EXISTS(
		SELECT 1
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE c.relname = table_name AND a.attname = column_name AND n.nspname = 'notification'
	);
$$ LANGUAGE sql STABLE;
