SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;


CREATE SCHEMA notification_tests;
ALTER SCHEMA notification_tests OWNER TO minerva_admin;

GRANT ALL ON SCHEMA notification_tests TO minerva_writer;
GRANT USAGE ON SCHEMA notification_tests TO minerva;

SET search_path = notification_tests, pg_catalog;


CREATE OR REPLACE FUNCTION test_create_notificationstore()
	RETURNS SETOF text
AS $$
BEGIN
	RETURN NEXT isa_ok(
		notification.create_notificationstore('some_datasource_name'),
		'notification.notificationstore',
		'the result of create_notificationstore'
	);

	RETURN NEXT has_table(
		'notification'::name, 'some_datasource_name'::name,
		'table with name of datasource should exist'
	);

	RETURN NEXT has_column(
		'notification'::name, 'some_datasource_name'::name, 'id'::name,
		'notification store table has a column id'
	);

	RETURN NEXT has_column(
		'notification'::name, 'some_datasource_name'::name, 'entity_id'::name,
		'notification store table has a column entity_id'
	);

	RETURN NEXT has_column(
		'notification'::name, 'some_datasource_name'::name, 'timestamp'::name,
		'notification store table has a column timestamp'
	);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION test_example_notificationsetstore()
	RETURNS SETOF text
AS $$
DECLARE
	notificationstore notification.notificationstore;
BEGIN
	notificationstore = notification.create_notificationstore('state_changes');

	RETURN NEXT isa_ok(
		notification.create_notificationsetstore('ticket'::name, notificationstore.id, 'tmp'::name),
		'notification.notificationsetstore',
		'the result of create_notificationsetstore'
	);

	RETURN NEXT has_table(
		'notification'::name, 'ticket'::name,
		'table with name of notificationsetstore should exist'
	);
END;
$$ LANGUAGE plpgsql;
