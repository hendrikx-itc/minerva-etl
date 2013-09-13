SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = notification, pg_catalog;


CREATE TRIGGER create_table_on_insert
	BEFORE INSERT ON notificationstore
	FOR EACH ROW
	EXECUTE PROCEDURE create_table_on_insert();


CREATE TRIGGER drop_table_on_delete
	AFTER DELETE ON notificationstore
	FOR EACH ROW
	EXECUTE PROCEDURE drop_table_on_delete();


CREATE TRIGGER create_column_on_insert
	BEFORE INSERT ON attribute
	FOR EACH ROW
	EXECUTE PROCEDURE create_attribute_column_on_insert();
