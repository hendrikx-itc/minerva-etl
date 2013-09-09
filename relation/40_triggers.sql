SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = relation, pg_catalog;


CREATE TRIGGER create_table_on_insert
	AFTER INSERT ON "type"
	FOR EACH ROW
	EXECUTE PROCEDURE create_relation_table_on_insert();


CREATE TRIGGER delete_relation_table_on_type_delete
	AFTER DELETE ON "type"
	FOR EACH ROW
	EXECUTE PROCEDURE drop_table_on_type_delete();


CREATE TRIGGER create_self_relation_on_entity_insert
	AFTER INSERT ON directory.entity
	FOR EACH ROW
	EXECUTE PROCEDURE create_self_relation();
