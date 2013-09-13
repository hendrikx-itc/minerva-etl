SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = attribute, pg_catalog;


CREATE TRIGGER delete_attributestores_on_datasource_delete
	BEFORE DELETE ON directory.datasource
	FOR EACH ROW
	EXECUTE PROCEDURE cleanup_on_datasource_delete();


CREATE TRIGGER cleanup_attributestore_on_delete
	BEFORE DELETE ON attributestore
	FOR EACH ROW
	EXECUTE PROCEDURE cleanup_attributestore_on_delete();


CREATE TRIGGER update_attribute_type
	AFTER UPDATE ON attribute
	FOR EACH ROW
	EXECUTE PROCEDURE update_datatype_on_change();
