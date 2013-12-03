SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = attribute_directory, pg_catalog;


CREATE OR REPLACE FUNCTION cleanup_on_datasource_delete()
	RETURNS TRIGGER
AS $$
BEGIN
	DELETE FROM attribute_directory.attributestore WHERE datasource_id = OLD.id;

	RETURN OLD;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION cleanup_attributestore_on_delete()
	RETURNS TRIGGER
AS $$
BEGIN
	PERFORM attribute_directory.drop_dependees(OLD);

	EXECUTE format('DROP TABLE IF EXISTS attribute_base.%I CASCADE', attribute_directory.to_table_name(OLD));

	EXECUTE format('DROP FUNCTION attribute_history.mark_modified_%s()', OLD.id);

	EXECUTE format('DROP TABLE IF EXISTS attribute_history.%I', attribute_directory.to_table_name(OLD) || '_curr_ptr');

	RETURN OLD;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cleanup_attribute_after_delete()
	RETURNS TRIGGER
AS $$
DECLARE
	table_name name;
BEGIN
	SELECT attribute_directory.to_table_name(attributestore) INTO table_name
	FROM attribute_directory.attributestore WHERE id = OLD.attributestore_id;

	PERFORM attribute_directory.drop_dependees(attributestore) FROM attribute_directory.attributestore WHERE id = OLD.attributestore_id;

	EXECUTE format('ALTER TABLE attribute_base.%I DROP COLUMN %I', table_name, OLD.name);

	PERFORM attribute_directory.create_dependees(attributestore) FROM attribute_directory.attributestore WHERE id = OLD.attributestore_id;

	RETURN OLD;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_datatype_on_change()
	RETURNS TRIGGER
AS $$
BEGIN
	IF OLD.datatype <> NEW.datatype THEN
		PERFORM attribute_directory.modify_datatype(NEW);
	END IF;

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION set_hash()
	RETURNS TRIGGER
AS $$
BEGIN
	NEW.hash = attribute_history.values_hash(NEW);

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
