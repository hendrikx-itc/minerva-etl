SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = attribute, pg_catalog;


CREATE OR REPLACE FUNCTION cleanup_on_datasource_delete()
	RETURNS TRIGGER
AS $$
BEGIN
	DELETE FROM attribute.attributestore WHERE datasource_id = OLD.id;

	RETURN OLD;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION cleanup_attributestore_on_delete()
	RETURNS TRIGGER
AS $$
BEGIN
	EXECUTE format('DROP TABLE IF EXISTS attribute.%I CASCADE', attribute.to_table_name(OLD));
	EXECUTE format('DROP TABLE IF EXISTS attribute.%I CASCADE', attribute.to_table_name(OLD) || '_staging');

	RETURN OLD;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_datatype_on_change()
	RETURNS TRIGGER
AS $$
BEGIN
	IF OLD.datatype <> NEW.datatype THEN
		PERFORM attribute.modify_datatype(NEW);
	END IF;

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION set_hash()
	RETURNS TRIGGER
AS $$
BEGIN
	NEW.hash = attribute.values_hash(NEW);

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_modified_column()
	RETURNS TRIGGER
AS $$
BEGIN
	NEW.modified = now();

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
