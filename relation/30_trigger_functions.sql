SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = relation, pg_catalog;


CREATE OR REPLACE FUNCTION create_relation_table_on_insert()
	RETURNS TRIGGER
AS $$
BEGIN
	PERFORM relation.create_relation_table(NEW.name, NEW.id);

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION drop_table_on_type_delete()
	RETURNS TRIGGER
AS $$
BEGIN
	EXECUTE format('DROP TABLE IF EXISTS %I.%I', 'relation', OLD.name);

	RETURN OLD;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION create_self_relation()
	RETURNS TRIGGER
AS $$
BEGIN
	INSERT INTO relation.self (source_id, target_id, type_id) SELECT NEW.id, NEW.id, "type".id FROM relation."type" WHERE name = 'self';

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
