SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = notification, pg_catalog;


CREATE OR REPLACE FUNCTION create_table_on_insert()
    RETURNS TRIGGER
AS $$
DECLARE
    table_name name;
BEGIN
    table_name = notification.table_name(NEW);

    IF NOT notification.table_exists(table_name) THEN
        PERFORM notification.create_table(table_name);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION drop_table_on_delete()
    RETURNS TRIGGER
AS $$
DECLARE
    table_name name;
BEGIN
    table_name = notification.table_name(OLD);

    EXECUTE format('DROP TABLE IF EXISTS notification.%I CASCADE', table_name);

    RETURN OLD;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION drop_notificationsetstore_table_on_delete()
    RETURNS TRIGGER
AS $$
BEGIN
    EXECUTE format('DROP TABLE IF EXISTS notification.%I', OLD.name || '_link');
    EXECUTE format('DROP TABLE IF EXISTS notification.%I', OLD.name);

    RETURN OLD;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_attribute_column_on_insert()
    RETURNS TRIGGER
AS $$
DECLARE
    table_name character varying;
BEGIN
    SELECT notification.table_name(notificationstore) INTO table_name FROM notification.notificationstore WHERE id = NEW.notificationstore_id;

    EXECUTE format('ALTER TABLE notification.%I ADD COLUMN %I %s', table_name, NEW.name, NEW.data_type);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION cleanup_on_datasource_delete()
    RETURNS TRIGGER
AS $$
BEGIN
    DELETE FROM notification.notificationstore WHERE datasource_id = OLD.id;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql VOLATILE;
