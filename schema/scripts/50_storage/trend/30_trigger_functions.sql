SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = trend, pg_catalog;


CREATE OR REPLACE FUNCTION changes_on_datasource_update()
    RETURNS TRIGGER
AS $$
BEGIN
    IF NEW.name <> OLD.name THEN
        UPDATE trend.partition SET
            table_name = trend.to_table_name_v4(partition)
        FROM trend.trendstore ts
        WHERE ts.datasource_id = NEW.id AND ts.id = partition.trendstore_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION cleanup_on_datasource_delete()
    RETURNS TRIGGER
AS $$
BEGIN
    DELETE FROM trend.trendstore WHERE datasource_id = OLD.id;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION changes_on_partition_update()
    RETURNS TRIGGER
AS $$
BEGIN
    IF NEW.table_name <> OLD.table_name THEN
        EXECUTE format('ALTER TABLE trend.%I RENAME TO %I', OLD.table_name, NEW.table_name);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION changes_on_trend_update()
    RETURNS TRIGGER
AS $$
DECLARE
    base_table_name text;
BEGIN
    IF NEW.name <> OLD.name THEN
        FOR base_table_name IN
            SELECT trend.to_base_table_name(ts)
            FROM trend.trend t
            JOIN trend.trendstore_trend_link ttl ON ttl.trend_id = t.id
            JOIN trend.trendstore ts ON ttl.trendstore_id = ts.id
            WHERE t.id = NEW.id
        LOOP
            EXECUTE format('ALTER TABLE trend.%I RENAME COLUMN %I TO %I', base_table_name, OLD.name, NEW.name);
        END LOOP;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_partition_table_on_insert()
    RETURNS TRIGGER
AS $$
DECLARE
    base_table_name text;
    vacuum_partition_index int;
    trendstore trend.trendstore;
BEGIN
    IF NEW.table_name IS NULL THEN
        NEW.table_name = trend.to_table_name_v4(NEW);
    END IF;

    IF NOT trend.partition_exists(NEW.table_name::text) THEN
        SELECT trendstore INTO trendstore FROM trend.trendstore WHERE id = NEW.trendstore_id;

        base_table_name = trend.to_base_table_name(trendstore);

        PERFORM trend.create_partition_table_v4(base_table_name, NEW.table_name, NEW.data_start, NEW.data_end);

        -- mark the second to last partition as available for vacuum full
        vacuum_partition_index = trend.timestamp_to_index(trendstore.partition_size, NEW.data_start) - 2;
        INSERT INTO trend.to_be_vacuumed (table_name) SELECT trend.partition_name(trendstore, vacuum_partition_index);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION drop_partition_table_on_delete()
    RETURNS TRIGGER
AS $$
DECLARE
    kind CHAR;
BEGIN
    SELECT INTO kind relkind FROM pg_class WHERE relname = OLD.table_name;

    IF kind = 'r' THEN
        EXECUTE format('DROP TABLE IF EXISTS trend.%I CASCADE', OLD.table_name);
    ELSIF kind = 'v' THEN
        EXECUTE format('DROP VIEW trend.%I', OLD.table_name);
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_modified_column()
    RETURNS TRIGGER
AS $$
BEGIN
    NEW.modified = NOW();

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION create_base_table_on_insert()
    RETURNS TRIGGER
AS $$
DECLARE
    table_name text;
BEGIN
    IF NEW.version = 4 AND NEW.type = 'table' THEN
        -- Only version 4 trendstores use a base table
        table_name = trend.to_base_table_name(NEW);

        PERFORM trend.create_partition_table(table_name);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION on_trendstore_update()
    RETURNS TRIGGER
AS $$
DECLARE
    base_table_name text;
    r trend.trend_with_type%rowtype;
BEGIN
    IF OLD.version = 3 AND NEW.version = 4 AND NEW.type = 'table' THEN
        base_table_name = trend.to_base_table_name(NEW);

        PERFORM trend.create_partition_table(base_table_name);

        FOR r IN SELECT * FROM trend.get_trends_for_v3_trendstore(OLD) LOOP
            PERFORM trend.add_trend_to_trendstore(NEW, r);
        END LOOP;
    ELSIF OLD.version = 4 AND NEW.version = 3 THEN
        base_table_name = trend.to_base_table_name(NEW);

        EXECUTE format('DROP TABLE trend.%I', base_table_name);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION cleanup_trendstore_on_delete()
    RETURNS TRIGGER
AS $$
DECLARE
    table_name text;
BEGIN
    IF OLD.version = 4 THEN
        table_name = trend.to_base_table_name(OLD);

        IF OLD.type = 'table' THEN
            EXECUTE format('DROP TABLE IF EXISTS trend.%I CASCADE', table_name);
        ELSIF OLD.type = 'view' THEN
            DELETE FROM trend.view WHERE trendstore_id = OLD.id;
        END IF;
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION set_trendstore_defaults()
    RETURNS TRIGGER
AS $$
BEGIN
    IF NEW.partition_size IS NULL THEN
        NEW.partition_size = trend.get_default_partition_size(NEW.granularity);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION drop_view_on_delete()
    RETURNS TRIGGER
AS $$
BEGIN
    PERFORM trend.drop_view(OLD);

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;
