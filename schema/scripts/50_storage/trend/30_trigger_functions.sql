CREATE FUNCTION trend_directory.changes_on_datasource_update()
    RETURNS TRIGGER
AS $$
BEGIN
    IF NEW.name <> OLD.name THEN
        UPDATE trend_directory.partition SET
            table_name = trend_directory.to_table_name(partition)
        FROM trend_directory.trendstore ts
        WHERE ts.datasource_id = NEW.id AND ts.id = partition.trendstore_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION trend_directory.cleanup_on_datasource_delete()
    RETURNS TRIGGER
AS $$
BEGIN
    DELETE FROM trend_directory.trendstore WHERE datasource_id = OLD.id;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION trend_directory.changes_on_partition_update()
    RETURNS TRIGGER
AS $$
BEGIN
    IF NEW.table_name <> OLD.table_name THEN
        EXECUTE format('ALTER TABLE trend_directory.%I RENAME TO %I', OLD.table_name, NEW.table_name);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION trend_directory.changes_on_trend_update()
    RETURNS TRIGGER
AS $$
DECLARE
    base_table_name text;
BEGIN
    IF NEW.name <> OLD.name THEN
        FOR base_table_name IN
            SELECT trend_directory.to_base_table_name(ts)
            FROM trend_directory.trend t
            JOIN trend_directory.trendstore_trend_link ttl ON ttl.trend_id = t.id
            JOIN trend_directory.trendstore ts ON ttl.trendstore_id = ts.id
            WHERE t.id = NEW.id
        LOOP
            EXECUTE format('ALTER TABLE trend_directory.%I RENAME COLUMN %I TO %I', base_table_name, OLD.name, NEW.name);
        END LOOP;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION trend_directory.create_partition_table_on_insert()
    RETURNS TRIGGER
AS $$
BEGIN
    IF NOT trend_directory.partition_exists(NEW) THEN
        PERFORM trend_directory.create_partition_table(NEW);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION trend_directory.drop_partition_table_on_delete()
    RETURNS TRIGGER
AS $$
DECLARE
    kind CHAR;
BEGIN
    SELECT INTO kind relkind FROM pg_class WHERE relname = OLD.table_name;

    IF kind = 'r' THEN
        EXECUTE format('DROP TABLE IF EXISTS trend_directory.%I CASCADE', OLD.table_name);
    ELSIF kind = 'v' THEN
        EXECUTE format('DROP VIEW trend_directory.%I', OLD.table_name);
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION trend_directory.update_modified_column()
    RETURNS TRIGGER
AS $$
BEGIN
    NEW.modified = NOW();

    RETURN NEW;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION trend_directory.cleanup_trendstore_on_delete()
    RETURNS TRIGGER
AS $$
DECLARE
    table_name text;
BEGIN
    table_name = trend_directory.to_base_table_name(OLD);

    IF OLD.type = 'table' THEN
        EXECUTE format('DROP TABLE IF EXISTS trend.%I CASCADE', table_name);
    ELSIF OLD.type = 'view' THEN
        DELETE FROM trend_directory.view WHERE trendstore_id = OLD.id;
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION trend_directory.drop_view_on_delete()
    RETURNS TRIGGER
AS $$
BEGIN
    PERFORM trend_directory.drop_view(OLD);

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;
