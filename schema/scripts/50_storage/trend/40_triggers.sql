SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = trend, pg_catalog;


CREATE TRIGGER propagate_changes_on_update_to_trend
    AFTER UPDATE on directory.datasource
    FOR EACH ROW
    EXECUTE PROCEDURE changes_on_datasource_update();


CREATE TRIGGER propagate_changes_on_update
    AFTER UPDATE ON partition
    FOR EACH ROW
    EXECUTE PROCEDURE changes_on_partition_update();


CREATE TRIGGER propagate_changes_on_trend_update
    AFTER UPDATE ON trend
    FOR EACH ROW
    EXECUTE PROCEDURE changes_on_trend_update();


CREATE TRIGGER create_table_on_insert
    BEFORE INSERT ON partition
    FOR EACH ROW
    EXECUTE PROCEDURE create_partition_table_on_insert();


CREATE TRIGGER drop_table_on_delete
    AFTER DELETE ON partition
    FOR EACH ROW
    EXECUTE PROCEDURE drop_partition_table_on_delete();


CREATE TRIGGER delete_trendstores_on_datasource_delete
    BEFORE DELETE ON directory.datasource
    FOR EACH ROW
    EXECUTE PROCEDURE cleanup_on_datasource_delete();


CREATE TRIGGER cleanup_trendstore_on_delete
    BEFORE DELETE ON trendstore
    FOR EACH ROW
    EXECUTE PROCEDURE cleanup_trendstore_on_delete();


CREATE TRIGGER create_trendstore_base_on_insert
    BEFORE INSERT ON trendstore
    FOR EACH ROW
    EXECUTE PROCEDURE create_base_table_on_insert();


CREATE TRIGGER handle_trendstore_update
    BEFORE UPDATE ON trendstore
    FOR EACH ROW
    EXECUTE PROCEDURE on_trendstore_update();


CREATE TRIGGER set_trendstore_defaults
    BEFORE INSERT ON trendstore
    FOR EACH ROW
    EXECUTE PROCEDURE set_trendstore_defaults();

CREATE TRIGGER drop_view_on_delete
    BEFORE DELETE ON view
    FOR EACH ROW
    EXECUTE PROCEDURE drop_view_on_delete();
