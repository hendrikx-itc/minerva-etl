CREATE TRIGGER propagate_changes_on_update_to_trend
    AFTER UPDATE on directory.datasource
    FOR EACH ROW
    EXECUTE PROCEDURE trend_directory.changes_on_datasource_update();


CREATE TRIGGER propagate_changes_on_trend_update
    AFTER UPDATE ON trend_directory.trend
    FOR EACH ROW
    EXECUTE PROCEDURE trend_directory.changes_on_trend_update();


CREATE TRIGGER create_table_on_insert
    BEFORE INSERT ON trend_directory.partition
    FOR EACH ROW
    EXECUTE PROCEDURE trend_directory.create_partition_table_on_insert();


CREATE TRIGGER drop_table_on_delete
    AFTER DELETE ON trend_directory.partition
    FOR EACH ROW
    EXECUTE PROCEDURE trend_directory.drop_partition_table_on_delete();


CREATE TRIGGER delete_trendstores_on_datasource_delete
    BEFORE DELETE ON directory.datasource
    FOR EACH ROW
    EXECUTE PROCEDURE trend_directory.cleanup_on_datasource_delete();


CREATE TRIGGER cleanup_trendstore_on_delete
    BEFORE DELETE ON trend_directory.trendstore
    FOR EACH ROW
    EXECUTE PROCEDURE trend_directory.cleanup_trendstore_on_delete();


CREATE TRIGGER drop_view_on_delete
    BEFORE DELETE ON trend_directory.view
    FOR EACH ROW
    EXECUTE PROCEDURE trend_directory.drop_view_on_delete();
