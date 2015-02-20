CREATE TRIGGER drop_table_on_delete
    BEFORE DELETE ON notification_directory.notification_store
    FOR EACH ROW
    EXECUTE PROCEDURE notification_directory.drop_table_on_delete();


CREATE TRIGGER drop_notificationsetstore_table_on_delete
    BEFORE DELETE ON notification_directory.notificationsetstore
    FOR EACH ROW
    EXECUTE PROCEDURE notification_directory.drop_notificationsetstore_table_on_delete();


CREATE TRIGGER delete_notification_stores_on_data_source_delete
    BEFORE DELETE ON directory.data_source
    FOR EACH ROW
    EXECUTE PROCEDURE notification_directory.cleanup_on_data_source_delete();
