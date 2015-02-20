CREATE TRIGGER drop_table_on_delete
    BEFORE DELETE ON notification.notification_store
    FOR EACH ROW
    EXECUTE PROCEDURE notification.drop_table_on_delete();


CREATE TRIGGER drop_notificationsetstore_table_on_delete
    BEFORE DELETE ON notification.notificationsetstore
    FOR EACH ROW
    EXECUTE PROCEDURE notification.drop_notificationsetstore_table_on_delete();


CREATE TRIGGER delete_notification_stores_on_data_source_delete
    BEFORE DELETE ON directory.data_source
    FOR EACH ROW
    EXECUTE PROCEDURE notification.cleanup_on_data_source_delete();
