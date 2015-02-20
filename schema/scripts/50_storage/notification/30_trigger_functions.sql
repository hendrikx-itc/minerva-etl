CREATE FUNCTION notification.drop_table_on_delete()
    RETURNS TRIGGER
AS $$
BEGIN
    EXECUTE format('DROP TABLE IF EXISTS notification.%I CASCADE', notification.staging_table_name(OLD));
    EXECUTE format('DROP TABLE IF EXISTS notification.%I CASCADE', notification.table_name(OLD));

    RETURN OLD;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION notification.drop_notificationsetstore_table_on_delete()
    RETURNS TRIGGER
AS $$
BEGIN
    EXECUTE format('DROP TABLE IF EXISTS notification.%I', OLD.name || '_link');
    EXECUTE format('DROP TABLE IF EXISTS notification.%I', OLD.name);

    RETURN OLD;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION notification.cleanup_on_data_source_delete()
    RETURNS TRIGGER
AS $$
BEGIN
    DELETE FROM notification.notification_store WHERE data_source_id = OLD.id;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql VOLATILE;
