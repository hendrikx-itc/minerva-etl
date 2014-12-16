SET search_path = attribute_directory, pg_catalog;


CREATE TRIGGER delete_attributestores_on_datasource_delete
    BEFORE DELETE ON directory.datasource
    FOR EACH ROW
    EXECUTE PROCEDURE cleanup_on_datasource_delete();


CREATE TRIGGER cleanup_attributestore_on_delete
    BEFORE DELETE ON attributestore
    FOR EACH ROW
    EXECUTE PROCEDURE cleanup_attributestore_on_delete();


CREATE TRIGGER update_attribute_type
    AFTER UPDATE ON attribute
    FOR EACH ROW
    EXECUTE PROCEDURE update_datatype_on_change();


CREATE TRIGGER after_delete_attribute
    AFTER DELETE ON attribute
    FOR EACH ROW
    EXECUTE PROCEDURE cleanup_attribute_after_delete();
