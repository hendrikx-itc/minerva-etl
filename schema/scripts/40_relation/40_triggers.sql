SET search_path = relation, pg_catalog;


CREATE TRIGGER create_table_on_insert
    AFTER INSERT ON "type"
    FOR EACH ROW
    EXECUTE PROCEDURE create_relation_table_on_insert();


CREATE TRIGGER delete_relation_table_on_type_delete
    AFTER DELETE ON "type"
    FOR EACH ROW
    EXECUTE PROCEDURE drop_table_on_type_delete();


CREATE TRIGGER create_self_relation_on_entity_insert
    AFTER INSERT ON directory.entity
    FOR EACH ROW
    EXECUTE PROCEDURE create_self_relation();
