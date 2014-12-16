SET search_path = attribute_directory, pg_catalog;


CREATE CAST (attribute_directory.attributestore AS text)
WITH FUNCTION attribute_directory.to_char(attribute_directory.attributestore);
