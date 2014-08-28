CREATE SCHEMA virtual_entity;
ALTER SCHEMA virtual_entity OWNER TO minerva_admin;

GRANT ALL ON SCHEMA virtual_entity TO minerva_admin;
GRANT USAGE ON SCHEMA virtual_entity TO minerva;

SET search_path = virtual_entity, pg_catalog;
