SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = directory, pg_catalog;


CREATE TRIGGER "create alias for new entity"
    AFTER INSERT
    ON directory.entity
    FOR EACH ROW
    EXECUTE PROCEDURE directory."create alias for new entity (func)"();

CREATE TRIGGER "create entitytaglink for new entity"
    AFTER INSERT
    ON directory.entity
    FOR EACH ROW
    EXECUTE PROCEDURE directory."create entitytaglink for new entity (func)"();

CREATE TRIGGER "create tag for new entitytypes"
    AFTER INSERT
    ON directory.entitytype
    FOR EACH ROW
    EXECUTE PROCEDURE directory."create tag for new entitytypes (func)"();
