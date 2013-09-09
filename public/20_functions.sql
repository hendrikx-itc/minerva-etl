SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = public, pg_catalog;


CREATE OR REPLACE FUNCTION integer_to_array(value integer)
	RETURNS integer[]
AS $$
BEGIN
	RETURN ARRAY[value];
END;
$$ LANGUAGE plpgsql STABLE STRICT;


CREATE OR REPLACE FUNCTION smallint_to_array(value smallint)
	RETURNS smallint[]
AS $$
BEGIN
	RETURN ARRAY[value];
END;
$$ LANGUAGE plpgsql STABLE STRICT;


-- Used to 'cast' from smallint to timestamp without timezone. This normally
-- isn't possible but we want to be able to switch. Implicitly, we just set all
-- values to NULL when converting a column from smallint to timestamp without
-- time zone.
CREATE OR REPLACE FUNCTION smallint_to_timestamp_without_time_zone (smallint)
	RETURNS timestamp without time zone AS
$$
BEGIN
	RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE STRICT;


-- Same 'cast' support for timestamp with time zone
CREATE OR REPLACE FUNCTION smallint_to_timestamp_with_time_zone (smallint)
	RETURNS timestamp with time zone
AS $$
BEGIN
	RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE STRICT;
