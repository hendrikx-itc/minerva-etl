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


CREATE OR REPLACE FUNCTION public.column_names(namespace name, "table" name)
    RETURNS SETOF name
AS $$
    SELECT a.attname
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
    WHERE
        n.nspname = $1 AND
        c.relname = $2 AND
        a.attisdropped = false AND
        a.attnum > 0;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION public.fst(anyelement, anyelement)
    RETURNS anyelement
AS $$
    SELECT $1;
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION public.snd(anyelement, anyelement)
    RETURNS anyelement
AS $$
    SELECT $2;
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION public.wal_location_to_int(text)
    RETURNS bigint
AS $$
    SELECT ('x' || lpad((regexp_split_to_array($1, '/'))[1] || lpad((regexp_split_to_array($1, '/'))[2], 8, '0'), 16, '0'))::bit(64)::bigint;
$$ LANGUAGE SQL IMMUTABLE STRICT;

COMMENT ON FUNCTION public.wal_location_to_int(text) IS
'Convert a textual WAL location in the form of ''1752F/CDC6E050'' into a bigint.
Use this function to monitor slave delay on the master:

SELECT
  client_addr,
  (public.wal_location_to_int(pg_current_xlog_location()) - public.wal_location_to_int(replay_location)) / 2^20 AS distance_mb
FROM pg_stat_replication;';


CREATE OR REPLACE FUNCTION safe_division(numerator anyelement, denominator anyelement)
	RETURNS anyelement
AS $$
SELECT CASE
	WHEN $2 = 0 THEN
		NULL
	ELSE
		$1 / $2
	END;
$$ LANGUAGE SQL IMMUTABLE;

ALTER FUNCTION safe_division(anyelement, anyelement)
	OWNER TO postgres;


CREATE OR REPLACE FUNCTION add_array(anyarray, anyarray) RETURNS anyarray
AS $$
SELECT array_agg((arr1 + arr2)) FROM
(
	SELECT
		unnest($1[1:least(array_length($1,1), array_length($2,1))]) AS arr1,
		unnest($2[1:least(array_length($1,1), array_length($2,1))]) AS arr2
) AS foo;
$$ LANGUAGE SQL STABLE STRICT;


CREATE AGGREGATE sum_array(anyarray)
(
	sfunc = add_array,
	stype = anyarray
);


CREATE OR REPLACE FUNCTION divide_array(anyarray, anyelement)
    RETURNS anyarray
AS $$
SELECT array_agg(arr / $2) FROM
(
    SELECT unnest($1) AS arr
) AS foo;
$$ LANGUAGE SQL STABLE STRICT;


CREATE OR REPLACE FUNCTION divide_array(anyarray, anyarray)
    RETURNS anyarray
AS $$
SELECT array_agg(public.safe_division(arr1, arr2)) FROM
(
    SELECT
    unnest($1[1:least(array_length($1,1), array_length($2,1))]) AS arr1,
    unnest($2[1:least(array_length($1,1), array_length($2,1))]) AS arr2
) AS foo;
$$ LANGUAGE SQL STABLE STRICT;

