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
