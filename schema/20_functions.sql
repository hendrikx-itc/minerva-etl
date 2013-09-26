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
	SELECT $1;
$$ LANGUAGE SQL IMMUTABLE STRICT;
