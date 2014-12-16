SET search_path = public, pg_catalog;


CREATE AGGREGATE first(
    sfunc    = public.fst,
    basetype = anyelement,
    stype    = anyelement
);


CREATE AGGREGATE last(
    sfunc    = public.snd,
    basetype = anyelement,
    stype    = anyelement
);
