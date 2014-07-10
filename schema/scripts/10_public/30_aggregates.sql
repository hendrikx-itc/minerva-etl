SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

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
