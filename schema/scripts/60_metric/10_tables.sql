SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

CREATE SCHEMA metric;
ALTER SCHEMA metric OWNER TO minerva_admin;

GRANT ALL ON SCHEMA metric TO minerva_admin;
GRANT USAGE ON SCHEMA metric TO minerva;

SET search_path = metric, pg_catalog;
