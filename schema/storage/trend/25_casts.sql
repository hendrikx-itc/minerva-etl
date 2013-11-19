SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = trend, pg_catalog;


CREATE CAST (trend.trendstore AS text) WITH FUNCTION trend.to_char(trend.trendstore) AS IMPLICIT;

CREATE CAST (trend.view AS text) WITH FUNCTION trend.to_char(trend.view) AS IMPLICIT;
