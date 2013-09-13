SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = attribute, pg_catalog;


CREATE CAST (attribute.attributestore AS text) WITH FUNCTION attribute.to_char(attribute.attributestore);
