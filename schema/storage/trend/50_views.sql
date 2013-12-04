SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = trend, pg_catalog;

CREATE VIEW view_dependencies AS
	SELECT dependent.relname AS src, pg_attribute.attname column_name, dependee.relname AS dst
	FROM pg_depend
	JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
	JOIN pg_class as dependee ON pg_rewrite.ev_class = dependee.oid
	JOIN pg_class as dependent ON pg_depend.refobjid = dependent.oid
	JOIN pg_namespace as n ON dependent.relnamespace = n.oid
	JOIN pg_attribute ON
			pg_depend.refobjid = pg_attribute.attrelid
			AND
			pg_depend.refobjsubid = pg_attribute.attnum
	WHERE n.nspname = 'trend' AND pg_attribute.attnum > 0;

ALTER VIEW view_dependencies OWNER TO minerva_admin;

GRANT SELECT ON TABLE view_dependencies TO minerva;