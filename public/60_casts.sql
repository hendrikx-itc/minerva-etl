SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = public, pg_catalog;


CREATE CAST (integer AS integer[])
	WITH FUNCTION integer_to_array(integer);


CREATE CAST (smallint AS smallint[])
	WITH FUNCTION smallint_to_array(smallint);


CREATE CAST (smallint as timestamp without time zone)
	WITH FUNCTION smallint_to_timestamp_without_time_zone (smallint);


CREATE CAST (smallint as timestamp with time zone)
	WITH FUNCTION smallint_to_timestamp_with_time_zone (smallint);
