SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = dimension, pg_catalog;

SELECT update_month();
SELECT update_week();
SELECT update_day();
SELECT update_hour();
SELECT update_quarter();
SELECT update_four_consec_qtr();
SELECT update_month_15m();
SELECT update_week_15m();
