SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = notification, pg_catalog;


CREATE CAST (notification.notificationstore AS text)
WITH FUNCTION notification.to_char(notification.notificationstore);
