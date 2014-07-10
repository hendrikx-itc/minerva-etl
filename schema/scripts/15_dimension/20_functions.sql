SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = dimension, pg_catalog;

CREATE OR REPLACE FUNCTION update_month()
    RETURNS void
AS $$
    TRUNCATE dimension.month;
    INSERT INTO dimension.month SELECT
        timestamp,
        timestamp - '1 month'::interval,
        timestamp
    FROM (
        SELECT generate_series(
            date_trunc('month', now() - '1 year'::interval),
            date_trunc('month', now() + '1 year'::interval),
            '1 month'::interval) AS timestamp
    ) timestamps;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION update_week()
    RETURNS void
AS $$
    TRUNCATE dimension.week;
    INSERT INTO dimension.week SELECT
        timestamp,
        timestamp - '1 week'::interval,
        timestamp
    FROM (
        SELECT generate_series(
            date_trunc('week', now() - '1 year'::interval),
            date_trunc('week', now() + '1 year'::interval),
            '1 week'::interval) AS timestamp
    ) timestamps;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION update_day()
    RETURNS void
AS $$
    TRUNCATE dimension.day;
    INSERT INTO dimension.day SELECT
        timestamp,
        timestamp - '1 day'::interval,
        timestamp
    FROM (
        SELECT generate_series(
            date_trunc('day', now()) - '1 year'::interval,
            date_trunc('day', now()) + '1 year'::interval,
            '1 day'::interval) AS timestamp
    ) timestamps;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION update_hour()
    RETURNS void
AS $$
    TRUNCATE dimension.hour;
    INSERT INTO dimension.hour SELECT
        timestamp,
        timestamp - '1 hour'::interval,
        timestamp
    FROM (
        SELECT generate_series(
            date_trunc('hour', now()) - '1 year'::interval,
            date_trunc('hour', now()) + '1 year'::interval,
            '1 hour'::interval) AS timestamp
    ) timestamps;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION update_quarter()
    RETURNS void
AS $$
    TRUNCATE dimension.quarter;
    INSERT INTO dimension.quarter SELECT
        timestamp,
        timestamp - '15 minute'::interval,
        timestamp
    FROM (
        SELECT generate_series(
            date_trunc('hour', now()) - '1 year'::interval,
            date_trunc('hour', now()) + '1 year'::interval,
            '15 minute'::interval) AS timestamp
    ) timestamps;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION update_5m()
    RETURNS void
AS $$
    TRUNCATE dimension."5m";
    INSERT INTO dimension."5m" SELECT
        timestamp,
        timestamp - '5 minute'::interval,
        timestamp
    FROM (
        SELECT generate_series(
            date_trunc('hour', now()) - '1 year'::interval,
            date_trunc('hour', now()) + '1 year'::interval,
            '5 minute'::interval) AS timestamp
    ) timestamps;
$$ LANGUAGE SQL;