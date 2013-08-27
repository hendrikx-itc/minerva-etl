SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

CREATE SCHEMA dimension;
ALTER SCHEMA dimension OWNER TO minerva_admin;

GRANT ALL ON SCHEMA dimension TO minerva_writer;
GRANT USAGE ON SCHEMA dimension TO minerva;

SET search_path = dimension, pg_catalog;

-- Table '"month"'

CREATE TABLE "month" (
	timestamp timestamp with time zone PRIMARY KEY,
	start timestamp with time zone,
	"end" timestamp with time zone
);

GRANT ALL ON TABLE "month" TO minerva_admin;
GRANT SELECT ON TABLE "month" TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE "month" TO minerva_writer;

-- Table 'week'

CREATE TABLE week (
	timestamp timestamp with time zone PRIMARY KEY,
	start timestamp with time zone,
	"end" timestamp with time zone
);

GRANT ALL ON TABLE week TO minerva_admin;
GRANT SELECT ON TABLE week TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE week TO minerva_writer;

-- Table '"day"'

CREATE TABLE "day" (
	timestamp timestamp with time zone PRIMARY KEY,
	start timestamp with time zone,
	"end" timestamp with time zone
);

GRANT ALL ON TABLE "day" TO minerva_admin;
GRANT SELECT ON TABLE "day" TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE "day" TO minerva_writer;

-- Table 'hour'

CREATE TABLE hour (
	timestamp timestamp with time zone PRIMARY KEY,
	start timestamp with time zone,
	"end" timestamp with time zone
);

GRANT ALL ON TABLE hour TO minerva_admin;
GRANT SELECT ON TABLE hour TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE hour TO minerva_writer;

-- Table 'quarter'

CREATE TABLE quarter (
	timestamp timestamp with time zone PRIMARY KEY,
	start timestamp with time zone,
	"end" timestamp with time zone
);

GRANT ALL ON TABLE quarter TO minerva_admin;
GRANT SELECT ON TABLE quarter TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE quarter TO minerva_writer;