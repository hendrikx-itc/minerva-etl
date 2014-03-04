SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

CREATE SCHEMA notification;
ALTER SCHEMA notification OWNER TO minerva_admin;

GRANT ALL ON SCHEMA notification TO minerva_writer;
GRANT USAGE ON SCHEMA notification TO minerva;

SET search_path = notification, pg_catalog;

CREATE TYPE attr_def AS (name name, data_type name);


-- Table 'notificationstore'

CREATE TABLE notificationstore (
	id serial PRIMARY KEY,
	datasource_id integer REFERENCES directory.datasource ON DELETE CASCADE,
	version integer not null,
	CONSTRAINT uniqueness UNIQUE(datasource_id)
);

ALTER TABLE notificationstore OWNER TO minerva_admin;

GRANT ALL ON TABLE notificationstore TO minerva_admin;
GRANT SELECT ON TABLE notificationstore TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE notificationstore TO minerva_writer;


-- Table 'attribute'

CREATE TABLE attribute (
	id serial PRIMARY KEY,
	notificationstore_id integer REFERENCES notification.notificationstore ON DELETE CASCADE,
	name name not null,
	data_type name not null,
	description varchar not null
);

ALTER TABLE attribute OWNER TO minerva_admin;

GRANT ALL ON TABLE attribute TO minerva_admin;
GRANT SELECT ON TABLE attribute TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE attribute TO minerva_writer;


-- Table 'notificationsetstore'

CREATE TABLE notificationsetstore (
	id serial PRIMARY KEY,
	name name not null,
	notificationstore_id integer REFERENCES notification.notificationstore ON DELETE CASCADE
);

ALTER TABLE notificationsetstore OWNER TO minerva_admin;

GRANT ALL ON TABLE notificationsetstore TO minerva_admin;
GRANT SELECT ON TABLE notificationsetstore TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE notificationsetstore TO minerva_writer;


-- Table 'setattribute'

CREATE TABLE setattribute (
	id serial PRIMARY KEY,
	notificationsetstore_id integer REFERENCES notification.notificationsetstore ON DELETE CASCADE,
	name name not null,
	data_type name not null,
	description varchar not null
);

ALTER TABLE attribute OWNER TO minerva_admin;

GRANT ALL ON TABLE attribute TO minerva_admin;
GRANT SELECT ON TABLE attribute TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE attribute TO minerva_writer;
