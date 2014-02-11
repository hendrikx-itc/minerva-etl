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


-- Table 'notificationstore'

CREATE TABLE notificationstore (
	id integer not null,
	datasource_id integer not null,
	version integer not null
);

ALTER TABLE notificationstore OWNER TO minerva_admin;

CREATE SEQUENCE notificationstore_id_seq
	START WITH 1
	INCREMENT BY 1
	NO MINVALUE
	NO MAXVALUE
	CACHE 1;

ALTER TABLE notificationstore_id_seq OWNER TO minerva_admin;

ALTER SEQUENCE notificationstore_id_seq OWNED BY notificationstore.id;

ALTER TABLE notificationstore
	ALTER COLUMN id
	SET DEFAULT nextval('notificationstore_id_seq'::regclass);

ALTER TABLE ONLY notificationstore
	ADD CONSTRAINT notificationstore_pkey PRIMARY KEY (id);

ALTER TABLE ONLY notificationstore
	ADD CONSTRAINT notification_notificationstore_datasource_id_fkey
	FOREIGN KEY(datasource_id) REFERENCES directory.datasource(id)
	ON DELETE CASCADE;

CREATE UNIQUE INDEX ix_trend_notificationstore_uniqueness
	ON notificationstore (datasource_id);

GRANT ALL ON TABLE notificationstore TO minerva_admin;
GRANT SELECT ON TABLE notificationstore TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE notificationstore TO minerva_writer;

GRANT ALL ON SEQUENCE notificationstore_id_seq TO minerva_admin;
GRANT SELECT ON SEQUENCE notificationstore_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE notificationstore_id_seq TO minerva_writer;


-- Table 'attribute'

CREATE TABLE attribute (
	id integer not null,
	notificationstore_id integer not null,
	name varchar not null,
	data_type varchar not null,
	description varchar not null
);

ALTER TABLE attribute OWNER TO minerva_admin;

CREATE SEQUENCE attribute_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE attribute_id_seq OWNER TO minerva_admin;

ALTER TABLE attribute ALTER COLUMN id SET DEFAULT nextval('attribute_id_seq'::regclass);

ALTER SEQUENCE attribute_id_seq OWNED BY attribute.id;

ALTER TABLE ONLY attribute
	ADD CONSTRAINT attribute_pkey PRIMARY KEY (id);

ALTER TABLE ONLY attribute
	ADD CONSTRAINT notification_attribute_notificationstore_id_fkey
	FOREIGN KEY(notificationstore_id) REFERENCES notification.notificationstore(id)
	ON DELETE CASCADE;

GRANT ALL ON TABLE attribute TO minerva_admin;
GRANT SELECT ON TABLE attribute TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE attribute TO minerva_writer;

GRANT ALL ON SEQUENCE attribute_id_seq TO minerva_admin;
GRANT SELECT ON SEQUENCE attribute_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE attribute_id_seq TO minerva_writer;

