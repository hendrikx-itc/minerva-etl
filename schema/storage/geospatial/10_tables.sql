SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;


--=============
-- Schema: gis
--=============

CREATE SCHEMA gis;

ALTER SCHEMA gis OWNER TO minerva_admin;
GRANT ALL ON SCHEMA gis TO minerva_admin;
GRANT USAGE ON SCHEMA gis TO minerva;
GRANT ALL ON SCHEMA gis TO minerva_writer;

SET search_path = gis, public, pg_catalog;

-- Create gis.site tables
CREATE TABLE gis.site (
    entity_id integer NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    hash character varying,
    CONSTRAINT site_pkey PRIMARY KEY (entity_id, timestamp)
) WITHOUT OIDS;

SELECT AddGeometryColumn('minerva', 'gis', 'site', 'position', 900913, 'POINT', 2);

ALTER TABLE gis.site OWNER TO minerva_admin;
GRANT ALL ON TABLE gis.site TO minerva_admin;
GRANT SELECT ON TABLE gis.site TO minerva;
GRANT INSERT, DELETE, UPDATE ON TABLE gis.site TO minerva_writer;

CREATE TABLE gis.site_curr (
    CONSTRAINT site_curr_pkey PRIMARY KEY (entity_id)
) INHERITS (gis.site);

ALTER TABLE gis.site_curr OWNER TO minerva_admin;
GRANT ALL ON TABLE gis.site_curr TO minerva_admin;
GRANT SELECT ON TABLE gis.site_curr TO minerva;
GRANT INSERT, DELETE, UPDATE ON TABLE gis.site_curr TO minerva_writer;

-- Create gis.cell tables
CREATE TABLE gis.cell (
    entity_id integer NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    hash character varying,
    azimuth integer NOT NULL,
    "type" character varying,
    CONSTRAINT cell_pkey PRIMARY KEY (entity_id, timestamp)
) WITHOUT OIDS;

ALTER TABLE gis.cell OWNER TO minerva_admin;
GRANT ALL ON TABLE gis.cell TO minerva_admin;
GRANT SELECT ON TABLE gis.cell TO minerva;
GRANT INSERT, DELETE, UPDATE ON TABLE gis.cell TO minerva_writer;

CREATE TABLE gis.cell_curr (
    CONSTRAINT cell_curr_pkey PRIMARY KEY (entity_id)
) INHERITS (gis.cell);

ALTER TABLE gis.cell_curr OWNER TO minerva_admin;
GRANT ALL ON TABLE gis.cell_curr TO minerva_admin;
GRANT SELECT ON TABLE gis.cell_curr TO minerva;
GRANT INSERT, DELETE, UPDATE ON TABLE gis.cell_curr TO minerva_writer;
