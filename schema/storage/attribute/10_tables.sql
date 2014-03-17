SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = attribute_directory, pg_catalog;

-- Table 'attributestore'

CREATE TABLE attributestore (
    id integer not null,
    datasource_id integer not null,
    entitytype_id integer not null
);

ALTER TABLE attributestore OWNER TO minerva_admin;

CREATE SEQUENCE attributestore_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE attributestore_id_seq OWNER TO minerva_admin;

ALTER TABLE attributestore ALTER COLUMN id SET DEFAULT nextval('attributestore_id_seq'::regclass);

ALTER SEQUENCE attributestore_id_seq OWNED BY attributestore.id;

ALTER TABLE ONLY attributestore
    ADD CONSTRAINT attributestore_pkey PRIMARY KEY (id);

ALTER TABLE ONLY attributestore
    ADD CONSTRAINT attribute_attributestore_entitytype_id_fkey
    FOREIGN KEY (entitytype_id) REFERENCES directory.entitytype(id)
    ON DELETE CASCADE;

ALTER TABLE ONLY attributestore
    ADD CONSTRAINT attribute_attributestore_datasource_id_fkey
    FOREIGN KEY(datasource_id) REFERENCES directory.datasource(id);

GRANT ALL ON TABLE attributestore TO minerva_admin;
GRANT SELECT ON TABLE attributestore TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE attributestore TO minerva_writer;

GRANT ALL ON SEQUENCE attributestore_id_seq TO minerva_admin;
GRANT SELECT ON SEQUENCE attributestore_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE attributestore_id_seq TO minerva_writer;

-- Table 'attribute'

CREATE TABLE attribute (
    id integer not null,
    attributestore_id integer not null,
    description text,
    name name not null,
    datatype varchar not null
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

ALTER TABLE attribute
    ADD CONSTRAINT attribute_uniqueness UNIQUE (attributestore_id, name);

ALTER TABLE ONLY attribute
    ADD CONSTRAINT attribute_attribute_attributestore_id_fkey
    FOREIGN KEY(attributestore_id) REFERENCES attribute_directory.attributestore(id)
    ON DELETE CASCADE;

GRANT ALL ON TABLE attribute TO minerva_admin;
GRANT SELECT ON TABLE attribute TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE attribute TO minerva_writer;

GRANT ALL ON SEQUENCE attribute_id_seq TO minerva_admin;
GRANT SELECT ON SEQUENCE attribute_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE attribute_id_seq TO minerva_writer;

-- Table 'attribute_tag_link'

CREATE TABLE attribute_tag_link (
    attribute_id integer NOT NULL,
    tag_id integer NOT NULL
);

ALTER TABLE attribute_tag_link OWNER TO minerva_admin;

ALTER TABLE ONLY attribute_tag_link
    ADD CONSTRAINT attribute_tag_link_pkey PRIMARY KEY (attribute_id, tag_id);

ALTER TABLE ONLY attribute_tag_link
    ADD CONSTRAINT attribute_tag_link_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES directory.tag(id)
    ON DELETE CASCADE;

ALTER TABLE ONLY attribute_tag_link
    ADD CONSTRAINT attribute_tag_link_attribute_id_fkey FOREIGN KEY (attribute_id) REFERENCES attribute(id)
    ON DELETE CASCADE;

GRANT ALL ON TABLE attribute_tag_link TO minerva_admin;
GRANT SELECT ON TABLE attribute_tag_link TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE attribute_tag_link TO minerva_writer;

-- Table 'attributestore_modified'

CREATE TABLE attributestore_modified (
    attributestore_id integer NOT NULL,
    modified timestamp with time zone NOT NULL
);

ALTER TABLE attributestore_modified OWNER TO minerva_admin;

ALTER TABLE ONLY attributestore_modified
    ADD CONSTRAINT attributestore_modified_pkey PRIMARY KEY (attributestore_id);

ALTER TABLE ONLY attributestore_modified
    ADD CONSTRAINT attributestore_modified_attributestore_id_fkey FOREIGN KEY (attributestore_id) REFERENCES attribute_directory.attributestore(id)
    ON DELETE CASCADE;

GRANT ALL ON TABLE attributestore_modified TO minerva_admin;
GRANT SELECT ON TABLE attributestore_modified TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE attributestore_modified TO minerva_writer;

-- Table 'attributestore_curr_materialized'

CREATE TABLE attributestore_curr_materialized (
    attributestore_id integer NOT NULL,
    materialized timestamp with time zone NOT NULL
);

ALTER TABLE attributestore_curr_materialized OWNER TO minerva_admin;

ALTER TABLE ONLY attributestore_curr_materialized
    ADD CONSTRAINT attributestore_curr_materialized_pkey PRIMARY KEY (attributestore_id);

ALTER TABLE ONLY attributestore_curr_materialized
    ADD CONSTRAINT attributestore_curr_materialized_attributestore_id_fkey FOREIGN KEY (attributestore_id) REFERENCES attribute_directory.attributestore(id)
    ON DELETE CASCADE;

GRANT ALL ON TABLE attributestore_curr_materialized TO minerva_admin;
GRANT SELECT ON TABLE attributestore_curr_materialized TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE attributestore_curr_materialized TO minerva_writer;

-- Table 'attributestore_compacted'

CREATE TABLE attributestore_compacted (
    attributestore_id integer NOT NULL,
    compacted timestamp with time zone NOT NULL
);

ALTER TABLE attributestore_compacted OWNER TO minerva_admin;

ALTER TABLE ONLY attributestore_compacted
    ADD CONSTRAINT attributestore_compacted_pkey PRIMARY KEY (attributestore_id);

ALTER TABLE ONLY attributestore_compacted
    ADD CONSTRAINT attributestore_compacted_attributestore_id_fkey FOREIGN KEY (attributestore_id) REFERENCES attribute_directory.attributestore(id)
    ON DELETE CASCADE;

GRANT ALL ON TABLE attributestore_compacted TO minerva_admin;
GRANT SELECT ON TABLE attributestore_compacted TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE attributestore_compacted TO minerva_writer;

