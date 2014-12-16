CREATE SCHEMA relation;

COMMENT ON SCHEMA relation IS
'Stores directional relations between entities.';

ALTER SCHEMA relation OWNER TO minerva_admin;

GRANT ALL ON SCHEMA relation TO minerva_admin;
GRANT ALL ON SCHEMA relation TO minerva_writer;
GRANT USAGE ON SCHEMA relation TO minerva;

CREATE SCHEMA relation_def;
ALTER SCHEMA relation_def OWNER TO minerva_admin;

GRANT ALL ON SCHEMA relation_def TO minerva_admin;
GRANT ALL ON SCHEMA relation_def TO minerva_writer;
GRANT USAGE ON SCHEMA relation_def TO minerva;

SET search_path = relation, pg_catalog;


-- Table 'group'

CREATE TABLE "group" (
    id integer NOT NULL,
    name character varying NOT NULL
);

ALTER TABLE "group" OWNER TO minerva_admin;

ALTER TABLE ONLY "group"
    ADD CONSTRAINT group_pkey PRIMARY KEY (id);

GRANT ALL ON TABLE "group" TO minerva_admin;
GRANT SELECT ON TABLE "group" TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE "group" TO minerva_writer;

CREATE SEQUENCE group_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE group_id_seq OWNER TO minerva_admin;

ALTER SEQUENCE group_id_seq OWNED BY "group".id;

ALTER TABLE "group"
    ALTER COLUMN id
    SET DEFAULT nextval('group_id_seq'::regclass);

GRANT ALL ON SEQUENCE group_id_seq TO minerva_admin;
GRANT SELECT ON SEQUENCE group_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE group_id_seq TO minerva_writer;

CREATE UNIQUE INDEX ix_group_name ON "group" (name);

-- Table 'type'

CREATE TYPE type_cardinality_enum AS ENUM (
    'one-to-one',
    'one-to-many',
    'many-to-one'
);

CREATE TABLE "type" (
    id integer NOT NULL,
    name character varying NOT NULL,
    cardinality type_cardinality_enum DEFAULT NULL,
    group_id integer DEFAULT NULL
);

ALTER TABLE "type" OWNER TO minerva_admin;

ALTER TABLE ONLY "type"
    ADD CONSTRAINT type_pkey PRIMARY KEY (id);

GRANT ALL ON TABLE "type" TO minerva_admin;
GRANT SELECT ON TABLE "type" TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE "type" TO minerva_writer;

CREATE SEQUENCE type_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE type_id_seq OWNER TO minerva_admin;

ALTER SEQUENCE type_id_seq OWNED BY "type".id;

ALTER TABLE "type"
    ALTER COLUMN id
    SET DEFAULT nextval('type_id_seq'::regclass);

ALTER TABLE ONLY "type"
    ADD CONSTRAINT group_id_fkey
    FOREIGN KEY (group_id) REFERENCES "group"(id)
    ON DELETE CASCADE;

GRANT ALL ON SEQUENCE type_id_seq TO minerva_admin;
GRANT SELECT ON SEQUENCE type_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE type_id_seq TO minerva_writer;

CREATE UNIQUE INDEX ix_type_name ON "type" (name);

-- Table 'all'

CREATE TABLE "all" (
    source_id integer NOT NULL,
    target_id integer NOT NULL,
    type_id integer NOT NULL
);

ALTER TABLE "all" OWNER TO minerva_admin;

ALTER TABLE ONLY "all"
    ADD PRIMARY KEY (source_id, target_id);

ALTER TABLE ONLY "all"
    ADD CONSTRAINT source_id_fkey
    FOREIGN KEY (source_id) REFERENCES directory.entity(id)
    ON DELETE CASCADE;

ALTER TABLE ONLY "all"
    ADD CONSTRAINT target_id_fkey
    FOREIGN KEY (target_id) REFERENCES directory.entity(id)
    ON DELETE CASCADE;

GRANT ALL ON TABLE "all" TO minerva_admin;
GRANT SELECT ON TABLE "all" TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE "all" TO minerva_writer;

ALTER TABLE ONLY "all"
    ADD CONSTRAINT type_id_fkey
    FOREIGN KEY (type_id) REFERENCES "type"(id)
    ON DELETE CASCADE;

CREATE INDEX ON "all" USING btree (target_id);
CREATE INDEX ON "all" USING btree (type_id);

-- Table 'all_materialized'

CREATE TABLE all_materialized (
    source_id integer NOT NULL,
    target_id integer NOT NULL,
    type_id integer NOT NULL
);

ALTER TABLE "all_materialized" OWNER TO minerva_admin;

ALTER TABLE "all_materialized"
    ADD PRIMARY KEY (source_id, target_id, type_id);

GRANT ALL ON TABLE "all_materialized" TO minerva_admin;
GRANT SELECT ON TABLE "all_materialized" TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE "all_materialized" TO minerva_writer;

CREATE INDEX ON "all_materialized" USING btree (target_id);
CREATE INDEX ON "all_materialized" USING btree (type_id);

