CREATE SCHEMA trend;

COMMENT ON SCHEMA trend IS
'Stores information with fixed interval and format, like periodic measurements.';

ALTER SCHEMA trend OWNER TO minerva_admin;

GRANT ALL ON SCHEMA trend TO minerva_writer;
GRANT USAGE ON SCHEMA trend TO minerva;

SET search_path = trend, pg_catalog;

-- Type 'trend_descr'

CREATE TYPE trend_descr AS (
    name name,
    datatype varchar,
    description text
);

ALTER TYPE trend_descr OWNER TO minerva_admin;


-- Table 'trend'

CREATE TABLE trend (
    id integer not null,
    name varchar not null,
    description varchar not null
);

ALTER TABLE trend OWNER TO minerva_admin;

CREATE SEQUENCE trend_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE trend_id_seq OWNER TO minerva_admin;

ALTER TABLE trend
    ALTER COLUMN id
    SET DEFAULT nextval('trend_id_seq'::regclass);

ALTER SEQUENCE trend_id_seq OWNED BY trend.id;

ALTER TABLE ONLY trend
    ADD CONSTRAINT trend_pkey PRIMARY KEY (id);

GRANT ALL ON TABLE trend TO minerva_admin;
GRANT SELECT ON TABLE trend TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE trend TO minerva_writer;

GRANT ALL ON SEQUENCE trend_id_seq TO minerva_admin;
GRANT SELECT ON SEQUENCE trend_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE trend_id_seq TO minerva_writer;

CREATE TYPE storetype AS ENUM ('table', 'view');

-- Table 'trendstore'

CREATE TABLE trendstore (
    id integer not null,
    entitytype_id integer not null,
    datasource_id integer not null,
    granularity varchar not null,
    partition_size integer not null,
    type storetype not null DEFAULT 'table',
    version integer not null DEFAULT 4,
    retention_period interval not null DEFAULT interval '1 month'
);

ALTER TABLE trendstore OWNER TO minerva_admin;

CREATE SEQUENCE trendstore_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE trendstore_id_seq OWNER TO minerva_admin;

ALTER SEQUENCE trendstore_id_seq OWNED BY trendstore.id;

ALTER TABLE trendstore
    ALTER COLUMN id
    SET DEFAULT nextval('trendstore_id_seq'::regclass);

ALTER TABLE ONLY trendstore
    ADD CONSTRAINT trendstore_pkey PRIMARY KEY (id);

ALTER TABLE ONLY trendstore
    ADD CONSTRAINT trend_trendstore_entitytype_id_fkey
    FOREIGN KEY (entitytype_id) REFERENCES directory.entitytype(id)
    ON DELETE CASCADE;

ALTER TABLE ONLY trendstore
    ADD CONSTRAINT trend_trendstore_datasource_id_fkey
    FOREIGN KEY(datasource_id) REFERENCES directory.datasource(id);

CREATE UNIQUE INDEX ix_trend_trendstore_uniqueness
    ON trendstore (entitytype_id, datasource_id, granularity);

GRANT ALL ON TABLE trendstore TO minerva_admin;
GRANT SELECT ON TABLE trendstore TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE trendstore TO minerva_writer;

GRANT ALL ON SEQUENCE trendstore_id_seq TO minerva_admin;
GRANT SELECT ON SEQUENCE trendstore_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE trendstore_id_seq TO minerva_writer;

-- Table 'trendstore_trend_link'

CREATE TABLE trendstore_trend_link (
    trendstore_id integer NOT NULL,
    trend_id integer NOT NULL
);

ALTER TABLE trendstore_trend_link OWNER TO minerva_admin;

ALTER TABLE ONLY trendstore_trend_link
    ADD CONSTRAINT trend_trendstore_trend_link_pkey
    PRIMARY KEY (trendstore_id, trend_id);

ALTER TABLE ONLY trendstore_trend_link
    ADD CONSTRAINT trend_trendstore_trend_link_trendstore_id_fkey
    FOREIGN KEY (trendstore_id) REFERENCES trendstore(id)
    ON DELETE CASCADE;

ALTER TABLE ONLY trendstore_trend_link
    ADD CONSTRAINT trend_trendstore_trend_link_trend_id_fkey
    FOREIGN KEY (trend_id) REFERENCES trend(id)
    ON DELETE CASCADE;

GRANT ALL ON TABLE trendstore_trend_link TO minerva_admin;
GRANT SELECT ON TABLE trendstore_trend_link TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE trendstore_trend_link TO minerva_writer;

-- Table 'partition'

CREATE TABLE partition (
    table_name name not null,
    trendstore_id integer,
    data_start timestamp with time zone not null,
    data_end timestamp with time zone not null,
    version integer not null default 3
);

ALTER TABLE partition OWNER TO minerva_admin;

ALTER TABLE ONLY partition
    ADD CONSTRAINT trend_partition_pkey
    PRIMARY KEY (table_name);

ALTER TABLE ONLY partition
    ADD CONSTRAINT trend_partition_trendstore_id_fkey
    FOREIGN KEY (trendstore_id) REFERENCES trend.trendstore(id)
    ON DELETE CASCADE;

GRANT ALL ON TABLE partition TO minerva_admin;
GRANT SELECT ON TABLE partition TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE partition TO minerva_writer;

-- Table 'trend_tag_link'

CREATE TABLE trend_tag_link (
    trend_id integer NOT NULL,
    tag_id integer NOT NULL
);

ALTER TABLE trend_tag_link OWNER TO minerva_admin;

ALTER TABLE ONLY trend_tag_link
    ADD CONSTRAINT trend_tag_link_pkey
    PRIMARY KEY (trend_id, tag_id);

ALTER TABLE ONLY trend_tag_link
    ADD CONSTRAINT trend_tag_link_tag_id_fkey
    FOREIGN KEY (tag_id) REFERENCES directory.tag(id)
    ON DELETE CASCADE;

ALTER TABLE ONLY trend_tag_link
    ADD CONSTRAINT trend_tag_link_trend_id_fkey
    FOREIGN KEY (trend_id) REFERENCES trend(id)
    ON DELETE CASCADE;

GRANT ALL ON TABLE trend_tag_link TO minerva_admin;
GRANT SELECT ON TABLE trend_tag_link TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE trend_tag_link TO minerva_writer;

-- Table 'modified'

CREATE TABLE modified (
    "timestamp" timestamp WITH time zone NOT NULL,
    table_name varchar NOT NULL,
    start timestamp WITH time zone NOT NULL,
    "end" timestamp WITH time zone NOT NULL
);

ALTER TABLE modified OWNER TO minerva_admin;

ALTER TABLE ONLY modified
    ADD CONSTRAINT modified_pkey PRIMARY KEY ("timestamp", table_name);

ALTER TABLE ONLY modified
    ADD CONSTRAINT modified_table_name_fkey
    FOREIGN KEY (table_name) REFERENCES trend.partition(table_name)
    ON UPDATE CASCADE
    ON DELETE CASCADE;

GRANT ALL ON TABLE modified TO minerva_admin;
GRANT SELECT ON TABLE modified TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE modified TO minerva_writer;


-- Table 'view'

CREATE TABLE view (
    id integer not null,
    description varchar not null,
    trendstore_id integer not null,
    sql text not null
);

ALTER TABLE view OWNER TO minerva_admin;

CREATE SEQUENCE view_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE view_id_seq OWNER TO minerva_admin;

ALTER TABLE view
    ALTER COLUMN id
    SET DEFAULT nextval('view_id_seq'::regclass);

ALTER SEQUENCE view_id_seq OWNED BY view.id;

ALTER TABLE ONLY view
    ADD CONSTRAINT trend_view_pkey
    PRIMARY KEY (id);

ALTER TABLE ONLY view
    ADD CONSTRAINT trend_view_trendstore_id_fkey
    FOREIGN KEY (trendstore_id) REFERENCES trend.trendstore(id)
    ON DELETE CASCADE;

CREATE UNIQUE INDEX ix_trend_view_uniqueness
    ON view (trendstore_id);

GRANT ALL ON TABLE view TO minerva_admin;
GRANT SELECT ON TABLE view TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE view TO minerva_writer;

GRANT ALL ON SEQUENCE view_id_seq TO minerva_admin;
GRANT SELECT ON SEQUENCE view_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE view_id_seq TO minerva_writer;


-- Table 'view_trendstore_link'

CREATE TABLE view_trendstore_link (
    view_id integer not null,
    trendstore_id integer not null
);

ALTER TABLE view_trendstore_link OWNER TO minerva_admin;

ALTER TABLE ONLY view_trendstore_link
    ADD CONSTRAINT trend_view_trendstore_link_pkey
    PRIMARY KEY (view_id, trendstore_id);

ALTER TABLE ONLY view_trendstore_link
    ADD CONSTRAINT view_trendstore_link_view_id_fkey
    FOREIGN KEY (view_id) REFERENCES view(id)
    ON DELETE CASCADE;

ALTER TABLE ONLY view_trendstore_link
    ADD CONSTRAINT view_trendstore_link_trendstore_id_fkey
    FOREIGN KEY (trendstore_id) REFERENCES trendstore(id)
    ON DELETE CASCADE;

GRANT ALL ON TABLE view_trendstore_link TO minerva_admin;
GRANT SELECT ON TABLE view_trendstore_link TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE view_trendstore_link TO minerva_writer;


CREATE TABLE to_be_vacuumed (
    table_name name not null primary key
);

ALTER TABLE to_be_vacuumed OWNER TO minerva_admin;
GRANT SELECT ON TABLE to_be_vacuumed TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE to_be_vacuumed TO minerva_writer;

