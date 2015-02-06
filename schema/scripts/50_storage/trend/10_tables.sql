CREATE SCHEMA trend;

COMMENT ON SCHEMA trend IS
'Stores information with fixed interval and format, like periodic measurements.';

ALTER SCHEMA trend OWNER TO minerva_admin;

GRANT ALL ON SCHEMA trend TO minerva_writer;
GRANT USAGE ON SCHEMA trend TO minerva;


CREATE SCHEMA trend_partition;

COMMENT ON SCHEMA trend_partition IS
'Stores information with fixed interval and format, like periodic measurements.';

ALTER SCHEMA trend_partition OWNER TO minerva_admin;

GRANT ALL ON SCHEMA trend_partition TO minerva_writer;
GRANT USAGE ON SCHEMA trend_partition TO minerva;


CREATE SCHEMA trend_directory;

COMMENT ON SCHEMA trend_directory IS
'Stores information with fixed interval and format, like periodic measurements.';

ALTER SCHEMA trend_directory OWNER TO minerva_admin;

GRANT ALL ON SCHEMA trend_directory TO minerva_writer;
GRANT USAGE ON SCHEMA trend_directory TO minerva;

-- Type 'trend_directory.trend_descr'

CREATE TYPE trend_directory.trend_descr AS (
    name name,
    data_type text,
    description text
);

ALTER TYPE trend_directory.trend_descr OWNER TO minerva_admin;


-- Type 'trend_directory.storetype'

CREATE TYPE trend_directory.storetype AS ENUM ('table', 'view');

-- Table 'trend_directory.trendstore'

CREATE TABLE trend_directory.trendstore (
    id integer not null,
    entitytype_id integer not null,
    datasource_id integer not null,
    granularity varchar not null,
    partition_size integer not null,
    type trend_directory.storetype not null DEFAULT 'table',
    retention_period interval not null DEFAULT interval '1 month'
);

ALTER TABLE trend_directory.trendstore OWNER TO minerva_admin;

CREATE SEQUENCE trend_directory.trendstore_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE trend_directory.trendstore_id_seq OWNER TO minerva_admin;

ALTER SEQUENCE trend_directory.trendstore_id_seq OWNED BY trend_directory.trendstore.id;

ALTER TABLE trend_directory.trendstore
    ALTER COLUMN id
    SET DEFAULT nextval('trend_directory.trendstore_id_seq'::regclass);

ALTER TABLE ONLY trend_directory.trendstore
    ADD CONSTRAINT trendstore_pkey PRIMARY KEY (id);

ALTER TABLE ONLY trend_directory.trendstore
    ADD CONSTRAINT trend_trendstore_entitytype_id_fkey
    FOREIGN KEY (entitytype_id) REFERENCES directory.entitytype(id)
    ON DELETE CASCADE;

ALTER TABLE ONLY trend_directory.trendstore
    ADD CONSTRAINT trend_trendstore_datasource_id_fkey
    FOREIGN KEY(datasource_id) REFERENCES directory.datasource(id);

CREATE UNIQUE INDEX ix_trend_trendstore_uniqueness
    ON trend_directory.trendstore (entitytype_id, datasource_id, granularity);

GRANT SELECT ON TABLE trend_directory.trendstore TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE trend_directory.trendstore TO minerva_writer;

GRANT SELECT ON SEQUENCE trend_directory.trendstore_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE trend_directory.trendstore_id_seq TO minerva_writer;


-- Table 'trend_directory.trend'

CREATE TABLE trend_directory.trend (
    id integer NOT NULL,
    name name NOT NULL,
    data_type text NOT NULL,
    trendstore_id integer NOT NULL REFERENCES trend_directory.trendstore(id) ON DELETE CASCADE,
    description text NOT NULL
);

ALTER TABLE trend_directory.trend OWNER TO minerva_admin;

CREATE SEQUENCE trend_directory.trend_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE trend_directory.trend_id_seq OWNER TO minerva_admin;

ALTER TABLE trend_directory.trend
    ALTER COLUMN id
    SET DEFAULT nextval('trend_directory.trend_id_seq'::regclass);

ALTER SEQUENCE trend_directory.trend_id_seq OWNED BY trend_directory.trend.id;

ALTER TABLE ONLY trend_directory.trend
    ADD CONSTRAINT trend_pkey PRIMARY KEY (id);

GRANT SELECT ON TABLE trend_directory.trend TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE trend_directory.trend TO minerva_writer;

GRANT SELECT ON SEQUENCE trend_directory.trend_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE trend_directory.trend_id_seq TO minerva_writer;


-- View 'trend_directory.trendstore_trend_link'
-- This view is for backward compatibility

CREATE VIEW trend_directory.trendstore_trend_link AS
SELECT
    trendstore_id,
    id AS trend_id
FROM
    trend_directory.trend;

ALTER VIEW trend_directory.trendstore_trend_link OWNER TO minerva_admin;

-- Table 'trend_directory.partition'

CREATE TABLE trend_directory.partition (
    trendstore_id integer,
    index integer
);

ALTER TABLE trend_directory.partition OWNER TO minerva_admin;

ALTER TABLE ONLY trend_directory.partition
    ADD CONSTRAINT trend_partition_pkey
    PRIMARY KEY (trendstore_id, index);

ALTER TABLE ONLY trend_directory.partition
    ADD CONSTRAINT trend_partition_trendstore_id_fkey
    FOREIGN KEY (trendstore_id) REFERENCES trend_directory.trendstore(id)
    ON DELETE CASCADE;

GRANT SELECT ON TABLE trend_directory.partition TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE trend_directory.partition TO minerva_writer;

-- Table 'trend_directory.trend_tag_link'

CREATE TABLE trend_directory.trend_tag_link (
    trend_id integer NOT NULL,
    tag_id integer NOT NULL
);

ALTER TABLE trend_directory.trend_tag_link OWNER TO minerva_admin;

ALTER TABLE ONLY trend_directory.trend_tag_link
    ADD CONSTRAINT trend_tag_link_pkey
    PRIMARY KEY (trend_id, tag_id);

ALTER TABLE ONLY trend_directory.trend_tag_link
    ADD CONSTRAINT trend_tag_link_tag_id_fkey
    FOREIGN KEY (tag_id) REFERENCES directory.tag(id)
    ON DELETE CASCADE;

ALTER TABLE ONLY trend_directory.trend_tag_link
    ADD CONSTRAINT trend_tag_link_trend_id_fkey
    FOREIGN KEY (trend_id) REFERENCES trend_directory.trend(id)
    ON DELETE CASCADE;

GRANT SELECT ON TABLE trend_directory.trend_tag_link TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE trend_directory.trend_tag_link TO minerva_writer;

-- Table 'trend_directory.modified'

CREATE TABLE trend_directory.modified (
    trendstore_id integer not null REFERENCES trend_directory.trendstore ON UPDATE CASCADE ON DELETE CASCADE,
    "timestamp" timestamp WITH time zone NOT NULL,
    start timestamp WITH time zone NOT NULL,
    "end" timestamp WITH time zone NOT NULL,
    PRIMARY KEY (trendstore_id, "timestamp")
);

ALTER TABLE trend_directory.modified OWNER TO minerva_admin;

GRANT SELECT ON TABLE trend_directory.modified TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE trend_directory.modified TO minerva_writer;


-- Table 'trend_directory.view'

CREATE TABLE trend_directory.view (
    id integer not null,
    description varchar not null,
    trendstore_id integer not null,
    sql text not null
);

ALTER TABLE trend_directory.view OWNER TO minerva_admin;

CREATE SEQUENCE trend_directory.view_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE trend_directory.view_id_seq OWNER TO minerva_admin;

ALTER TABLE trend_directory.view
    ALTER COLUMN id
    SET DEFAULT nextval('trend_directory.view_id_seq'::regclass);

ALTER SEQUENCE trend_directory.view_id_seq OWNED BY trend_directory.view.id;

ALTER TABLE ONLY trend_directory.view
    ADD CONSTRAINT trend_view_pkey
    PRIMARY KEY (id);

ALTER TABLE ONLY trend_directory.view
    ADD CONSTRAINT trend_view_trendstore_id_fkey
    FOREIGN KEY (trendstore_id) REFERENCES trend_directory.trendstore(id)
    ON DELETE CASCADE;

CREATE UNIQUE INDEX ix_trend_view_uniqueness
    ON trend_directory.view (trendstore_id);

GRANT SELECT ON TABLE trend_directory.view TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE trend_directory.view TO minerva_writer;

GRANT SELECT ON SEQUENCE trend_directory.view_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE trend_directory.view_id_seq TO minerva_writer;


-- Table 'trend_directory.view_trendstore_link'

CREATE TABLE trend_directory.view_trendstore_link (
    view_id integer not null,
    trendstore_id integer not null
);

ALTER TABLE trend_directory.view_trendstore_link OWNER TO minerva_admin;

ALTER TABLE ONLY trend_directory.view_trendstore_link
    ADD CONSTRAINT trend_view_trendstore_link_pkey
    PRIMARY KEY (view_id, trendstore_id);

ALTER TABLE ONLY trend_directory.view_trendstore_link
    ADD CONSTRAINT view_trendstore_link_view_id_fkey
    FOREIGN KEY (view_id) REFERENCES trend_directory.view(id)
    ON DELETE CASCADE;

ALTER TABLE ONLY trend_directory.view_trendstore_link
    ADD CONSTRAINT view_trendstore_link_trendstore_id_fkey
    FOREIGN KEY (trendstore_id) REFERENCES trend_directory.trendstore(id)
    ON DELETE CASCADE;

GRANT SELECT ON TABLE trend_directory.view_trendstore_link TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE trend_directory.view_trendstore_link TO minerva_writer;
