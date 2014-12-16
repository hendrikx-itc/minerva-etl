CREATE SCHEMA system;
ALTER SCHEMA system OWNER TO minerva_admin;

GRANT ALL ON SCHEMA system TO minerva_admin;
GRANT ALL ON SCHEMA system TO minerva_writer;

SET search_path = system, pg_catalog;


CREATE TYPE job_state_enum AS ENUM (
    'queued',
    'running',
    'finished',
    'failed'
);

-- Table 'job_source'

CREATE TABLE job_source (
    id integer NOT NULL,
    name character varying(64) NOT NULL,
    job_type character varying(64) NOT NULL,
    config character varying
);

ALTER TABLE system.job_source OWNER TO minerva_admin;

CREATE SEQUENCE job_source_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE system.job_source_id_seq OWNER TO minerva_admin;

ALTER SEQUENCE job_source_id_seq OWNED BY job_source.id;

ALTER TABLE job_source
    ALTER COLUMN id
    SET DEFAULT nextval('job_source_id_seq'::regclass);

ALTER TABLE ONLY job_source
    ADD CONSTRAINT job_source_pkey PRIMARY KEY (id);

CREATE UNIQUE INDEX ix_system_job_source_name
    ON job_source USING btree (name);

GRANT ALL ON TABLE job_source TO minerva_admin;
GRANT SELECT ON TABLE job_source TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE job_source TO minerva_writer;

GRANT ALL ON SEQUENCE job_source_id_seq TO minerva_admin;
GRANT SELECT ON SEQUENCE job_source_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE job_source_id_seq TO minerva_writer;


-- Table 'job'

CREATE TABLE job (
    id integer NOT NULL,
    type character varying NOT NULL,
    description character varying NOT NULL,
    size bigint NOT NULL,
    created timestamp with time zone NOT NULL DEFAULT now(),
    started timestamp with time zone,
    finished timestamp with time zone,
    job_source_id integer NOT NULL,
    state job_state_enum NOT NULL DEFAULT 'queued'
);

ALTER TABLE system.job OWNER TO minerva_admin;

CREATE SEQUENCE job_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE system.job_id_seq OWNER TO minerva_admin;

ALTER SEQUENCE job_id_seq OWNED BY job.id;

ALTER TABLE job
    ALTER COLUMN id
    SET DEFAULT nextval('job_id_seq'::regclass);

ALTER TABLE ONLY job
    ADD CONSTRAINT job_pkey PRIMARY KEY (id);

ALTER TABLE ONLY job
    ADD CONSTRAINT job_job_source_id_fkey
    FOREIGN KEY (job_source_id) REFERENCES job_source(id)
    ON DELETE CASCADE;

GRANT ALL ON TABLE job TO minerva_admin;
GRANT SELECT ON TABLE job TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE job TO minerva_writer;

GRANT ALL ON SEQUENCE job_id_seq TO minerva_admin;
GRANT SELECT ON SEQUENCE job_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE job_id_seq TO minerva_writer;

-- Table 'job_error_log'

CREATE TABLE job_error_log (
    job_id integer NOT NULL,
    message character varying
);

ALTER TABLE system.job_error_log OWNER TO minerva_admin;

ALTER TABLE ONLY job_error_log
    ADD CONSTRAINT job_error_log_pkey PRIMARY KEY (job_id);

GRANT ALL ON TABLE job_error_log TO minerva_admin;
GRANT SELECT ON TABLE job_error_log TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE job_error_log TO minerva_writer;

-- Table 'job_queue'

CREATE TABLE job_queue (
    job_id integer NOT NULL
);

ALTER TABLE system.job_queue OWNER TO minerva_admin;

ALTER TABLE ONLY job_queue
    ADD CONSTRAINT job_queue_pkey PRIMARY KEY (job_id);

ALTER TABLE ONLY job_queue
    ADD CONSTRAINT job_queue_job_id_fkey
    FOREIGN KEY (job_id) REFERENCES job(id)
    ON DELETE CASCADE;

GRANT ALL ON TABLE job_queue TO minerva_admin;
GRANT SELECT ON TABLE job_queue TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE job_queue TO minerva_writer;

-- Table 'setting'

CREATE TABLE setting (
    id integer NOT NULL,
    name text NOT NULL,
    value text
);

ALTER TABLE system.setting OWNER TO minerva_admin;

CREATE SEQUENCE setting_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE system.setting_id_seq OWNER TO minerva_admin;

ALTER SEQUENCE setting_id_seq OWNED BY setting.id;

ALTER TABLE setting
    ALTER COLUMN id
    SET DEFAULT nextval('setting_id_seq'::regclass);

ALTER TABLE ONLY setting
    ADD CONSTRAINT setting_pkey PRIMARY KEY (id);

GRANT ALL ON TABLE setting TO minerva_admin;
GRANT SELECT ON TABLE setting TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE setting TO minerva_writer;
