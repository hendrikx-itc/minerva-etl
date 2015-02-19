-- Table 'type'

CREATE TABLE entity_tag.type (
    id serial,
    name name UNIQUE,
    tag_group_id integer REFERENCES directory.tag_group(id) ON DELETE CASCADE
);

ALTER TABLE entity_tag.type OWNER TO minerva_admin;


-- Table 'entity_tag_link_staging'

CREATE UNLOGGED TABLE entity_tag.entity_tag_link_staging (
    entity_id integer NOT NULL,
    tag_name text NOT NULL,
    tag_group_id integer NOT NULL
);

ALTER TABLE entity_tag.entity_tag_link_staging OWNER TO minerva_admin;

GRANT SELECT ON TABLE entity_tag.entity_tag_link_staging TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE entity_tag.entity_tag_link_staging TO minerva_writer;
