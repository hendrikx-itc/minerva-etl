CREATE SCHEMA notification;

COMMENT ON SCHEMA notification IS
'Stores information of events that can occur at irregular intervals, but '
'still have a fixed, known format.';

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

COMMENT ON TABLE notificationstore IS
'Describes notificationstores. Each notificationstore maps to a set of tables '
'and functions that can store and manage notifications of a certain type. '
'These corresponding tables and functions are created automatically for each '
'notificationstore. Because each notificationstore maps one-on-one to a '
'datasource, the name of the notificationstore is the same as that of the '
'datasource. Use the create_notificationstore function to create new '
'notificationstores.';

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

COMMENT ON TABLE attribute IS
'Describes attributes of notificationstores. An attribute of a '
'notificationstore is an attribute that each notification stored in that '
'notificationstore has. An attribute corresponds directly to a column in '
'the main notificationstore table';

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

COMMENT ON TABLE notificationsetstore IS
'Describes notificationsetstores. A notificationsetstore can hold information '
'over sets of notifications that are related to each other.';

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

COMMENT ON TABLE setattribute IS
'Describes attributes of notificationsetstores. A setattribute of a '
'notificationsetstore is an attribute that each notification set has. '
'A setattribute corresponds directly to a column in the main '
'notificationsetstore table.';

ALTER TABLE attribute OWNER TO minerva_admin;

GRANT ALL ON TABLE attribute TO minerva_admin;
GRANT SELECT ON TABLE attribute TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE attribute TO minerva_writer;
