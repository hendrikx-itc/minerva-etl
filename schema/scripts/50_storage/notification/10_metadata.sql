CREATE SCHEMA notification;

COMMENT ON SCHEMA notification IS
'Stores information of events that can occur at irregular intervals, but '
'still have a fixed, known format.';

ALTER SCHEMA notification OWNER TO minerva_admin;

GRANT ALL ON SCHEMA notification TO minerva_writer;
GRANT USAGE ON SCHEMA notification TO minerva;

CREATE TYPE notification.attr_def AS (
    name name,
    data_type name,
    description text
);


-- Table 'notification.notification_store'

CREATE TABLE notification.notification_store (
    id serial PRIMARY KEY,
    data_source_id integer REFERENCES directory.data_source ON DELETE CASCADE,
    CONSTRAINT uniqueness UNIQUE(data_source_id)
);

COMMENT ON TABLE notification.notification_store IS
'Describes notification_stores. Each notification_store maps to a set of tables '
'and functions that can store and manage notifications of a certain type. '
'These corresponding tables and functions are created automatically for each '
'notification_store. Because each notification_store maps one-on-one to a '
'data_source, the name of the notification_store is the same as that of the '
'data_source. Use the create_notification_store function to create new '
'notification_stores.';

ALTER TABLE notification.notification_store OWNER TO minerva_admin;

GRANT SELECT ON TABLE notification.notification_store TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE notification.notification_store TO minerva_writer;


-- Table 'notification.attribute'

CREATE TABLE notification.attribute (
    id serial PRIMARY KEY,
    notification_store_id integer REFERENCES notification.notification_store ON DELETE CASCADE,
    name name not null,
    data_type name not null,
    description varchar not null
);

COMMENT ON TABLE notification.attribute IS
'Describes attributes of notification_stores. An attribute of a '
'notification_store is an attribute that each notification stored in that '
'notification_store has. An attribute corresponds directly to a column in '
'the main notification_store table';

ALTER TABLE notification.attribute OWNER TO minerva_admin;

GRANT SELECT ON TABLE notification.attribute TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE notification.attribute TO minerva_writer;


-- Table 'notification.notificationsetstore'

CREATE TABLE notification.notificationsetstore (
    id serial PRIMARY KEY,
    name name not null,
    notification_store_id integer REFERENCES notification.notification_store ON DELETE CASCADE
);

COMMENT ON TABLE notification.notificationsetstore IS
'Describes notificationsetstores. A notificationsetstore can hold information '
'over sets of notifications that are related to each other.';

ALTER TABLE notification.notificationsetstore OWNER TO minerva_admin;

GRANT SELECT ON TABLE notification.notificationsetstore TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE notification.notificationsetstore TO minerva_writer;


-- Table 'notification.setattribute'

CREATE TABLE notification.setattribute (
    id serial PRIMARY KEY,
    notificationsetstore_id integer REFERENCES notification.notificationsetstore ON DELETE CASCADE,
    name name not null,
    data_type name not null,
    description varchar not null
);

COMMENT ON TABLE notification.setattribute IS
'Describes attributes of notificationsetstores. A setattribute of a '
'notificationsetstore is an attribute that each notification set has. '
'A setattribute corresponds directly to a column in the main '
'notificationsetstore table.';

ALTER TABLE notification.attribute OWNER TO minerva_admin;

GRANT SELECT ON TABLE notification.attribute TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE notification.attribute TO minerva_writer;
