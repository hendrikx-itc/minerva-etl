notification
============

Stores information of events that can occur at irregular intervals, but still have a fixed, known format.

Tables
------

attribute
`````````

Describes attributes of notification_stores. An attribute of a notification_store is an attribute that each notification stored in that notification_store has. An attribute corresponds directly to a column in the main notification_store table

+-----------------------+-------------------+---------------+
| Name                  | Type              |   Description |
+=======================+===================+===============+
| id                    | integer           |               |
+-----------------------+-------------------+---------------+
| notification_store_id | integer           |               |
+-----------------------+-------------------+---------------+
| name                  | name              |               |
+-----------------------+-------------------+---------------+
| data_type             | name              |               |
+-----------------------+-------------------+---------------+
| description           | character varying |               |
+-----------------------+-------------------+---------------+


notification_store
``````````````````

Describes notification_stores. Each notification_store maps to a set of tables and functions that can store and manage notifications of a certain type. These corresponding tables and functions are created automatically for each notification_store. Because each notification_store maps one-on-one to a data_source, the name of the notification_store is the same as that of the data_source. Use the create_notification_store function to create new notification_stores.

+----------------+---------+---------------+
| Name           | Type    |   Description |
+================+=========+===============+
| id             | integer |               |
+----------------+---------+---------------+
| data_source_id | integer |               |
+----------------+---------+---------------+


notificationsetstore
````````````````````

Describes notificationsetstores. A notificationsetstore can hold information over sets of notifications that are related to each other.

+-----------------------+---------+---------------+
| Name                  | Type    |   Description |
+=======================+=========+===============+
| id                    | integer |               |
+-----------------------+---------+---------------+
| name                  | name    |               |
+-----------------------+---------+---------------+
| notification_store_id | integer |               |
+-----------------------+---------+---------------+


setattribute
````````````

Describes attributes of notificationsetstores. A setattribute of a notificationsetstore is an attribute that each notification set has. A setattribute corresponds directly to a column in the main notificationsetstore table.

+-------------------------+-------------------+---------------+
| Name                    | Type              |   Description |
+=========================+===================+===============+
| id                      | integer           |               |
+-------------------------+-------------------+---------------+
| notificationsetstore_id | integer           |               |
+-------------------------+-------------------+---------------+
| name                    | name              |               |
+-------------------------+-------------------+---------------+
| data_type               | name              |               |
+-------------------------+-------------------+---------------+
| description             | character varying |               |
+-------------------------+-------------------+---------------+

Functions
---------

+------------------------------------------------------------------------------+-----------------------------------+---------------+
| Name                                                                         | Return Type                       |   Description |
+==============================================================================+===================================+===============+
| action(anyelement, text)                                                     | anyelement                        |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| add_attribute_column_sql(name, notification.attribute)                       | text                              |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| add_staging_attribute_column_sql(notification.attribute)                     | text                              |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| cleanup_on_data_source_delete()                                              | trigger                           |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| column_exists(schema_name name, table_name name, column_name name)           | boolean                           |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| column_exists(table_name name, column_name name)                             | boolean                           |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| create_attribute(notification.notification_store, name, name)                | SETOF notification.attribute      |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| create_attribute_column(notification.attribute)                              | notification.attribute            |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| create_attribute_column_on_insert()                                          | trigger                           |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| create_notification_store(data_source_id integer, notification.attr_def[])   | notification.notification_store   |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| create_notification_store(data_source_name text, notification.attr_def[])    | notification.notification_store   |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| create_notification_store(data_source_name text)                             | notification.notification_store   |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| create_notification_store(data_source_id integer)                            | notification.notification_store   |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| create_notificationsetstore(name name, notification.notification_store)      | notification.notificationsetstore |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| create_notificationsetstore(name name, notification_store_id integer)        | notification.notificationsetstore |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| create_staging_table(notification.notification_store)                        | notification.notification_store   |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| create_table(notification.notification_store)                                | notification.notification_store   |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| create_table_on_insert()                                                     | trigger                           |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| create_table_sql(notification.notification_store)                            | text[]                            |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| define_notificationsetstore(name name, notification_store_id integer)        | notification.notificationsetstore |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| drop_notificationsetstore_table_on_delete()                                  | trigger                           |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| drop_table_on_delete()                                                       | trigger                           |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| get_attr_defs(notification.notification_store)                               | SETOF notification.attr_def       |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| get_column_type_name(namespace_name name, table_name name, column_name name) | name                              |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| get_column_type_name(notification.notification_store, name)                  | name                              |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| get_notification_store(data_source_name name)                                | notification.notification_store   |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| init_notificationsetstore(notification.notificationsetstore)                 | notification.notificationsetstore |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| notification_store(notification.notificationsetstore)                        | notification.notification_store   |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| staging_table_name(notification.notification_store)                          | name                              |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| table_exists(name)                                                           | boolean                           |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| table_name(notification.notification_store)                                  | name                              |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
| to_char(notification.notification_store)                                     | text                              |               |
+------------------------------------------------------------------------------+-----------------------------------+---------------+
