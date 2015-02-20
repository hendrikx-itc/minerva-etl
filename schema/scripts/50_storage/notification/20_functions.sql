CREATE FUNCTION notification.to_char(notification.notification_store)
    RETURNS text
AS $$
    SELECT data_source.name
    FROM directory.data_source
    WHERE data_source.id = $1.data_source_id;
$$ LANGUAGE sql STABLE STRICT;


CREATE FUNCTION notification.get_notification_store(data_source_name name)
    RETURNS notification.notification_store
AS $$
    SELECT ns
    FROM notification.notification_store ns
    JOIN directory.data_source ds ON ds.id = ns.data_source_id
    WHERE ds.name = $1;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION notification.table_name(notification.notification_store)
    RETURNS name
AS $$
    SELECT name::name
    FROM directory.data_source
    WHERE id = $1.data_source_id;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION notification.staging_table_name(notification.notification_store)
    RETURNS name
AS $$
    SELECT (notification.table_name($1) || '_staging')::name;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION notification.create_table_sql(notification.notification_store)
    RETURNS text[]
AS $$
    SELECT ARRAY[
        format(
            'CREATE TABLE notification.%I ('
            '  id serial PRIMARY KEY,'
            '  entity_id integer NOT NULL,'
            '  "timestamp" timestamp with time zone NOT NULL'
            '%s'
            ');',
            notification.table_name($1),
            (SELECT array_to_string(array_agg(format(',%I %s', name, data_type)), ' ') FROM notification.attribute WHERE notification_store_id = $1.id)
        ),
        format(
            'ALTER TABLE notification.%I OWNER TO minerva_writer;',
            notification.table_name($1)
        ),
        format(
            'GRANT SELECT ON TABLE notification.%I TO minerva;',
            notification.table_name($1)
        ),
        format(
            'GRANT INSERT,DELETE,UPDATE '
            'ON TABLE notification.%I TO minerva_writer;',
            notification.table_name($1)
        ),
        format(
            'CREATE INDEX %I ON notification.%I USING btree (timestamp);',
            'idx_notification_' || notification.table_name($1) || '_timestamp', notification.table_name($1)
        )
    ];
$$ LANGUAGE sql STABLE;


CREATE FUNCTION notification.create_table(notification.notification_store)
    RETURNS notification.notification_store
AS $$
    SELECT public.action($1, notification.create_table_sql($1));
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION notification.initialize_notification_store(notification.notification_store)
    RETURNS notification.notification_store
AS $$
    SELECT notification.create_table($1);
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION notification.create_staging_table(notification.notification_store)
    RETURNS notification.notification_store
AS $$
BEGIN
    EXECUTE format(
        'CREATE UNLOGGED TABLE notification.%I ('
        '  entity_id integer NOT NULL,'
        '  "timestamp" timestamp with time zone NOT NULL'
        ');', notification.staging_table_name($1));

    EXECUTE format('ALTER TABLE notification.%I OWNER TO minerva_writer;',
        notification.staging_table_name($1));

    EXECUTE format('GRANT SELECT ON TABLE notification.%I TO minerva;',
        notification.staging_table_name($1));

    EXECUTE format(
        'GRANT INSERT,DELETE,UPDATE '
        'ON TABLE notification.%I TO minerva_writer;',
        notification.staging_table_name($1));

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION notification.table_exists(name)
    RETURNS boolean
AS $$
    SELECT public.table_exists('notification', $1);
$$ LANGUAGE sql STABLE;


CREATE FUNCTION notification.column_exists(schema_name name, table_name name, column_name name)
    RETURNS boolean
AS $$
    SELECT EXISTS(
        SELECT 1
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3
    );
$$ LANGUAGE sql STABLE;


CREATE FUNCTION notification.column_exists(table_name name, column_name name)
    RETURNS boolean
AS $$
    SELECT notification.column_exists('notification', $1, $2);
$$ LANGUAGE sql STABLE;


CREATE FUNCTION notification.create_notification_store(data_source_id integer)
    RETURNS notification.notification_store
AS $$
    INSERT INTO notification.notification_store(data_source_id) VALUES ($1) RETURNING *;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION notification.create_notification_store(data_source_name text)
    RETURNS notification.notification_store
AS $$
    SELECT notification.create_notification_store((directory.name_to_data_source($1)).id);
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION notification.define_attribute(notification.notification_store, name, name, text)
    RETURNS SETOF notification.attribute
AS $$
    INSERT INTO notification.attribute(notification_store_id, name, data_type, description)
    VALUES($1.id, $2, $3, $4) RETURNING *;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION notification.define_attributes(
        notification.notification_store,
        notification.attr_def[])
    RETURNS notification.notification_store
AS $$
    SELECT notification.define_attribute($1, name, data_type, description)
    FROM unnest($2);

    SELECT $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION notification.define_notification_store(data_source_id integer)
    RETURNS notification.notification_store
AS $$
    INSERT INTO notification.notification_store(data_source_id)
    VALUES ($1)
    RETURNING *;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION notification.define_notification_store(data_source_id integer, notification.attr_def[])
    RETURNS notification.notification_store
AS $$
    SELECT notification.define_attributes(
        notification.define_notification_store($1),
        $2
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION notification.create_notification_store(data_source_id integer, notification.attr_def[])
    RETURNS notification.notification_store
AS $$
    SELECT notification.initialize_notification_store(
        notification.define_notification_store($1, $2)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION notification.create_notification_store(data_source_name text, notification.attr_def[])
    RETURNS notification.notification_store
AS $$
    SELECT notification.create_notification_store((directory.name_to_data_source($1)).id, $2);
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION notification.define_notificationsetstore(name name, notification_store_id integer)
    RETURNS notification.notificationsetstore
AS $$
    INSERT INTO notification.notificationsetstore(name, notification_store_id)
    VALUES ($1, $2)
    RETURNING *;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION notification.notification_store(notification.notificationsetstore)
    RETURNS notification.notification_store
AS $$
    SELECT notification_store FROM notification.notification_store WHERE id = $1.notification_store_id;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION notification.init_notificationsetstore(notification.notificationsetstore)
    RETURNS notification.notificationsetstore
AS $$
BEGIN
    EXECUTE format(
        'CREATE TABLE notification.%I('
        '  id serial PRIMARY KEY'
        ')', $1.name);

    EXECUTE format(
        'CREATE TABLE notification.%I('
        '  notification_id integer REFERENCES notification.%I ON DELETE CASCADE,'
        '  set_id integer REFERENCES notification.%I ON DELETE CASCADE'
        ')',
        $1.name || '_link',
        notification.table_name(notification.notification_store($1)),
        $1.name
    );

    RETURN $1;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION notification.create_notificationsetstore(name name, notification_store_id integer)
    RETURNS notification.notificationsetstore
AS $$
    SELECT notification.init_notificationsetstore(
        notification.define_notificationsetstore($1, $2)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION notification.create_notificationsetstore(name name, notification.notification_store)
    RETURNS notification.notificationsetstore
AS $$
    SELECT notification.create_notificationsetstore($1, $2.id);
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION notification.get_column_type_name(namespace_name name, table_name name, column_name name)
    RETURNS name
AS $$
    SELECT typname
    FROM pg_type
    JOIN pg_attribute ON pg_attribute.atttypid = pg_type.oid
    JOIN pg_class ON pg_class.oid = pg_attribute.attrelid
    JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
    WHERE nspname = $1 AND relname = $2 AND typname = $3 AND attnum > 0 AND not pg_attribute.attisdropped;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION notification.get_column_type_name(notification.notification_store, name)
    RETURNS name
AS $$
    SELECT notification.get_column_type_name('notification', notification.table_name($1), $2);
$$ LANGUAGE sql STABLE;


CREATE FUNCTION notification.add_attribute_column_sql(name, notification.attribute)
    RETURNS text
AS $$
    SELECT format(
        'ALTER TABLE notification.%I ADD COLUMN %I %s',
        $1, $2.name, $2.data_type
    );
$$ LANGUAGE sql STABLE;


CREATE FUNCTION notification.add_staging_attribute_column_sql(notification.attribute)
    RETURNS text
AS $$
    SELECT
        format(
            'ALTER TABLE notification.%I ADD COLUMN %I %s',
            notification.staging_table_name(notification_store), $1.name, $1.data_type
        )
    FROM notification.notification_store WHERE id = $1.notification_store_id;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION notification.create_attribute_column(notification.attribute)
    RETURNS notification.attribute
AS $$
SELECT
    public.action(
        $1,
        notification.add_attribute_column_sql(
            notification.table_name(notification_store),
            $1
        )
    )
FROM notification.notification_store WHERE id = $1.notification_store_id;

SELECT
    public.action(
        $1,
        notification.add_attribute_column_sql(
            notification.staging_table_name(notification_store),
            $1
        )
    )
FROM notification.notification_store WHERE id = $1.notification_store_id;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION notification.get_attr_defs(notification.notification_store)
    RETURNS SETOF notification.attr_def AS
$$
    SELECT (attname, typname, '')::notification.attr_def
    FROM pg_type
    JOIN pg_attribute ON pg_attribute.atttypid = pg_type.oid
    JOIN pg_class ON pg_class.oid = pg_attribute.attrelid
    JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
    WHERE
    nspname = 'notification' AND
    relname = notification.table_name($1) AND
    attnum > 0 AND
    NOT attname IN ('id', 'entity_id', 'timestamp') AND
    NOT pg_attribute.attisdropped;
$$ LANGUAGE sql STABLE;
