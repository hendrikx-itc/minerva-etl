CREATE FUNCTION trend_directory.base_table_schema()
    RETURNS name
AS $$
    SELECT 'trend'::name;
$$ LANGUAGE sql IMMUTABLE;


CREATE FUNCTION trend_directory.partition_table_schema()
    RETURNS name
AS $$
    SELECT 'trend_partition'::name;
$$ LANGUAGE sql IMMUTABLE;


CREATE FUNCTION trend_directory.staging_table_schema()
    RETURNS name
AS $$
    SELECT 'trend'::name;
$$ LANGUAGE sql IMMUTABLE;


CREATE FUNCTION trend_directory.view_schema()
    RETURNS name
AS $$
    SELECT 'trend'::name;
$$ LANGUAGE sql IMMUTABLE;


CREATE FUNCTION trend_directory.granularity_to_text(interval)
    RETURNS text
AS $$
    SELECT CASE $1
        WHEN '300'::interval THEN
            '5m'
        WHEN '900'::interval THEN
            'qtr'
        WHEN '1 hour'::interval THEN
            'hr'
        WHEN '12 hours'::interval THEN
            '12hr'
        WHEN '1 day'::interval THEN
            'day'
        WHEN '1 week'::interval THEN
            'wk'
        WHEN '1 month'::interval THEN
            'month'
        ELSE
            $1::text
        END;
$$ LANGUAGE sql IMMUTABLE STRICT;


CREATE FUNCTION trend_directory.base_table_name(trend_store trend_directory.trend_store)
    RETURNS name
AS $$
    SELECT format(
        '%s_%s_%s',
        data_source.name,
        entity_type.name,
        trend_directory.granularity_to_text($1.granularity)
    )::name
    FROM directory.data_source, directory.entity_type
    WHERE data_source.id = $1.data_source_id AND entity_type.id = $1.entity_type_id;
$$ LANGUAGE sql STABLE STRICT;


CREATE FUNCTION trend_directory.to_char(trend_directory.trend_store)
    RETURNS text
AS $$
    SELECT trend_directory.base_table_name($1)::text;
$$ LANGUAGE sql STABLE STRICT;


CREATE FUNCTION trend_directory.to_char(trend_directory.view)
    RETURNS text
AS $$
    SELECT trend_store::text FROM trend_directory.trend_store WHERE id = $1.trend_store_id;
$$ LANGUAGE sql STABLE STRICT;


CREATE FUNCTION trend_directory.get_trend_store_by_attributes(
        data_source_name character varying, entity_type_name character varying,
        granularity interval)
    RETURNS trend_directory.trend_store
AS $$
    SELECT ts
    FROM trend_directory.trend_store ts
    JOIN directory.data_source ds ON ds.id = ts.data_source_id
    JOIN directory.entity_type et ON et.id = ts.entity_type_id
    WHERE lower(ds.name) = lower($1) AND lower(et.name) = lower($2) AND ts.granularity = $3;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.create_base_table_sql(name text, trend_directory.trend[])
    RETURNS text[]
AS $$
SELECT ARRAY[
    format(
        'CREATE TABLE %I.%I ('
        'entity_id integer NOT NULL, '
        '"timestamp" timestamp with time zone NOT NULL, '
        'modified timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP, '
        '%s'
        'PRIMARY KEY (entity_id, "timestamp") '
        ');',
        trend_directory.base_table_schema(),
        name,
        (SELECT array_to_string(array_agg(format('%I %s,', t.name, t.data_type)), ' ') FROM unnest($2) t)
    ),
    format(
        'ALTER TABLE %I.%I OWNER TO minerva_writer;',
        trend_directory.base_table_schema(),
        name
    ),
    format(
        'GRANT SELECT ON TABLE %I.%I TO minerva;',
        trend_directory.base_table_schema(),
        name
    ),
    format(
        'GRANT INSERT,DELETE,UPDATE ON TABLE %I.%I TO minerva_writer;',
        trend_directory.base_table_schema(),
        name
    ),
    format(
        'CREATE INDEX ON %I.%I USING btree (modified);',
        trend_directory.base_table_schema(),
        name
    ),
    format(
        'CREATE INDEX ON %I.%I USING btree (timestamp);',
        trend_directory.base_table_schema(),
        name
    )
];
$$ LANGUAGE sql STABLE STRICT;


CREATE FUNCTION trend_directory.create_base_table(name name, trend_directory.trend[])
    RETURNS name
AS $$
    SELECT public.action($1, trend_directory.create_base_table_sql($1, $2))
$$ LANGUAGE sql VOLATILE STRICT;


CREATE FUNCTION trend_directory.create_base_table(trend_directory.trend_store, trend_directory.trend[])
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.create_base_table(trend_directory.base_table_name($1), $2);
    SELECT $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.get_trend_store_trends(trend_directory.trend_store)
    RETURNS trend_directory.trend[]
AS $$
    SELECT COALESCE(array_agg(trend), ARRAY[]::trend_directory.trend[])
    FROM trend_directory.trend
    WHERE trend_store_id = $1.id
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.create_base_table(trend_directory.trend_store)
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.create_base_table(
        $1,
        trend_directory.get_trend_store_trends($1)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.staging_table_name(trend_directory.trend_store)
    RETURNS name
AS $$
    SELECT (trend_directory.base_table_name($1) || '_staging')::name;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.create_staging_table_sql(trend_store trend_directory.trend_store)
    RETURNS text[]
AS $$
SELECT ARRAY[
    format('CREATE UNLOGGED TABLE trend.%I () INHERITS (trend.%I);', trend_directory.staging_table_name($1), trend_directory.base_table_name(trend_store)),
    format('ALTER TABLE ONLY trend.%I ADD PRIMARY KEY (entity_id, "timestamp");', trend_directory.staging_table_name($1)),
    format('ALTER TABLE trend.%I OWNER TO minerva_writer;', trend_directory.staging_table_name($1)),
    format('GRANT SELECT ON TABLE trend.%I TO minerva;', trend_directory.staging_table_name($1)),
    format('GRANT INSERT,DELETE,UPDATE ON TABLE trend.%I TO minerva_writer;', trend_directory.staging_table_name($1))
];
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.create_staging_table(trend_store trend_directory.trend_store)
    RETURNS trend_directory.trend_store
AS $$
    SELECT public.action($1, trend_directory.create_staging_table_sql($1));
$$ LANGUAGE sql VOLATILE STRICT;


CREATE FUNCTION trend_directory.initialize_trend_store(trend_directory.trend_store)
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.create_base_table($1);
    SELECT trend_directory.create_staging_table($1);

    SELECT $1;
$$ LANGUAGE sql VOLATILE;

COMMENT ON FUNCTION trend_directory.initialize_trend_store(trend_directory.trend_store) IS
'Create all database objects required for the trend_store to be fully functional
and capable of storing data.';


CREATE FUNCTION trend_directory.get_default_partition_size(granularity interval)
    RETURNS integer
AS $$
    SELECT CASE $1
        WHEN '300'::interval THEN
            3 * 3600
        WHEN '900'::interval THEN
            6 * 3600
        WHEN '1800'::interval THEN
            6 * 3600
        WHEN '1 hour'::interval THEN
            24 * 3600
        WHEN '12 hours'::interval THEN
            24 * 3600 * 7
        WHEN '1 day'::interval THEN
            24 * 3600 * 7
        WHEN '1 week'::interval THEN
            24 * 3600 * 7 * 4
        WHEN '1 month'::interval THEN
            24 * 3600 * 7 * 24
        END;
$$ LANGUAGE sql IMMUTABLE STRICT;


CREATE FUNCTION trend_directory.define_trend_store(
        data_source_name character varying, entity_type_name character varying,
        granularity interval, partition_size integer,
        type trend_directory.storetype)
    RETURNS trend_directory.trend_store
AS $$
    INSERT INTO trend_directory.trend_store (
        data_source_id,
        entity_type_id,
        granularity,
        partition_size,
        type
    )
    VALUES (
        (directory.name_to_data_source($1)).id,
        (directory.name_to_entity_type($2)).id,
        $3,
        $4,
        $5
    ) RETURNING *;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.define_trend_store(
        data_source_name character varying, entity_type_name character varying,
        granularity interval
    )
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.define_trend_store(
        $1,
        $2,
        $3,
        trend_directory.get_default_partition_size($3),
        'table'
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.define_trend_store(
        data_source_name character varying, entity_type_name character varying,
        granularity interval, type trend_directory.storetype)
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.define_trend_store(
        $1,
        $2,
        $3,
        trend_directory.get_default_partition_size($3),
        $4
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_trend_store(
        data_source_name character varying, entity_type_name character varying,
        granularity interval)
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.initialize_trend_store(
        trend_directory.define_trend_store($1, $2, $3)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.define_trends(
        trend_directory.trend_store,
        trend_directory.trend_descr[])
    RETURNS trend_directory.trend_store
AS $$
    INSERT INTO trend_directory.trend(name, data_type, trend_store_id, description) (
        SELECT name, data_type, $1.id, description FROM unnest($2)
    );

    SELECT $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.define_trend_store(
        data_source_name character varying, entity_type_name character varying,
        granularity interval, trends trend_directory.trend_descr[],
        partition_size integer, type trend_directory.storetype)
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.define_trends(
        trend_directory.define_trend_store($1, $2, $3, $5, $6),
        $4
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.define_trend_store(
        data_source_name character varying, entity_type_name character varying,
        granularity interval, trends trend_directory.trend_descr[])
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.define_trends(
        trend_directory.define_trend_store($1, $2, $3),
        $4
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_trend_store(
        data_source_name character varying, entity_type_name character varying,
        granularity interval, type trend_directory.storetype)
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.initialize_trend_store(
        trend_directory.define_trend_store($1, $2, $3, $4)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.attributes_to_trend_store(
        data_source_name character varying, entity_type_name character varying,
        granularity interval)
    RETURNS trend_directory.trend_store
AS $$
    SELECT COALESCE(
        trend_directory.get_trend_store_by_attributes($1, $2, $3),
        trend_directory.create_trend_store($1, $2, $3)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.attributes_to_view_trend_store(
        data_source_name character varying, entity_type_name character varying,
        granularity interval)
    RETURNS trend_directory.trend_store
AS $$
    SELECT COALESCE(
        trend_directory.get_trend_store_by_attributes($1, $2, $3),
        trend_directory.define_trend_store($1, $2, $3, 'view'::trend_directory.storetype)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.partition_name(trend_store trend_directory.trend_store, index integer)
    RETURNS name
AS $$
    SELECT (trend_directory.base_table_name($1) || '_' || $2)::name;
$$ LANGUAGE sql STABLE STRICT;


CREATE FUNCTION trend_directory.timestamp_to_index(
        partition_size integer, "timestamp" timestamp with time zone)
    RETURNS integer
AS $$
DECLARE
    unix_timestamp integer;
    div integer;
    modulo integer;
BEGIN
    unix_timestamp = extract(EPOCH FROM "timestamp")::integer;
    div = unix_timestamp / partition_size;
    modulo = mod(unix_timestamp, partition_size);

    IF modulo > 0 THEN
        return div;
    ELSE
        return div - 1;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;


CREATE FUNCTION trend_directory.partition_name(
        trend_directory.trend_store, timestamp with time zone)
    RETURNS name
AS $$
    SELECT trend_directory.partition_name(
        $1, trend_directory.timestamp_to_index($1.partition_size, $2)
    );
$$ LANGUAGE sql STABLE STRICT;


CREATE FUNCTION trend_directory.table_name(trend_directory.partition)
    RETURNS name
AS $$
    SELECT trend_directory.partition_name(trend_store, $1.index)
    FROM trend_directory.trend_store
    WHERE id = $1.trend_store_id;
$$ LANGUAGE sql STABLE STRICT;


CREATE FUNCTION trend_directory.get_index_on(character varying, character varying)
    RETURNS name
AS $$
    SELECT
            i.relname
    FROM
            pg_class t,
            pg_class i,
            pg_index ix,
            pg_attribute a
    WHERE
            t.oid = ix.indrelid
            and i.oid = ix.indexrelid
            and a.attrelid = t.oid
            and a.attnum = ANY(ix.indkey)
            and t.relkind = 'r'
            and t.relname = $1
            and a.attname = $2;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.trend_store(trend_directory.partition)
    RETURNS trend_directory.trend_store
AS $$
    SELECT * FROM trend_directory.trend_store WHERE id = $1.trend_store_id;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.staged_timestamps(trend_store trend_directory.trend_store)
    RETURNS SETOF timestamp with time zone
AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        'SELECT timestamp FROM %I.%I GROUP BY timestamp',
        trend_directory.staging_table_schema(),
        trend_directory.staging_table_name(trend_store)
    );
END;
$$ LANGUAGE plpgsql STABLE;


CREATE FUNCTION trend_directory.transfer_staged(trend_store trend_directory.trend_store, "timestamp" timestamp with time zone)
    RETURNS integer
AS $$
DECLARE
    row_count integer;
BEGIN
    EXECUTE format(
        'INSERT INTO %I.%I SELECT * FROM %I.%I WHERE timestamp = $1',
        trend_directory.partition_table_schema(),
        trend_directory.table_name(trend_directory.attributes_to_partition(
            trend_store,
            trend_directory.timestamp_to_index(trend_store.partition_size, timestamp)
        )),
        trend_directory.staging_table_schema(),
        trend_directory.staging_table_name(trend_store)
    ) USING timestamp;

    GET DIAGNOSTICS row_count = ROW_COUNT;

    RETURN row_count;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION trend_directory.transfer_staged(trend_store trend_directory.trend_store)
    RETURNS void
AS $$
    SELECT
        trend_directory.transfer_staged(trend_store, timestamp)
    FROM trend_directory.staged_timestamps(trend_store) timestamp;

    SELECT public.action(format(
        'TRUNCATE %I.%I',
        trend_directory.staging_table_schema(),
        trend_directory.staging_table_name(trend_store)
    ));
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.cluster_partition_table_on_timestamp_sql(name text)
    RETURNS text
AS $$
    SELECT format(
        'CLUSTER %I.%I USING %I',
        trend_directory.partition_table_schema(),
        $1,
        trend_directory.get_index_on($1, 'timestamp')
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.cluster_partition_table_on_timestamp(name text)
    RETURNS text
AS $$
    SELECT public.action(
        $1,
        trend_directory.cluster_partition_table_on_timestamp_sql($1)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_partition_column(
        partition_name name, trend_id integer, data_type text)
    RETURNS void
AS $$
DECLARE
    trend_name varchar;
BEGIN
    SELECT name INTO trend_name FROM trend WHERE id = trend_id;

    EXECUTE format(
        'ALTER TABLE %I ADD COLUMN %I %I;',
        partition_name,
        trend_name,
        data_type
    );
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION trend_directory.modify_trend_store_column(
        trend_directory.trend_store, column_name name, data_type text)
    RETURNS trend_directory.trend_store
AS $$
    SELECT dep_recurse.alter(
        dep_recurse.table_ref(
            trend_directory.base_table_schema(),
            trend_directory.base_table_name($1)
        ),
        ARRAY[
            format(
                'ALTER TABLE %I.%I ALTER %I TYPE %s USING CAST(%I AS %s);',
                trend_directory.base_table_schema(),
                trend_directory.base_table_name($1),
                $2, $3, $2, $3
            )
        ]
    );

    SELECT $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.modify_trend_store_column(
        trend_store_id integer, column_name name, data_type text)
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.modify_trend_store_column(
        trend_store, $2, $3
    )
    FROM trend_directory.trend_store
    WHERE trend_store.id = $1;
$$ LANGUAGE sql VOLATILE;


CREATE TYPE trend_directory.column_info AS (
    name name,
    data_type text
);


CREATE FUNCTION trend_directory.table_columns(namespace name, "table" name)
    RETURNS SETOF trend_directory.column_info
AS $$
    SELECT
        a.attname,
        format_type(a.atttypid, a.atttypmod)
    FROM
        pg_catalog.pg_class c
    JOIN
        pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    JOIN
        pg_catalog.pg_attribute a ON a.attrelid = c.oid
    WHERE
        n.nspname = $1 AND
        c.relname = $2 AND
        a.attisdropped = false AND
        a.attnum > 0;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.alter_column_types(
        namespace_name name, table_name name, columns trend_directory.column_info[])
    RETURNS dep_recurse.obj_ref
AS $$
    SELECT dep_recurse.alter(
        dep_recurse.table_ref('trend', table_name),
        ARRAY[
            format(
                'ALTER TABLE %I.%I %s',
                namespace_name,
                table_name,
                array_to_string(
                    array_agg(
                        format(
                            'ALTER %I TYPE %s USING CAST (%I AS %s)',
                            c.name,
                            c.data_type,
                            c.name,
                            c.data_type
                        )
                    ),
                    ', '
                )
            )
        ]
    )
    FROM unnest(columns) AS c;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.modify_trend_store_columns(
        trend_directory.trend_store, columns trend_directory.column_info[])
    RETURNS trend_directory.trend_store
AS $$
   SELECT trend_directory.alter_column_types(
        'trend',
        trend_directory.base_table_name($1),
        $2
   );

    SELECT $1;
$$ LANGUAGE sql;


CREATE FUNCTION trend_directory.modify_trend_store_columns(
        trend_store_id integer, columns trend_directory.column_info[])
    RETURNS trend_directory.trend_store
AS $$
   SELECT trend_directory.modify_trend_store_columns(
        trend_store,
        columns
    )
    FROM trend_directory.trend_store
    WHERE trend_store.id = trend_store_id;
$$ LANGUAGE sql;


CREATE FUNCTION trend_directory.view_name(trend_directory.view)
    RETURNS name
AS $$
    SELECT trend_directory.base_table_name(trend_store)
    FROM trend_directory.trend_store
    WHERE trend_store.id = $1.trend_store_id;
$$ LANGUAGE sql;


CREATE FUNCTION trend_directory.drop_view_sql(view trend_directory.view)
    RETURNS text
AS $$
    SELECT format(
        'DROP VIEW IF EXISTS %I.%I',
        trend_directory.view_schema(),
        trend_directory.view_name(view)
    );
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.unlink_view_dependencies(trend_directory.view)
    RETURNS trend_directory.view
AS $$
    DELETE FROM trend_directory.view_trend_store_link
    WHERE view_id = $1.id
    RETURNING $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.delete_view_trends(view trend_directory.view)
    RETURNS void
AS $$
    DELETE FROM trend_directory.trend
    WHERE trend_store_id = $1.trend_store_id;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.drop_view(view trend_directory.view)
    RETURNS trend_directory.view
AS $$
    SELECT public.action($1, trend_directory.drop_view_sql($1));

    SELECT trend_directory.unlink_view_dependencies(view);

    SELECT trend_directory.delete_view_trends(view);

    SELECT $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.alter_view(trend_directory.view, text)
    RETURNS trend_directory.view
AS $$
    SELECT dep_recurse.alter(
        dep_recurse.view_ref('trend', $1::text),
        ARRAY[
            format(
                'SELECT trend_directory.recreate_view(view) '
                'FROM trend_directory.view '
                'WHERE id = %L',
                $1.id
            )
        ]
    );

    SELECT $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_view_sql(trend_directory.view, sql text)
    RETURNS text[]
AS $$
SELECT ARRAY[
    format('CREATE VIEW %I.%I AS %s;', trend_directory.view_schema(), trend_directory.view_name($1), $2),
    format('ALTER TABLE %I.%I OWNER TO minerva_writer;', trend_directory.view_schema(), trend_directory.view_name($1)),
    format('GRANT SELECT ON TABLE %I.%I TO minerva;', trend_directory.view_schema(), trend_directory.view_name($1))
];
$$ LANGUAGE sql STABLE;


-- View required by function 'link_view_dependencies'
CREATE VIEW trend_directory.view_dependencies AS
    SELECT dependent.relname AS src, pg_attribute.attname column_name, dependee.relname AS dst
    FROM pg_depend
    JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
    JOIN pg_class as dependee ON pg_rewrite.ev_class = dependee.oid
    JOIN pg_class as dependent ON pg_depend.refobjid = dependent.oid
    JOIN pg_namespace as n ON dependent.relnamespace = n.oid
    JOIN pg_attribute ON
            pg_depend.refobjid = pg_attribute.attrelid
            AND
            pg_depend.refobjsubid = pg_attribute.attnum
    WHERE n.nspname = 'trend' AND pg_attribute.attnum > 0;

ALTER VIEW trend_directory.view_dependencies OWNER TO minerva_admin;

GRANT SELECT ON TABLE trend_directory.view_dependencies TO minerva;


CREATE FUNCTION trend_directory.link_view_dependencies(trend_directory.view)
    RETURNS trend_directory.view
AS $$
    INSERT INTO trend_directory.view_trend_store_link (view_id, trend_store_id)
    SELECT $1.id, trend_store.id
    FROM trend_directory.view_dependencies vdeps
    JOIN trend_directory.trend_store ON trend_directory.base_table_name(trend_store) = vdeps.src
    LEFT JOIN trend_directory.view_trend_store_link vtl ON vtl.view_id = $1.id AND vtl.trend_store_id = trend_store.id
    WHERE vdeps.dst = trend_directory.view_name($1) AND vtl.view_id IS NULL
    GROUP BY trend_store.id
    RETURNING $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.get_view_trends(view_name name)
    RETURNS SETOF trend_directory.trend_descr
AS $$
    SELECT (a.attname, 'integer', 'deduced from view')::trend_directory.trend_descr
    FROM pg_class c
    JOIN pg_attribute a ON a.attrelid = c.oid
    WHERE c.relname = $1 AND a.attnum >= 0 AND NOT a.attisdropped;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.define_trend(
        name name, data_type text, trend_store_id integer,
        description text)
    RETURNS trend_directory.trend
AS $$
    INSERT INTO trend_directory.trend (name, data_type, trend_store_id, description)
    VALUES ($1, $2, $3, $4)
    RETURNING trend;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.add_trend_to_trend_store(
        trend_directory.trend_store, trend_directory.trend)
    RETURNS trend_directory.trend
AS $$
    SELECT dep_recurse.alter(
        dep_recurse.table_ref('trend', trend_directory.base_table_name($1)),
        ARRAY[
            format(
                'ALTER TABLE %I.%I ADD COLUMN %I %s;',
                trend_directory.base_table_schema(),
                trend_directory.base_table_name($1),
                $2.name,
                $2.data_type
            )
        ]
    );

    SELECT $2;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.add_trend_to_trend_store(
         trend_directory.trend_store, name,
        data_type text, description text)
    RETURNS trend_directory.trend
AS $$
    SELECT trend_directory.add_trend_to_trend_store(
        $1,
        trend_directory.define_trend($2, $3, $1.id, $4)
    )
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_trend(trend_directory.trend_store, trend_directory.trend_descr)
    RETURNS trend_directory.trend
AS $$
    SELECT trend_directory.add_trend_to_trend_store($1, $2.name, $2.data_type, $2.description);
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_trends(trend_directory.trend_store, trend_directory.trend_descr[])
    RETURNS SETOF trend_directory.trend
AS $$
    SELECT trend_directory.create_trend($1, descr)
    FROM unnest($2) descr;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.missing_trends(trend_directory.trend_store, required trend_directory.trend_descr[])
    RETURNS SETOF trend_directory.trend_descr
AS $$
    SELECT (required.name, required.data_type, required.description)::trend_directory.trend_descr
    FROM unnest($2) required
    LEFT JOIN trend_directory.trend ON trend.name = required.name AND trend.trend_store_id = $1.id
    WHERE trend.id IS NULL;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.assure_trends_exist(trend_directory.trend_store, trend_directory.trend_descr[])
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.create_trend($1, t)
    FROM trend_directory.missing_trends($1, $2) t;

    SELECT $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.modify_data_type(trend_directory.trend_store, trend_directory.trend, required_data_type text)
    RETURNS trend_directory.trend_store
AS $$
    UPDATE trend_directory.trend SET data_type = $3;

    SELECT trend_directory.modify_trend_store_column($1, $2.name, $3);

    SELECT $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.assure_data_types(trend_directory.trend_store, trend_directory.trend_descr[])
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.modify_data_type($1, trend, required.data_type)
    FROM unnest($2) required
    JOIN trend_directory.trend ON
        trend.name = required.name
            AND
        trend.trend_store_id = $1.id
            AND
        trend.data_type <> required.data_type;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.get_trend_store(view trend_directory.view)
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_store FROM trend_directory.trend_store WHERE id = $1.trend_store_id;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.create_view_trends(view trend_directory.view)
    RETURNS SETOF trend_directory.trend
AS $$
    SELECT
        trend_directory.define_trend(
            vt.name,
            vt.data_type,
            id(trend_directory.get_trend_store($1)),
            vt.description
        )
    FROM trend_directory.get_view_trends(trend_directory.view_name($1)) vt
    WHERE vt.name NOT IN ('entity_id', 'timestamp', 'samples');
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_view(trend_directory.view, text)
    RETURNS trend_directory.view
AS $$
    SELECT public.action($1, trend_directory.create_view_sql($1, $2));

    SELECT trend_directory.link_view_dependencies($1);

    SELECT trend_directory.create_view_trends($1);

    SELECT $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.get_most_recent_timestamp(
        dest_granularity interval, ts timestamp with time zone)
    RETURNS timestamp with time zone
AS $$
DECLARE
    minute integer;
    rounded_minutes integer;
BEGIN
    IF dest_granularity < '1 hour'::interval THEN
        minute := extract(minute FROM ts);
        rounded_minutes := minute - (minute % (dest_granularity / 60));

        return date_trunc('hour', ts) + (rounded_minutes || 'minutes')::INTERVAL;
    ELSIF dest_granularity = '1 hour'::interval THEN
        return date_trunc('hour', ts);
    ELSIF dest_granularity = '1 day'::interval THEN
        return date_trunc('day', ts);
    ELSIF dest_granularity = '1 week'::interval THEN
        return date_trunc('week', ts);
    ELSE
        RAISE EXCEPTION 'Invalid granularity: %', dest_granularity;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE FUNCTION trend_directory.is_integer(varchar)
    RETURNS boolean
AS $$
    SELECT $1 ~ '^[1-9][0-9]*$'
$$ LANGUAGE sql IMMUTABLE;


CREATE FUNCTION trend_directory.get_most_recent_timestamp(
        dest_granularity varchar, ts timestamp with time zone)
    RETURNS timestamp with time zone
AS $$
DECLARE
    minute integer;
    rounded_minutes integer;
    seconds integer;
BEGIN
    IF trend_directory.is_integer(dest_granularity) THEN
        seconds = cast(dest_granularity as integer);

        return trend_directory.get_most_recent_timestamp(seconds, ts);
    ELSIF dest_granularity = 'month' THEN
        return date_trunc('month', ts);
    ELSE
        RAISE EXCEPTION 'Invalid granularity: %', dest_granularity;
    END IF;

    return seconds;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE FUNCTION trend_directory.get_timestamp_for(
        granularity interval, ts timestamp with time zone)
    RETURNS timestamp with time zone
AS $$
DECLARE
    most_recent_timestamp timestamp with time zone;
BEGIN
    most_recent_timestamp = trend_directory.get_most_recent_timestamp($1, $2);

    IF most_recent_timestamp != ts THEN
        IF granularity = 86400 THEN
            return most_recent_timestamp + ('1 day')::INTERVAL;
        ELSE
            return most_recent_timestamp + ($1 || ' seconds')::INTERVAL;
        END IF;
    ELSE
        return most_recent_timestamp;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE FUNCTION trend_directory.get_timestamp_for(
        granularity varchar, ts timestamp with time zone)
    RETURNS timestamp with time zone
AS $$
DECLARE
    most_recent_timestamp timestamp with time zone;
BEGIN
    most_recent_timestamp = trend_directory.get_most_recent_timestamp($1, $2);

    IF most_recent_timestamp != ts THEN
        IF trend_directory.is_integer(granularity) THEN
            IF granularity = '86400' THEN
                return most_recent_timestamp + ('1 day')::INTERVAL;
            ELSE
                return most_recent_timestamp + ($1 || ' seconds')::INTERVAL;
            END IF;
        ELSIF granularity = 'month' THEN
            return most_recent_timestamp + '1 month'::INTERVAL;
        END IF;
    ELSE
        return most_recent_timestamp;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE FUNCTION trend_directory.index_to_timestamp(partition_size integer, index integer)
    RETURNS timestamp with time zone
AS $$
    SELECT to_timestamp($1 * $2 + 1);
$$ LANGUAGE sql IMMUTABLE STRICT;


CREATE FUNCTION trend_directory.data_start(trend_directory.partition)
    RETURNS timestamp with time zone
AS $$
    SELECT trend_directory.index_to_timestamp(
        (trend_directory.trend_store($1)).partition_size, $1.index
    );
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.data_end(trend_directory.partition)
    RETURNS timestamp with time zone
AS $$
    SELECT trend_directory.index_to_timestamp(
        (trend_directory.trend_store($1)).partition_size, $1.index + 1
    );
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.create_partition_table_sql(trend_directory.partition)
    RETURNS text[]
AS $$
    SELECT ARRAY[
        format(
            'CREATE TABLE %I.%I ('
            'CHECK ("timestamp" > %L AND "timestamp" <= %L)'
            ') INHERITS (trend.%I);',
            trend_directory.partition_table_schema(),
            trend_directory.table_name($1),
            trend_directory.data_start($1),
            trend_directory.data_end($1),
            trend_directory.base_table_name(trend_directory.trend_store($1))
        ),
        format(
            'ALTER TABLE ONLY %I.%I '
            'ADD PRIMARY KEY (entity_id, "timestamp");',
            trend_directory.partition_table_schema(),
            trend_directory.table_name($1)
        ),
        format(
            'CREATE INDEX ON %I.%I USING btree (modified);',
            trend_directory.partition_table_schema(),
            trend_directory.table_name($1)
        ),
        format(
            'CREATE INDEX ON %I.%I USING btree (timestamp);',
            trend_directory.partition_table_schema(),
            trend_directory.table_name($1)
        ),
        format(
            'ALTER TABLE %I.%I OWNER TO minerva_writer;',
            trend_directory.partition_table_schema(),
            trend_directory.table_name($1)
        ),
        format(
            'GRANT SELECT ON TABLE %I.%I TO minerva;',
            trend_directory.partition_table_schema(),
            trend_directory.table_name($1)
        ),
        format(
            'GRANT INSERT,DELETE,UPDATE ON TABLE %I.%I TO minerva_writer;',
            trend_directory.partition_table_schema(),
            trend_directory.table_name($1)
        ),
        format(
            'SELECT trend_directory.cluster_partition_table_on_timestamp(%L)',
            trend_directory.table_name($1)
        )
    ];
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.create_partition_table(trend_directory.partition)
    RETURNS trend_directory.partition
AS $$
    SELECT public.action($1, trend_directory.create_partition_table_sql($1));
$$ LANGUAGE sql VOLATILE STRICT;


CREATE FUNCTION trend_directory.get_trend(
        trend_store trend_directory.trend_store, trend_name name)
    RETURNS trend_directory.trend
AS $$
    SELECT trend
    FROM trend_directory.trend
    WHERE trend.trend_store_id = $1.id AND name = $2;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.get_trends_for_trend_store(trend_store_id integer)
    RETURNS SETOF trend_directory.trend
AS $$
    SELECT * FROM trend_directory.trend WHERE trend.trend_store_id = $1;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.get_trends_for_trend_store(trend_directory.trend_store)
    RETURNS SETOF trend_directory.trend
AS $$
    SELECT trend_directory.get_trends_for_trend_store($1.id);
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.trend_store_has_trend_with_name(
        trend_store trend_directory.trend_store, trend_name character varying)
    RETURNS boolean
AS $$
    SELECT exists(
        SELECT 1
        FROM trend_directory.trend_store_trend_link ttl
        JOIN trend_directory.trend t ON ttl.trend_id = t.id
        WHERE ttl.trend_store_id = $1.id AND t.name = $2
    );
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.attributes_to_trend(trend_directory.trend_store, name name, data_type text)
    RETURNS trend_directory.trend
AS $$
    SELECT COALESCE(
        trend_directory.get_trend($1, $2),
        trend_directory.define_trend($2, $3, $1.id, '')
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.remove_trend_from_trend_store(
        trend_store trend_directory.trend_store, trend_name character varying)
    RETURNS trend_directory.trend_store
AS $$
    DELETE FROM trend_directory.trend WHERE trend_store_id = $1.id;

    SELECT public.action(
        $1,
        format(
            'ALTER TABLE trend_directory.%I DROP COLUMN %I;',
            trend_directory.base_table_name(trend_store),
            trend_name
        )
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.remove_trend_from_trend_store(
        trend_store text, trend_name character varying)
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.remove_trend_from_trend_store(trend_store, $2)
    FROM trend_directory.trend_store
    WHERE trend_store::text = $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.column_exists(
        table_name name, column_name name)
    RETURNS boolean
AS $$
    SELECT EXISTS(
        SELECT 1
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relname = table_name AND a.attname = column_name AND n.nspname = 'trend'
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.data_type_order(data_type text)
    RETURNS integer
AS $$
BEGIN
    CASE data_type
        WHEN 'smallint' THEN
            RETURN 1;
        WHEN 'integer' THEN
            RETURN 2;
        WHEN 'bigint' THEN
            RETURN 3;
        WHEN 'real' THEN
            RETURN 4;
        WHEN 'double precision' THEN
            RETURN 5;
        WHEN 'numeric' THEN
            RETURN 6;
        WHEN 'timestamp without time zone' THEN
            RETURN 7;
        WHEN 'smallint[]' THEN
            RETURN 8;
        WHEN 'integer[]' THEN
            RETURN 9;
        WHEN 'text[]' THEN
            RETURN 10;
        WHEN 'text' THEN
            RETURN 11;
        WHEN NULL THEN
            RETURN NULL;
        ELSE
            RAISE EXCEPTION 'Unsupported data type: %', data_type;
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;


CREATE FUNCTION trend_directory.greatest_data_type(
        data_type_a text, data_type_b text)
    RETURNS text
AS $$
    SELECT
        CASE WHEN trend_directory.data_type_order($2) > trend_directory.data_type_order($1) THEN
            $2
        ELSE
            $1
        END;
$$ LANGUAGE sql IMMUTABLE;


CREATE AGGREGATE trend_directory.max_data_type (text)
(
    sfunc = trend_directory.greatest_data_type,
    stype = text,
    initcond = 'smallint'
);

CREATE TYPE trend_directory.upgrade_record AS (
    timestamp timestamp with time zone,
    number_of_rows integer
);


CREATE FUNCTION trend_directory.get_partition(trend_store trend_directory.trend_store, index integer)
    RETURNS trend_directory.partition
AS $$
    SELECT partition
    FROM trend_directory.partition
    WHERE trend_store_id = $1.id AND index = $2;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.define_partition(trend_store trend_directory.trend_store, index integer)
    RETURNS trend_directory.partition
AS $$
    INSERT INTO trend_directory.partition(
        trend_store_id,
        index
    )
    VALUES (
        $1.id,
        $2
    )
    RETURNING partition;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_partition(trend_store trend_directory.trend_store, index integer)
    RETURNS trend_directory.partition
AS $$
    SELECT trend_directory.create_partition_table(
        trend_directory.define_partition($1, $2)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.attributes_to_partition(
        trend_store trend_directory.trend_store, index integer)
    RETURNS trend_directory.partition
AS $$
    SELECT COALESCE(
        trend_directory.get_partition($1, $2),
        trend_directory.create_partition($1, $2)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.partition_exists(trend_directory.partition)
    RETURNS boolean
AS $$
    SELECT public.table_exists(
        trend_directory.partition_table_schema(),
        trend_directory.table_name($1)
    );
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.get_trend_store(id integer)
    RETURNS trend_directory.trend_store
AS $$
    SELECT * FROM trend_directory.trend_store WHERE id = $1
$$ LANGUAGE sql;


CREATE FUNCTION trend_directory.get_max_modified(
        trend_directory.trend_store, timestamp with time zone)
    RETURNS timestamp with time zone
AS $$
DECLARE
    max_modified timestamp with time zone;
BEGIN
    EXECUTE format(
        'SELECT max(modified) FROM trend_directory.%I WHERE timestamp = $1',
        trend_directory.base_table_name($1)
    ) INTO max_modified USING $2;

    RETURN max_modified;
END;
$$ LANGUAGE plpgsql STABLE;


CREATE FUNCTION trend_directory.update_modified(
        trend_store_id integer, "timestamp" timestamp with time zone,
        modified timestamp with time zone)
    RETURNS trend_directory.modified
AS $$
    UPDATE trend_directory.modified
    SET "end" = greatest("end", $3)
    WHERE "timestamp" = $2 AND trend_store_id = $1
    RETURNING modified;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.store_modified(
        trend_store_id integer, "timestamp" timestamp with time zone,
        modified timestamp with time zone)
    RETURNS trend_directory.modified
AS $$
    INSERT INTO trend_directory.modified(
        trend_store_id, "timestamp", start, "end"
    ) VALUES (
        $1, $2, $3, $3
    ) RETURNING modified;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.mark_modified(
        trend_store_id integer, "timestamp" timestamp with time zone,
        modified timestamp with time zone)
    RETURNS trend_directory.modified
AS $$
    SELECT COALESCE(
        trend_directory.update_modified($1, $2, $3),
        trend_directory.store_modified($1, $2, $3)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.mark_modified(
        trend_store_id integer, "timestamp" timestamp with time zone)
    RETURNS trend_directory.modified
AS $$
    SELECT COALESCE(
        trend_directory.update_modified($1, $2, now()),
        trend_directory.store_modified($1, $2, now())
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.populate_modified(partition trend_directory.partition)
    RETURNS SETOF trend_directory.modified
AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        'SELECT (trend_directory.mark_modified($1, "timestamp", max(modified))).* '
        'FROM trend_directory.%I GROUP BY timestamp',
        partition.trend_store_id, partition.table_name
    );
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION trend_directory.populate_modified(trend_directory.trend_store)
    RETURNS SETOF trend_directory.modified
AS $$
    SELECT
        trend_directory.populate_modified(partition)
    FROM trend_directory.partition
    WHERE trend_store_id = $1.id;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.available_timestamps(partition trend_directory.partition)
    RETURNS SETOF timestamp with time zone
AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        'SELECT timestamp FROM %I.%I GROUP BY timestamp',
        trend_directory.partition_table_schema(),
        trend_directory.table_name(partition)
    );
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION trend_directory.get_dependent_view_names(table_name name)
    RETURNS SETOF name
AS $$
    SELECT dst
    FROM trend_directory.view_dependencies
    WHERE src = $1
    GROUP BY dst;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.get_dependent_views(table_name name)
    RETURNS SETOF trend_directory.view
AS $$
    SELECT view
    FROM trend_directory.get_dependent_view_names($1) AS view_name
    JOIN trend_directory.view ON trend_directory.view_name(view) = view_name;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.get_dependent_views(trend_directory.trend_store)
    RETURNS SETOF trend_directory.view
AS $$
    SELECT trend_directory.get_dependent_views(trend_directory.base_table_name($1));
$$ LANGUAGE sql;


CREATE FUNCTION trend_directory.get_dependent_views(trend_store_id integer)
    RETURNS SETOF trend_directory.view
AS $$
    SELECT trend_directory.get_dependent_views(trend_store)
    FROM trend_directory.trend_store
    WHERE id = $1;
$$ LANGUAGE sql;


CREATE FUNCTION trend_directory.get_dependent_view_names(table_name name, column_name name)
    RETURNS SETOF name
AS $$
    SELECT dst
    FROM trend_directory.view_dependencies
    WHERE src = $1 AND column_name = $2
    GROUP BY dst;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.get_dependent_views(table_name name, column_name name)
    RETURNS SETOF trend_directory.view
AS $$
    SELECT view
    FROM trend_directory.get_dependent_view_names($1, $2) AS view_name
    JOIN trend_directory.view ON trend_directory.view_name(view) = view_name;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.get_dependent_view_names(table_name name, column_names name[])
    RETURNS SETOF name
AS $$
    SELECT dst
    FROM trend_directory.view_dependencies
    WHERE src = $1 AND ARRAY[column_name] <@ $2
    GROUP BY dst;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.get_dependent_views(table_name name, column_names name[])
    RETURNS SETOF trend_directory.view
AS $$
    SELECT view
    FROM trend_directory.get_dependent_view_names($1, $2) AS view_name
    JOIN trend_directory.view ON trend_directory.view_name(view) = view_name;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.define_view(trend_directory.trend_store)
    RETURNS trend_directory.view
AS $$
    INSERT INTO trend_directory.view (trend_store_id, description)
    VALUES ($1.id, $1::text)
    RETURNING view;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.define_view(trend_store_id integer)
    RETURNS trend_directory.view
AS $$
    SELECT trend_directory.define_view(trend_store)
    FROM trend_directory.trend_store
    WHERE id = $1;
$$ LANGUAGE sql VOLATILE;


CREATE TYPE trend_directory.transfer_result AS (
    row_count int,
    max_modified timestamp with time zone
);


CREATE FUNCTION trend_directory.transfer(
        source trend_directory.trend_store, target trend_directory.trend_store,
        "timestamp" timestamp with time zone, trend_names text[])
    RETURNS trend_directory.transfer_result
AS $$
DECLARE
    columns_part text;
    dst_partition trend_directory.partition;
    result trend_directory.transfer_result;
BEGIN
    SELECT
        array_to_string(array_agg(quote_ident(trend_name)), ',') INTO columns_part
    FROM unnest(
        ARRAY['entity_id', 'timestamp', 'modified'] || trend_names
    ) AS trend_name;

    dst_partition = trend_directory.attributes_to_partition(
        target,
        trend_directory.timestamp_to_index(target.partition_size, timestamp)
    );

    EXECUTE format(
        'INSERT INTO trend_directory.%I (%s) SELECT %s FROM trend_directory.%I WHERE timestamp = $1',
        dst_partition.table_name,
        columns_part,
        columns_part,
        trend_directory.base_table_name(source)
    ) USING timestamp;

    GET DIAGNOSTICS result.row_count = ROW_COUNT;

    SELECT (
        trend_directory.mark_modified(
            target.id,
            timestamp,
            trend_directory.get_max_modified(target, timestamp)
        )
    ).end INTO result.max_modified;

    RETURN result;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION trend_directory.create_trend_store(
        data_source_name character varying, entity_type_name character varying,
        granularity interval, trends trend_directory.trend_descr[])
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.initialize_trend_store(
        trend_directory.define_trend_store($1, $2, $3, $4)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_trend_store(
        data_source_name character varying, entity_type_name character varying,
        granularity interval, trends trend_directory.trend_descr[],
        partition_size integer)
    RETURNS trend_directory.trend_store
AS $$
    SELECT trend_directory.initialize_trend_store(
        trend_directory.define_trend_store($1, $2, $3, $4, $5, 'table')
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.show_trends(trend_directory.trend_store)
    RETURNS SETOF trend_directory.trend_descr
AS $$
    SELECT
        trend.name::name,
        format_type(a.atttypid, a.atttypmod)::character varying,
        trend.description
    FROM trend_directory.trend
    JOIN pg_catalog.pg_class c ON c.relname = $1::text
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attname = trend.name
    WHERE
        n.nspname = 'trend' AND
        a.attisdropped = false AND
        a.attnum > 0 AND trend.trend_store_id = $1.id;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.show_trends(trend_store_id integer)
    RETURNS SETOF trend_directory.trend_descr
AS $$
    SELECT trend_directory.show_trends(trend_store) FROM trend_directory.trend_store WHERE id = $1;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.clear_timestamp(trend_directory.trend_store, timestamp with time zone)
    RETURNS integer
AS $$
DECLARE
    row_count integer;
BEGIN
    EXECUTE format(
        'DELETE FROM %I.%I WHERE timestamp = $1',
        trend_directory.base_table_schema(),
        trend_directory.base_table_name($1)
    ) USING $2;

    GET DIAGNOSTICS row_count = ROW_COUNT;

    RETURN row_count;
END;
$$ LANGUAGE plpgsql VOLATILE;

