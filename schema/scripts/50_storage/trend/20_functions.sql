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


CREATE FUNCTION trend_directory.base_table_name(trendstore trend_directory.trendstore)
    RETURNS name
AS $$
    SELECT format(
        '%s_%s_%s',
        datasource.name,
        entitytype.name,
        trend_directory.granularity_to_text($1.granularity)
    )::name
    FROM directory.datasource, directory.entitytype
    WHERE datasource.id = $1.datasource_id AND entitytype.id = $1.entitytype_id;
$$ LANGUAGE sql STABLE STRICT;


CREATE FUNCTION trend_directory.to_char(trend_directory.trendstore)
    RETURNS text
AS $$
    SELECT trend_directory.base_table_name($1)::text;
$$ LANGUAGE sql STABLE STRICT;


CREATE FUNCTION trend_directory.to_char(trend_directory.view)
    RETURNS text
AS $$
    SELECT trendstore::text FROM trend_directory.trendstore WHERE id = $1.trendstore_id;
$$ LANGUAGE sql STABLE STRICT;


CREATE FUNCTION trend_directory.get_trendstore_by_attributes(
        datasource_name character varying, entitytype_name character varying,
        granularity interval)
    RETURNS trend_directory.trendstore
AS $$
    SELECT ts
    FROM trend_directory.trendstore ts
    JOIN directory.datasource ds ON ds.id = ts.datasource_id
    JOIN directory.entitytype et ON et.id = ts.entitytype_id
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


CREATE FUNCTION trend_directory.create_base_table(trend_directory.trendstore, trend_directory.trend[])
    RETURNS trend_directory.trendstore
AS $$
    SELECT trend_directory.create_base_table(trend_directory.base_table_name($1), $2);
    SELECT $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.get_trendstore_trends(trend_directory.trendstore)
    RETURNS trend_directory.trend[]
AS $$
    SELECT COALESCE(array_agg(trend), ARRAY[]::trend_directory.trend[])
    FROM trend_directory.trend
    WHERE trendstore_id = $1.id
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.create_base_table(trend_directory.trendstore)
    RETURNS trend_directory.trendstore
AS $$
    SELECT trend_directory.create_base_table(
        $1,
        trend_directory.get_trendstore_trends($1)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.staging_table_name(trend_directory.trendstore)
    RETURNS name
AS $$
    SELECT (trend_directory.base_table_name($1) || '_staging')::name;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.create_staging_table_sql(trendstore trend_directory.trendstore)
    RETURNS text[]
AS $$
SELECT ARRAY[
    format('CREATE UNLOGGED TABLE trend.%I () INHERITS (trend.%I);', trend_directory.staging_table_name($1), trend_directory.base_table_name(trendstore)),
    format('ALTER TABLE ONLY trend.%I ADD PRIMARY KEY (entity_id, "timestamp");', trend_directory.staging_table_name($1)),
    format('ALTER TABLE trend.%I OWNER TO minerva_writer;', trend_directory.staging_table_name($1)),
    format('GRANT SELECT ON TABLE trend.%I TO minerva;', trend_directory.staging_table_name($1)),
    format('GRANT INSERT,DELETE,UPDATE ON TABLE trend.%I TO minerva_writer;', trend_directory.staging_table_name($1))
];
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.create_staging_table(trendstore trend_directory.trendstore)
    RETURNS trend_directory.trendstore
AS $$
    SELECT public.action($1, trend_directory.create_staging_table_sql($1));
$$ LANGUAGE sql VOLATILE STRICT;


CREATE FUNCTION trend_directory.initialize_trendstore(trend_directory.trendstore)
    RETURNS trend_directory.trendstore
AS $$
    SELECT trend_directory.create_base_table($1);
    SELECT trend_directory.create_staging_table($1);

    SELECT $1;
$$ LANGUAGE sql VOLATILE;

COMMENT ON FUNCTION trend_directory.initialize_trendstore(trend_directory.trendstore) IS
'Create all database objects required for the trendstore to be fully functional
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


CREATE FUNCTION trend_directory.define_trendstore(
        datasource_name character varying, entitytype_name character varying,
        granularity interval, partition_size integer,
        type trend_directory.storetype)
    RETURNS trend_directory.trendstore
AS $$
    INSERT INTO trend_directory.trendstore (
        datasource_id,
        entitytype_id,
        granularity,
        partition_size,
        type
    )
    VALUES (
        (directory.name_to_datasource($1)).id,
        (directory.name_to_entitytype($2)).id,
        $3,
        $4,
        $5
    ) RETURNING *;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.define_trendstore(
        datasource_name character varying, entitytype_name character varying,
        granularity interval
    )
    RETURNS trend_directory.trendstore
AS $$
    SELECT trend_directory.define_trendstore(
        $1,
        $2,
        $3,
        trend_directory.get_default_partition_size($3),
        'table'
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.define_trendstore(
        datasource_name character varying, entitytype_name character varying,
        granularity interval, type trend_directory.storetype)
    RETURNS trend_directory.trendstore
AS $$
    SELECT trend_directory.define_trendstore(
        $1,
        $2,
        $3,
        trend_directory.get_default_partition_size($3),
        $4
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_trendstore(
        datasource_name character varying, entitytype_name character varying,
        granularity interval)
    RETURNS trend_directory.trendstore
AS $$
    SELECT trend_directory.initialize_trendstore(
        trend_directory.define_trendstore($1, $2, $3)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.define_trends(
        trend_directory.trendstore,
        trend_directory.trend_descr[])
    RETURNS trend_directory.trendstore
AS $$
    INSERT INTO trend_directory.trend(name, data_type, trendstore_id, description) (
        SELECT name, data_type, $1.id, description FROM unnest($2)
    );

    SELECT $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.define_trendstore(
        datasource_name character varying, entitytype_name character varying,
        granularity interval, trends trend_directory.trend_descr[],
        partition_size integer, type trend_directory.storetype)
    RETURNS trend_directory.trendstore
AS $$
    SELECT trend_directory.define_trends(
        trend_directory.define_trendstore($1, $2, $3, $5, $6),
        $4
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.define_trendstore(
        datasource_name character varying, entitytype_name character varying,
        granularity interval, trends trend_directory.trend_descr[])
    RETURNS trend_directory.trendstore
AS $$
    SELECT trend_directory.define_trends(
        trend_directory.define_trendstore($1, $2, $3),
        $4
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_trendstore(
        datasource_name character varying, entitytype_name character varying,
        granularity interval, type trend_directory.storetype)
    RETURNS trend_directory.trendstore
AS $$
    SELECT trend_directory.initialize_trendstore(
        trend_directory.define_trendstore($1, $2, $3, $4)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.attributes_to_trendstore(
        datasource_name character varying, entitytype_name character varying,
        granularity interval)
    RETURNS trend_directory.trendstore
AS $$
    SELECT COALESCE(
        trend_directory.get_trendstore_by_attributes($1, $2, $3),
        trend_directory.create_trendstore($1, $2, $3)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.attributes_to_view_trendstore(
        datasource_name character varying, entitytype_name character varying,
        granularity interval)
    RETURNS trend_directory.trendstore
AS $$
    SELECT COALESCE(
        trend_directory.get_trendstore_by_attributes($1, $2, $3),
        trend_directory.define_trendstore($1, $2, $3, 'view'::trend_directory.storetype)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.partition_name(trendstore trend_directory.trendstore, index integer)
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
        trend_directory.trendstore, timestamp with time zone)
    RETURNS name
AS $$
    SELECT trend_directory.partition_name(
        $1, trend_directory.timestamp_to_index($1.partition_size, $2)
    );
$$ LANGUAGE sql STABLE STRICT;


CREATE FUNCTION trend_directory.table_name(trend_directory.partition)
    RETURNS name
AS $$
    SELECT trend_directory.partition_name(trendstore, $1.index)
    FROM trend_directory.trendstore
    WHERE id = $1.trendstore_id;
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


CREATE FUNCTION trend_directory.trendstore(trend_directory.partition)
    RETURNS trend_directory.trendstore
AS $$
    SELECT * FROM trend_directory.trendstore WHERE id = $1.trendstore_id;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.staged_timestamps(trendstore trend_directory.trendstore)
    RETURNS SETOF timestamp with time zone
AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        'SELECT timestamp FROM %I.%I GROUP BY timestamp',
        trend_directory.staging_table_schema(),
        trend_directory.staging_table_name(trendstore)
    );
END;
$$ LANGUAGE plpgsql STABLE;


CREATE FUNCTION trend_directory.transfer_staged(trendstore trend_directory.trendstore, "timestamp" timestamp with time zone)
    RETURNS integer
AS $$
DECLARE
    row_count integer;
BEGIN
    EXECUTE format(
        'INSERT INTO %I.%I SELECT * FROM %I.%I WHERE timestamp = $1',
        trend_directory.partition_table_schema(),
        trend_directory.table_name(trend_directory.attributes_to_partition(
            trendstore,
            trend_directory.timestamp_to_index(trendstore.partition_size, timestamp)
        )),
        trend_directory.staging_table_schema(),
        trend_directory.staging_table_name(trendstore)
    ) USING timestamp;

    GET DIAGNOSTICS row_count = ROW_COUNT;

    RETURN row_count;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION trend_directory.transfer_staged(trendstore trend_directory.trendstore)
    RETURNS void
AS $$
    SELECT
        trend_directory.transfer_staged(trendstore, timestamp)
    FROM trend_directory.staged_timestamps(trendstore) timestamp;

    SELECT public.action(format(
        'TRUNCATE %I.%I',
        trend_directory.staging_table_schema(),
        trend_directory.staging_table_name(trendstore)
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
        partition_name varchar, trend_id integer, datatype varchar)
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
        datatype
    );
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION trend_directory.render_view_query(view_id integer)
    RETURNS text
AS $$
    SELECT sql FROM trend_directory.view WHERE id = view_id;
$$ LANGUAGE sql;


CREATE FUNCTION trend_directory.modify_partition_column(
        partition_name varchar, column_name varchar, datatype varchar)
    RETURNS void
AS $$
BEGIN
    PERFORM dep_recurse.alter(
        dep_recurse.table_ref('trend', base_table_name),
        ARRAY[
            format(
                'ALTER TABLE trend_directory.%I ALTER %I TYPE %s USING CAST(%I AS %s);',
                partition_name,
                column_name,
                datatype,
                column_name,
                datatype
            )
        ]
    );
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION trend_directory.modify_trendstore_column(
        trendstore_id integer, column_name varchar, datatype varchar)
    RETURNS void
AS $$
DECLARE
    base_table_name varchar;
BEGIN
    SELECT trend_directory.base_table_name(trendstore) INTO base_table_name
        FROM trend_directory.trendstore
        WHERE trendstore.id = trendstore_id;

    PERFORM dep_recurse.alter(
        dep_recurse.table_ref('trend', base_table_name),
        ARRAY[
            format(
                'ALTER TABLE trend_directory.%I ALTER %I TYPE %s USING CAST(%I AS %s);',
                base_table_name,
                column_name,
                datatype,
                column_name,
                datatype
            )
        ]
    );
END;
$$ LANGUAGE plpgsql;


CREATE TYPE trend_directory.column_info AS (
    name varchar,
    datatype varchar
);


CREATE FUNCTION trend_directory.table_columns(namespace name, "table" name)
    RETURNS SETOF trend_directory.column_info
AS $$
    SELECT
        a.attname::character varying,
        format_type(a.atttypid, a.atttypmod)::character varying
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


CREATE FUNCTION trend_directory.modify_trendstore_columns(
        trendstore_id integer, columns trend_directory.column_info[])
    RETURNS void
AS $$
DECLARE
    dependent_views trend_directory.view[];
BEGIN
    IF array_length(columns, 1) IS NULL THEN
        RETURN;
    END IF;

    SELECT array_agg(trend_directory.drop_view(dependent_view)) INTO dependent_views
        FROM trend_directory.get_dependent_views(trendstore_id) dependent_view;

    PERFORM
        trend_directory.alter_column_types(
            'trend',
            trend_directory.base_table_name(trendstore),
            columns
        )
    FROM trend_directory.trendstore
    WHERE trendstore.id = trendstore_id;

    PERFORM trend_directory.create_view(dependent_view)
        FROM unnest(dependent_views) AS dependent_view;
END;
$$ LANGUAGE plpgsql;


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
                            c.datatype,
                            c.name,
                            c.datatype
                        )
                    ),
                    ', '
                )
            )
        ]
    )
    FROM unnest(columns) AS c;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.view_name(trend_directory.view)
    RETURNS name
AS $$
    SELECT trend_directory.base_table_name(trendstore)
    FROM trend_directory.trendstore
    WHERE trendstore.id = $1.trendstore_id;
$$ LANGUAGE sql;


CREATE FUNCTION trend_directory.drop_view(view trend_directory.view)
    RETURNS trend_directory.view
AS $$
BEGIN
    EXECUTE format(
        'DROP VIEW IF EXISTS %I.%I',
        trend_directory.view_schema(),
        trend_directory.view_name(view)
    );

    PERFORM trend_directory.unlink_view_dependencies(view);

    PERFORM trend_directory.delete_view_trends(view);

    RETURN view;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION trend_directory.update_view_sql(trend_directory.view, text)
    RETURNS trend_directory.view
AS $$
    UPDATE trend_directory.view SET sql = $2 WHERE id = $1.id RETURNING *;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.alter_view(trend_directory.view, text)
    RETURNS trend_directory.view
AS $$
DECLARE
    result trend_directory.view;
BEGIN
    result = trend_directory.update_view_sql($1, $2);

    PERFORM dep_recurse.alter(
        dep_recurse.view_ref('trend', $1::text),
        ARRAY[
            format('SELECT trend_directory.recreate_view(view) FROM trend_directory.view WHERE id = %L', $1.id)
        ]
    );

    RETURN result;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION trend_directory.create_view_sql(trend_directory.view)
    RETURNS text[]
AS $$
SELECT ARRAY[
    format('CREATE VIEW %I.%I AS %s;', trend_directory.view_schema(), trend_directory.view_name($1), $1.sql),
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
    INSERT INTO trend_directory.view_trendstore_link (view_id, trendstore_id)
    SELECT $1.id, trendstore.id
    FROM trend_directory.view_dependencies vdeps
    JOIN trend_directory.trendstore ON trend_directory.base_table_name(trendstore) = vdeps.src
    LEFT JOIN trend_directory.view_trendstore_link vtl ON vtl.view_id = $1.id AND vtl.trendstore_id = trendstore.id
    WHERE vdeps.dst = trend_directory.view_name($1) AND vtl.view_id IS NULL
    GROUP BY trendstore.id
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
        name name, data_type text, trendstore_id integer,
        description text)
    RETURNS trend_directory.trend
AS $$
    INSERT INTO trend_directory.trend (name, data_type, trendstore_id, description)
    VALUES ($1, $2, $3, $4)
    RETURNING trend;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.add_trend_to_trendstore(
        trend_directory.trendstore, trend_directory.trend)
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


CREATE FUNCTION trend_directory.add_trend_to_trendstore(
         trend_directory.trendstore, name,
        data_type text, description text)
    RETURNS trend_directory.trend
AS $$
    SELECT trend_directory.add_trend_to_trendstore(
        $1,
        trend_directory.define_trend($2, $3, $1.id, $4)
    )
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_trends(trend_directory.trendstore, trend_directory.trend_descr[])
    RETURNS SETOF trend_directory.trend
AS $$
    SELECT trend_directory.add_trend_to_trendstore(
        $1,
        name,
        data_type,
        ''
    )
    FROM unnest($2);
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.get_trendstore(view trend_directory.view)
    RETURNS trend_directory.trendstore
AS $$
    SELECT trendstore FROM trend_directory.trendstore WHERE id = $1.trendstore_id;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.create_view_trends(view trend_directory.view)
    RETURNS SETOF trend_directory.trend
AS $$
    SELECT
        trend_directory.define_trend(
            vt.name,
            vt.data_type,
            id(trend_directory.get_trendstore($1)),
            vt.description
        )
    FROM trend_directory.get_view_trends(trend_directory.view_name($1)) vt
    WHERE vt.name NOT IN ('entity_id', 'timestamp', 'samples');
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_view(trend_directory.view)
    RETURNS trend_directory.view
AS $$
    SELECT public.action($1, trend_directory.create_view_sql($1));

    SELECT trend_directory.link_view_dependencies($1);

    SELECT trend_directory.create_view_trends($1);

    SELECT $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_view(text)
    RETURNS trend_directory.view
AS $$
    SELECT trend_directory.create_view(view) FROM trend_directory.view WHERE view::text = $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.recreate_view(view trend_directory.view)
    RETURNS trend_directory.view
AS $$
    SELECT trend_directory.create_view(trend_directory.drop_view($1));
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.recreate_view(text)
    RETURNS trend_directory.view
AS $$
    SELECT trend_directory.create_view(trend_directory.drop_view(view))
    FROM trend_directory.view
    WHERE view::text = $1;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.delete_view_trends(view trend_directory.view)
    RETURNS void
AS $$
    DELETE FROM trend_directory.trend
    WHERE trendstore_id = $1.trendstore_id;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.unlink_view_dependencies(trend_directory.view)
    RETURNS trend_directory.view
AS $$
    DELETE FROM trend_directory.view_trendstore_link
    WHERE view_id = $1.id
    RETURNING $1;
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
        (trend_directory.trendstore($1)).partition_size, $1.index
    );
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.data_end(trend_directory.partition)
    RETURNS timestamp with time zone
AS $$
    SELECT trend_directory.index_to_timestamp(
        (trend_directory.trendstore($1)).partition_size, $1.index + 1
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
            trend_directory.base_table_name(trend_directory.trendstore($1))
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
        trendstore trend_directory.trendstore, trend_name name)
    RETURNS trend_directory.trend
AS $$
    SELECT trend
    FROM trend_directory.trend
    WHERE trend.trendstore_id = $1.id AND name = $2;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.get_trends_for_trendstore(trendstore_id integer)
    RETURNS SETOF trend_directory.trend
AS $$
    SELECT * FROM trend_directory.trend WHERE trend.trendstore_id = $1;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.get_trends_for_trendstore(trend_directory.trendstore)
    RETURNS SETOF trend_directory.trend
AS $$
    SELECT trend_directory.get_trends_for_trendstore($1.id);
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.trendstore_has_trend_with_name(
        trendstore trend_directory.trendstore, trend_name character varying)
    RETURNS boolean
AS $$
    SELECT exists(
        SELECT 1
        FROM trend_directory.trendstore_trend_link ttl
        JOIN trend_directory.trend t ON ttl.trend_id = t.id
        WHERE ttl.trendstore_id = $1.id AND t.name = $2
    );
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.attributes_to_trend(trend_directory.trendstore, name name, data_type text)
    RETURNS trend_directory.trend
AS $$
    SELECT COALESCE(
        trend_directory.get_trend($1, $2),
        trend_directory.define_trend($2, $3, $1.id, '')
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.remove_trend_from_trendstore(
        trendstore trend_directory.trendstore, trend_name character varying)
    RETURNS trend_directory.trendstore
AS $$
    DELETE FROM trend_directory.trend WHERE trendstore_id = $1.id;

    SELECT public.action(
        $1,
        format(
            'ALTER TABLE trend_directory.%I DROP COLUMN %I;',
            trend_directory.base_table_name(trendstore),
            trend_name
        )
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.remove_trend_from_trendstore(
        trendstore text, trend_name character varying)
    RETURNS trend_directory.trendstore
AS $$
    SELECT trend_directory.remove_trend_from_trendstore(trendstore, $2)
    FROM trend_directory.trendstore
    WHERE trendstore::text = $1;
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


CREATE FUNCTION trend_directory.datatype_order(data_type character varying)
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
            RAISE EXCEPTION 'Unsupported data type: %', datatype;
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;


CREATE FUNCTION trend_directory.greatest_datatype(
        datatype_a character varying, datatype_b character varying)
    RETURNS character varying
AS $$
BEGIN
    IF trend_directory.datatype_order(datatype_b) > trend_directory.datatype_order(datatype_a) THEN
        RETURN datatype_b;
    ELSE
        RETURN datatype_a;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE AGGREGATE trend_directory.max_datatype (character varying)
(
    sfunc = trend_directory.greatest_datatype,
    stype = character varying,
    initcond = 'smallint'
);

CREATE TYPE trend_directory.upgrade_record AS (
    timestamp timestamp with time zone,
    number_of_rows integer
);


CREATE FUNCTION trend_directory.get_partition(trendstore trend_directory.trendstore, index integer)
    RETURNS trend_directory.partition
AS $$
    SELECT partition
    FROM trend_directory.partition
    WHERE trendstore_id = $1.id AND index = $2;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.create_partition(trendstore trend_directory.trendstore, index integer)
    RETURNS trend_directory.partition
AS $$
    INSERT INTO trend_directory.partition(
        trendstore_id,
        index
    )
    VALUES (
        $1.id,
        $2
    )
    RETURNING partition;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.attributes_to_partition(
        trendstore trend_directory.trendstore, index integer)
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


CREATE FUNCTION trend_directory.get_trendstore(id integer)
    RETURNS trend_directory.trendstore
AS $$
    SELECT * FROM trend_directory.trendstore WHERE id = $1
$$ LANGUAGE sql;


CREATE FUNCTION trend_directory.get_max_modified(
        trend_directory.trendstore, timestamp with time zone)
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
        trendstore_id integer, "timestamp" timestamp with time zone,
        modified timestamp with time zone)
    RETURNS trend_directory.modified
AS $$
    UPDATE trend_directory.modified
    SET "end" = greatest("end", $3)
    WHERE "timestamp" = $2 AND trendstore_id = $1
    RETURNING modified;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.store_modified(
        trendstore_id integer, "timestamp" timestamp with time zone,
        modified timestamp with time zone)
    RETURNS trend_directory.modified
AS $$
    INSERT INTO trend_directory.modified(
        trendstore_id, "timestamp", start, "end"
    ) VALUES (
        $1, $2, $3, $3
    ) RETURNING modified;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.mark_modified(
        trendstore_id integer, "timestamp" timestamp with time zone,
        modified timestamp with time zone)
    RETURNS trend_directory.modified
AS $$
    SELECT COALESCE(
        trend_directory.update_modified($1, $2, $3),
        trend_directory.store_modified($1, $2, $3)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.mark_modified(
        trendstore_id integer, "timestamp" timestamp with time zone)
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
        partition.trendstore_id, partition.table_name
    );
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION trend_directory.populate_modified(trend_directory.trendstore)
    RETURNS SETOF trend_directory.modified
AS $$
    SELECT
        trend_directory.populate_modified(partition)
    FROM trend_directory.partition
    WHERE trendstore_id = $1.id;
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


CREATE FUNCTION trend_directory.get_dependent_views(trend_directory.trendstore)
    RETURNS SETOF trend_directory.view
AS $$
    SELECT trend_directory.get_dependent_views(trend_directory.base_table_name($1));
$$ LANGUAGE sql;


CREATE FUNCTION trend_directory.get_dependent_views(trendstore_id integer)
    RETURNS SETOF trend_directory.view
AS $$
    SELECT trend_directory.get_dependent_views(trendstore)
    FROM trend_directory.trendstore
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


CREATE FUNCTION trend_directory.define_view(trendstore_id integer, sql text)
    RETURNS trend_directory.view
AS $$
    INSERT INTO trend_directory.view (trendstore_id, description, sql)
    (SELECT $1, trendstore::text, $2 FROM trend_directory.trendstore WHERE id = $1)
    RETURNING view;
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.define_view(trend_directory.trendstore, sql text)
    RETURNS trend_directory.view
AS $$
    INSERT INTO trend_directory.view (trendstore_id, description, sql)
    VALUES ($1.id, $1::text, $2)
    RETURNING view;
$$ LANGUAGE sql VOLATILE;


CREATE TYPE trend_directory.transfer_result AS (
    row_count int,
    max_modified timestamp with time zone
);


CREATE FUNCTION trend_directory.transfer(
        source trend_directory.trendstore, target trend_directory.trendstore,
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


CREATE FUNCTION trend_directory.create_trendstore(
        datasource_name character varying, entitytype_name character varying,
        granularity interval, trends trend_directory.trend_descr[])
    RETURNS trend_directory.trendstore
AS $$
    SELECT trend_directory.initialize_trendstore(
        trend_directory.define_trendstore($1, $2, $3, $4)
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.create_trendstore(
        datasource_name character varying, entitytype_name character varying,
        granularity interval, trends trend_directory.trend_descr[],
        partition_size integer)
    RETURNS trend_directory.trendstore
AS $$
    SELECT trend_directory.initialize_trendstore(
        trend_directory.define_trendstore($1, $2, $3, $4, $5, 'table')
    );
$$ LANGUAGE sql VOLATILE;


CREATE FUNCTION trend_directory.show_trends(trend_directory.trendstore)
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
        a.attnum > 0 AND trend.trendstore_id = $1.id;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.show_trends(trendstore_id integer)
    RETURNS SETOF trend_directory.trend_descr
AS $$
    SELECT trend_directory.show_trends(trendstore) FROM trend_directory.trendstore WHERE id = $1;
$$ LANGUAGE sql STABLE;


CREATE FUNCTION trend_directory.clear_timestamp(trend_directory.trendstore, timestamp with time zone)
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

