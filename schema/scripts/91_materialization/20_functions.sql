CREATE FUNCTION materialization.to_char(materialization.type)
    RETURNS text
AS $$
    SELECT trend_directory.base_table_name(src) || ' -> ' || trend_directory.base_table_name(dst)
    FROM trend_directory.trendstore src, trend_directory.trendstore dst
    WHERE src.id = $1.src_trendstore_id AND dst.id = $1.dst_trendstore_id
$$ LANGUAGE SQL STABLE STRICT;


CREATE FUNCTION materialization.add_new_state()
    RETURNS integer
AS $$
DECLARE
    count integer;
BEGIN
    INSERT INTO materialization.state(type_id, timestamp, max_modified, source_states)
    SELECT type_id, timestamp, max_modified, source_states
    FROM materialization.new_materializables;

    GET DIAGNOSTICS count = ROW_COUNT;

    RETURN count;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION materialization.update_modified_state()
    RETURNS integer
AS $$
DECLARE
    count integer;
BEGIN
    UPDATE materialization.state
    SET
        max_modified = mzb.max_modified,
        source_states = mzb.source_states
    FROM materialization.modified_materializables mzb
    WHERE
        state.type_id = mzb.type_id AND
        state.timestamp = mzb.timestamp;

    GET DIAGNOSTICS count = ROW_COUNT;

    RETURN count;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION materialization.delete_obsolete_state()
    RETURNS integer
AS $$
DECLARE
    count integer;
BEGIN
    DELETE FROM materialization.state
    USING materialization.obsolete_state
    WHERE
        state.type_id = obsolete_state.type_id AND
        state.timestamp = obsolete_state.timestamp;

    GET DIAGNOSTICS count = ROW_COUNT;

    RETURN count;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION materialization.update_state()
    RETURNS text
AS $$
    SELECT 'added: ' || materialization.add_new_state() || ', updated: ' || materialization.update_modified_state() || ', deleted: ' || materialization.delete_obsolete_state();
$$ LANGUAGE SQL VOLATILE;


CREATE FUNCTION materialization.missing_columns(src trend_directory.trendstore, dst trend_directory.trendstore)
    RETURNS TABLE (name character varying, datatype character varying)
AS $$
    SELECT name, datatype
    FROM trend_directory.table_columns('trend', trend_directory.base_table_name($1))
    WHERE name NOT IN (
        SELECT name FROM trend_directory.table_columns('trend', trend_directory.base_table_name($2))
    );
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION materialization.missing_columns(src trend_directory.trendstore, dst trend_directory.trendstore)
IS 'The set of table columns (name, datatype) that exist in the source trendstore but not yet in the destination.';


CREATE FUNCTION materialization.missing_columns(materialization.type)
    RETURNS TABLE (name character varying, datatype character varying)
AS $$
    SELECT materialization.missing_columns(src, dst)
    FROM trend_directory.trendstore src, trend_directory.trendstore dst
    WHERE src.id = $1.src_trendstore_id AND dst.id = $1.dst_trendstore_id;
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION materialization.add_missing_trends(src trend_directory.trendstore, dst trend_directory.trendstore)
    RETURNS bigint
AS $$
    SELECT count(trend_directory.add_trend_to_trendstore($2, name, datatype, 'auto-created by materialization'))
    FROM materialization.missing_columns($1, $2);
$$ LANGUAGE SQL VOLATILE;

COMMENT ON FUNCTION materialization.add_missing_trends(src trend_directory.trendstore, dst trend_directory.trendstore)
IS 'Add trends and actual table columns to destination that exist in the source
trendstore but not yet in the destination.';


CREATE FUNCTION materialization.add_missing_trends(materialization.type)
    RETURNS materialization.type
AS $$
    SELECT materialization.add_missing_trends(src, dst)
    FROM trend_directory.trendstore src, trend_directory.trendstore dst
    WHERE src.id = $1.src_trendstore_id AND dst.id = $1.dst_trendstore_id;

    SELECT $1;
$$ LANGUAGE SQL VOLATILE;


CREATE FUNCTION materialization.modify_mismatching_trends(src trend_directory.trendstore, dst trend_directory.trendstore)
    RETURNS void
AS $$
    SELECT trend_directory.modify_trendstore_columns($2.id, array_agg(src_column))
    FROM trend_directory.table_columns('trend', trend_directory.base_table_name($1)) src_column
    JOIN trend_directory.table_columns('trend', trend_directory.base_table_name($2)) dst_column ON
        src_column.name = dst_column.name
            AND
        src_column.datatype <> dst_column.datatype;
$$ LANGUAGE SQL VOLATILE;


CREATE FUNCTION materialization.modify_mismatching_trends(materialization.type)
    RETURNS void
AS $$
    SELECT materialization.modify_mismatching_trends(src, dst)
    FROM trend_directory.trendstore src, trend_directory.trendstore dst
    WHERE src.id = $1.src_trendstore_id AND dst.id = $1.dst_trendstore_id;
$$ LANGUAGE SQL VOLATILE;


CREATE FUNCTION materialization.materialize(type materialization.type, "timestamp" timestamp with time zone)
    RETURNS integer
AS $$
DECLARE
    src_trendstore trend_directory.trendstore;
    dst_trendstore trend_directory.trendstore;
    table_name character varying;
    dst_table_name character varying;
    dst_partition trend_directory.partition;
    sources_query character varying;
    data_query character varying;
    conn_str character varying;
    columns_part character varying;
    column_defs_part character varying;
    modified timestamp with time zone;
    row_count integer;
    replicated_server_conn system.setting;
    tmp_source_states materialization.source_fragment_state[];
BEGIN
    SELECT * INTO src_trendstore FROM trend_directory.trendstore WHERE id = type.src_trendstore_id;
    SELECT * INTO dst_trendstore FROM trend_directory.trendstore WHERE id = type.dst_trendstore_id;

    table_name = trend_directory.base_table_name(src_trendstore);
    dst_table_name = trend_directory.base_table_name(dst_trendstore);

    PERFORM materialization.add_missing_trends(src_trendstore, dst_trendstore);
    PERFORM materialization.modify_mismatching_trends(src_trendstore, dst_trendstore);

    dst_partition = trend_directory.attributes_to_partition(
        dst_trendstore,
        trend_directory.timestamp_to_index(dst_trendstore.partition_size, "timestamp")
    );

    PERFORM trend_directory.remove_timestamp(dst_trendstore, timestamp);

    SELECT
        array_to_string(array_agg(quote_ident(name)), ', ') INTO columns_part
    FROM
        trend_directory.table_columns(trend_directory.base_table_schema(), table_name);

    sources_query = format(
        'SELECT source_states
    FROM materialization.materializables mz
    JOIN materialization.type ON type.id = mz.type_id
    WHERE
        mz.timestamp = %L AND mz.type_id = %L;',
        "timestamp",
        type.id
    );

    data_query = format(
        'SELECT %s FROM %I.%I WHERE timestamp = %L',
        columns_part, trend_directory.base_table_schema(), table_name, timestamp
    );

    replicated_server_conn = system.get_setting('replicated_server_conn');

    IF replicated_server_conn IS NULL THEN
        -- Local materialization
        EXECUTE sources_query INTO tmp_source_states;
        EXECUTE format(
            'INSERT INTO %I.%I (%s) %s',
            trend_directory.partition_table_schema(),
            trend_directory.table_name(dst_partition),
            columns_part,
            data_query
        );
    ELSE
        -- Remote materialization
        conn_str = replicated_server_conn.value;

        SELECT
            array_to_string(array_agg(format('%I %s', col.name, col.datatype)), ', ') INTO column_defs_part
        FROM
            trend_directory.table_columns(trend_directory.base_table_schema(), table_name) col;

        SELECT sources INTO tmp_source_states
        FROM public.dblink(conn_str, sources_query) AS r (sources materialization.source_fragment_state[]);

        EXECUTE format(
            'INSERT INTO %I.%I (%s) SELECT * FROM public.dblink(%L, %L) AS rel (%s)',
            trend_directory.partition_table_name(),
            trend_directory.table_name(dst_partition),
            columns_part,
            conn_str,
            data_query,
            column_defs_part
        );
    END IF;

    GET DIAGNOSTICS row_count = ROW_COUNT;

    UPDATE materialization.state
    SET processed_states = tmp_source_states
    WHERE state.type_id = $1.id AND state.timestamp = $2;

    IF row_count = 0 THEN
        RAISE NOTICE 'NO ROWS materialized FOR materialization of %, %', $1::text, timestamp;
        RETURN row_count;
    END IF;

    PERFORM trend_directory.mark_modified($1.dst_trendstore_id, "timestamp");

    RETURN row_count;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION materialization.materialize(materialization text, "timestamp" timestamp with time zone)
    RETURNS integer
AS $$
    SELECT materialization.materialize(type, $2)
    FROM materialization.type
    WHERE materialization.to_char(type) = $1;
$$ LANGUAGE SQL VOLATILE;


CREATE FUNCTION materialization.materialize(id integer, "timestamp" timestamp with time zone)
    RETURNS integer
AS $$
    SELECT materialization.materialize(type, $2)
    FROM materialization.type
    WHERE id = $1;
$$ LANGUAGE SQL VOLATILE;


CREATE FUNCTION materialization.default_processing_delay(granularity character varying)
    RETURNS interval
AS $$
    SELECT CASE
        WHEN $1 = '1800' OR $1 = '900' OR $1 = '300' THEN
            interval '0 seconds'
        WHEN $1 = '3600' THEN
            interval '15 minutes'
        ELSE
            interval '3 hours'
        END;
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION materialization.default_stability_delay(granularity character varying)
    RETURNS interval
AS $$
    SELECT CASE
        WHEN $1 = '1800' OR $1 = '900' OR $1 = '300' THEN
            interval '180 seconds'
        WHEN $1 = '3600' THEN
            interval '5 minutes'
        ELSE
            interval '15 minutes'
        END;
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION materialization.define(src_trendstore_id integer, dst_trendstore_id integer)
    RETURNS materialization.type
AS $$
    INSERT INTO materialization.type (src_trendstore_id, dst_trendstore_id, processing_delay, stability_delay, reprocessing_period)
    SELECT $1, $2, materialization.default_processing_delay(granularity), materialization.default_stability_delay(granularity), interval '3 days'
    FROM trend_directory.trendstore WHERE id = $2
    RETURNING type;
$$ LANGUAGE SQL VOLATILE;


CREATE FUNCTION materialization.define(src trend_directory.trendstore, dst trend_directory.trendstore)
    RETURNS materialization.type
AS $$
    SELECT materialization.define($1.id, $2.id);
$$ LANGUAGE SQL VOLATILE;


CREATE FUNCTION materialization.define(text, text)
    RETURNS materialization.type
AS $$
    SELECT
        materialization.define(src.id, dst.id)
    FROM
        trend_directory.trendstore src,
        trend_directory.trendstore dst
    WHERE src::text = $1 AND dst::text = $2;
$$ LANGUAGE SQL VOLATILE;


CREATE FUNCTION materialization.define(trend_directory.trendstore)
    RETURNS materialization.type
AS $$
    SELECT materialization.add_missing_trends(
        materialization.define(
            $1,
            trend_directory.attributes_to_trendstore(substring(ds.name, '^v(.*)'), et.name, ts.granularity)
        )
    )
    FROM trend_directory.view
    JOIN trend_directory.trendstore ts on ts.id = view.trendstore_id
    JOIN directory.datasource ds on ds.id = ts.datasource_id
    JOIN directory.entitytype et on et.id = ts.entitytype_id
    WHERE view.trendstore_id = $1.id;
$$ LANGUAGE SQL VOLATILE;

COMMENT ON FUNCTION materialization.define(trend_directory.trendstore)
IS 'Defines a new materialization with the convention that the datasource of
the source trendstore should start with a ''v'' for views and that the
destination trendstore has the same properties except for a datasource with a
name without the leading ''v''. A new trendstore and datasource are created if
they do not exist.';


CREATE FUNCTION materialization.materialized_datasource_name(name character varying)
  RETURNS character varying
AS $$
BEGIN
  IF NOT name ~ '^v.*' THEN
    RAISE EXCEPTION '% does not start with a ''v''', name;
  ELSE
    RETURN substring(name, '^v(.*)');
  END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE FUNCTION materialization.define(trend_directory.view)
    RETURNS materialization.type
AS $$
SELECT materialization.add_missing_trends(
    materialization.define(
        ts,
        trend_directory.attributes_to_trendstore(
            materialization.materialized_datasource_name(ds.name),
            et.name,
            ts.granularity
        )
    )
)
FROM trend_directory.trendstore ts
JOIN directory.datasource ds on ds.id = ts.datasource_id
JOIN directory.entitytype et on et.id = ts.entitytype_id
WHERE ts.id = $1.trendstore_id;
$$ LANGUAGE SQL VOLATILE;

COMMENT ON FUNCTION materialization.define(trend_directory.view)
IS 'Defines a new materialization with the convention that the datasource of
the source trendstore should start with a ''v'' for views and that the
destination trendstore has the same properties except for a datasource with a
name without the leading ''v''. A new trendstore and datasource are created if
they do not exist.';


CREATE FUNCTION materialization.render_job_json(type_id integer, timestamp with time zone)
    RETURNS character varying
AS $$
    SELECT format('{"type_id": %s, "timestamp": "%s"}', $1, $2);
$$ LANGUAGE SQL IMMUTABLE;


CREATE FUNCTION materialization.create_job(type_id integer, "timestamp" timestamp with time zone)
    RETURNS integer
AS $$
DECLARE
    description text;
    new_job_id integer;
BEGIN
    description := materialization.render_job_json(type_id, "timestamp");

    SELECT system.create_job('materialize', description, 1, job_source.id) INTO new_job_id
        FROM system.job_source
        WHERE name = 'compile-materialize-jobs';

    UPDATE materialization.state
        SET job_id = new_job_id
        WHERE state.type_id = $1 AND state.timestamp = $2;

    RETURN new_job_id;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE FUNCTION materialization.source_data_ready(type materialization.type, "timestamp" timestamp with time zone, max_modified timestamp with time zone)
    RETURNS boolean
AS $$
    SELECT
        $2 < now() - $1.processing_delay AND
        $3 < now() - $1.stability_delay;
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION materialization.runnable(type materialization.type, "timestamp" timestamp with time zone, max_modified timestamp with time zone)
    RETURNS boolean
AS $$
    SELECT
        $1.enabled AND
        materialization.source_data_ready($1, $2, $3) AND
        ($1.reprocessing_period IS NULL OR now() - $2 < $1.reprocessing_period);
$$ LANGUAGE SQL IMMUTABLE;


CREATE FUNCTION materialization.runnable(materialization.type, materialization.state)
    RETURNS boolean
AS $$
    SELECT materialization.runnable($1, $2.timestamp, $2.max_modified);
$$ LANGUAGE SQL IMMUTABLE;


CREATE FUNCTION materialization.open_job_slots(slot_count integer)
    RETURNS integer
AS $$
    SELECT greatest($1 - COUNT(*), 0)::integer
    FROM system.job
    WHERE type = 'materialize' AND (state = 'running' OR state = 'queued');
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION materialization.runnable_materializations(tag varchar)
    RETURNS TABLE (type_id integer, "timestamp" timestamp with time zone)
AS $$
DECLARE
    runnable_materializations_query text;
    conn_str text;
    replicated_server_conn system.setting;
BEGIN
    replicated_server_conn = system.get_setting('replicated_server_conn');

    IF replicated_server_conn IS NULL THEN
        RETURN QUERY SELECT trm.type_id, trm.timestamp
        FROM materialization.tagged_runnable_materializations trm
        WHERE trm.tag = $1;
    ELSE
        runnable_materializations_query = format('SELECT type_id, timestamp
            FROM materialization.tagged_runnable_materializations
            WHERE tag = %L', tag);

        RETURN QUERY SELECT replicated_state.type_id, replicated_state.timestamp
        FROM public.dblink(replicated_server_conn.value, runnable_materializations_query)
            AS replicated_state(type_id integer, "timestamp" timestamp with time zone)
        JOIN materialization.tagged_runnable_materializations rj ON
            replicated_state.type_id = rj.type_id
                AND
            replicated_state.timestamp = rj.timestamp
        WHERE materialization.no_slave_lag();
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION materialization.runnable_materializations(tag varchar)
IS 'Return table with all combinations (type_id, timestamp) that are ready to
run. This includes the check between the master and slave states.';


CREATE FUNCTION materialization.create_jobs(tag varchar, job_limit integer)
    RETURNS integer
AS $$
    SELECT COUNT(materialization.create_job(type_id, timestamp))::integer
    FROM (
        SELECT type_id, timestamp
        FROM materialization.runnable_materializations($1)
        LIMIT materialization.open_job_slots($2)
    ) mzs;
$$ LANGUAGE SQL;


CREATE FUNCTION materialization.create_jobs(tag varchar)
    RETURNS integer
AS $$
    SELECT COUNT(materialization.create_job(type_id, timestamp))::integer
    FROM materialization.runnable_materializations($1);
$$ LANGUAGE SQL;


CREATE FUNCTION materialization.create_jobs_limited(tag varchar, job_limit integer)
    RETURNS integer
AS $$
    SELECT materialization.create_jobs($1, $2);
$$ LANGUAGE SQL;

COMMENT ON FUNCTION materialization.create_jobs_limited(tag varchar, job_limit integer)
IS 'Deprecated function that just calls the overloaded create_jobs function.';


CREATE FUNCTION materialization.tag(tag_name character varying, type_id integer)
    RETURNS materialization.type_tag_link
AS $$
    INSERT INTO materialization.type_tag_link (type_id, tag_id)
    SELECT $2, tag.id FROM directory.tag WHERE name = $1
    RETURNING *;
$$ LANGUAGE SQL VOLATILE;

COMMENT ON FUNCTION materialization.tag(character varying, type_id integer)
IS 'Add tag with name tag_name to materialization type with id type_id.
The tag must already exist.';


CREATE FUNCTION materialization.tag(tag_name character varying, materialization.type)
    RETURNS materialization.type
AS $$
    INSERT INTO materialization.type_tag_link (type_id, tag_id)
    SELECT $2.id, tag.id FROM directory.tag WHERE name = $1
    RETURNING $2;
$$ LANGUAGE SQL VOLATILE;

COMMENT ON FUNCTION materialization.tag(character varying, materialization.type)
IS 'Add tag with name tag_name to materialization type. The tag must already exist.';


CREATE FUNCTION materialization.untag(materialization.type)
    RETURNS materialization.type
AS $$
    DELETE FROM materialization.type_tag_link WHERE type_id = $1.id RETURNING $1;
$$ LANGUAGE SQL VOLATILE;

COMMENT ON FUNCTION materialization.untag(materialization.type)
IS 'Remove all tags from the materialization';


CREATE FUNCTION materialization.reset(type_id integer)
    RETURNS SETOF materialization.state
AS $$
    UPDATE materialization.state SET processed_states = NULL
    WHERE
        type_id = $1 AND
        source_states = processed_states
    RETURNING *;
$$ LANGUAGE SQL VOLATILE;


CREATE FUNCTION materialization.reset_hard(materialization.type)
    RETURNS void
AS $$
    DELETE FROM trend_directory.partition WHERE trendstore_id = $1.dst_trendstore_id;
    DELETE FROM materialization.state WHERE type_id = $1.id;
$$ LANGUAGE SQL VOLATILE;

COMMENT ON FUNCTION materialization.reset_hard(materialization.type)
IS 'Remove data (partitions) resulting from this materialization and the
corresponding state records, so materialization for all timestamps can be done
again';


CREATE FUNCTION materialization.reset(type_id integer, timestamp with time zone)
    RETURNS materialization.state
AS $$
    UPDATE materialization.state SET processed_states = NULL
    WHERE type_id = $1 AND timestamp = $2
    RETURNING *;
$$ LANGUAGE SQL VOLATILE;


CREATE FUNCTION materialization.reset(materialization.type, timestamp with time zone)
    RETURNS materialization.state
AS $$
    SELECT materialization.reset($1.id, $2);
$$ LANGUAGE SQL VOLATILE;


CREATE FUNCTION materialization.enable(materialization.type)
    RETURNS materialization.type
AS $$
    UPDATE materialization.type SET enabled = true WHERE id = $1.id RETURNING type;
$$ LANGUAGE SQL VOLATILE;


CREATE FUNCTION materialization.disable(materialization.type)
    RETURNS materialization.type
AS $$
    UPDATE materialization.type SET enabled = false WHERE id = $1.id RETURNING type;
$$ LANGUAGE SQL VOLATILE;


CREATE FUNCTION materialization.fragments(materialization.source_fragment_state[])
    RETURNS materialization.source_fragment[]
AS $$
    SELECT array_agg(fragment) FROM unnest($1);
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION materialization.requires_update(materialization.state)
    RETURNS boolean
AS $$
    SELECT (
        $1.source_states <> $1.processed_states AND
        materialization.fragments($1.source_states) @> materialization.fragments($1.processed_states)
    )
    OR $1.processed_states IS NULL;
$$ LANGUAGE SQL STABLE;


-- Stub to allow recursive definition.
CREATE FUNCTION materialization.dependencies(trend_directory.trendstore, level integer)
    RETURNS TABLE(trendstore trend_directory.trendstore, level integer)
AS $$
    SELECT $1, $2;
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION materialization.direct_view_dependencies(trend_directory.trendstore)
    RETURNS SETOF trend_directory.trendstore
AS $$
    SELECT trendstore
    FROM trend_directory.trendstore
    JOIN trend_directory.view_trendstore_link vtl ON vtl.trendstore_id = trendstore.id
    JOIN trend_directory.view ON view.id = vtl.view_id
    WHERE view.trendstore_id = $1.id;
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION materialization.direct_table_dependencies(trend_directory.trendstore)
    RETURNS SETOF trend_directory.trendstore
AS $$
    SELECT trendstore
    FROM trend_directory.trendstore
    JOIN materialization.type ON type.src_trendstore_id = trendstore.id
    WHERE dst_trendstore_id = $1.id;
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION materialization.direct_dependencies(trend_directory.trendstore)
    RETURNS SETOF trend_directory.trendstore
AS $$
    SELECT
    CASE WHEN $1.type = 'view' THEN
        materialization.direct_view_dependencies($1)
    WHEN $1.type = 'table' THEN
        materialization.direct_table_dependencies($1)
    END;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION materialization.dependencies(trend_directory.trendstore, level integer)
    RETURNS TABLE(trendstore trend_directory.trendstore, level integer)
AS $$
    SELECT (d.dependencies).* FROM (
        SELECT materialization.dependencies(dependency, $2 + 1)
        FROM materialization.direct_dependencies($1) dependency
    ) d
    UNION ALL
    SELECT dependency, $2
    FROM materialization.direct_dependencies($1) dependency;
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION materialization.dependencies(trend_directory.trendstore)
    RETURNS TABLE(trendstore trend_directory.trendstore, level integer)
AS $$
    SELECT materialization.dependencies($1, 1);
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION materialization.dependencies(name text)
    RETURNS TABLE(trendstore trend_directory.trendstore, level integer)
AS $$
    SELECT materialization.dependencies(trendstore)
    FROM trend_directory.trendstore
    WHERE trend_directory.to_char(trendstore) = $1;
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION materialization.no_slave_lag()
    RETURNS boolean
    LANGUAGE sql
AS $$SELECT bytes_lag < 10000000
FROM metric.replication_lag
WHERE client_addr = '192.168.42.19';$$;


-- View 'runnable_materializations'

CREATE view materialization.runnable_materializations AS
SELECT type, state
FROM materialization.state
JOIN materialization.type ON type.id = state.type_id
WHERE
    materialization.requires_update(state)
    AND
    materialization.runnable(type, materialization.state."timestamp", materialization.state.max_modified);

ALTER VIEW materialization.runnable_materializations OWNER TO minerva_admin;


-- View 'next_up_materializations'

CREATE VIEW materialization.next_up_materializations AS
SELECT
    type_id,
    timestamp,
    (tag).name,
    cost,
    cumsum,
    resources AS group_resources,
    (job.id IS NOT NULL AND job.state IN ('queued', 'running')) AS job_active
FROM
(
    SELECT
        (rm.type).id AS type_id,
        (rm.state).timestamp,
        tag,
        (rm.type).cost,
        sum((rm.type).cost) over (partition by tag.name order by trend_directory.granularity_seconds(ts.granularity) asc, (rm.state).timestamp desc, rm.type) as cumsum,
        (rm.state).job_id
    FROM materialization.runnable_materializations rm
    JOIN trend_directory.trendstore ts ON ts.id = (rm.type).dst_trendstore_id
    JOIN materialization.type_tag_link ttl ON ttl.type_id = (rm.type).id
    JOIN directory.tag ON tag.id = ttl.tag_id
) summed
JOIN materialization.group_priority ON (summed.tag).id = group_priority.tag_id
LEFT JOIN system.job ON job.id = job_id
WHERE cumsum <= group_priority.resources;

ALTER VIEW materialization.next_up_materializations OWNER TO minerva_admin;


CREATE FUNCTION materialization.create_jobs()
    RETURNS integer
    LANGUAGE sql
AS $function$
    SELECT COUNT(materialization.create_job(num.type_id, timestamp))::integer
    FROM materialization.next_up_materializations num
    WHERE NOT job_active AND materialization.no_slave_lag();
$function$;

