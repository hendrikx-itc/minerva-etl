SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = directory, pg_catalog;


CREATE OR REPLACE FUNCTION getentitybydn(character varying)
    RETURNS TABLE(
        id integer,
        entitytype_id integer,
        name character varying,
        parent_id integer)
AS $$
    SELECT id, entitytype_id, name, parent_id FROM directory.entity WHERE dn=$1;
$$ LANGUAGE sql STABLE COST 100 ROWS 1;

CREATE OR REPLACE FUNCTION getentitybyid(integer)
    RETURNS TABLE(
        dn character varying,
        entitytype_id integer,
        name character varying,
        parent_id integer)
AS $$
    SELECT dn, entitytype_id, name, parent_id FROM directory.entity WHERE id=$1;
$$ LANGUAGE sql STABLE COST 100 ROWS 1;

CREATE OR REPLACE FUNCTION addentity(timestamp with time zone, character varying(100), integer, character varying, integer)
    RETURNS integer
AS $$
    INSERT INTO directory.entity (id, first_appearance, name, entitytype_id, dn, parent_id)
    VALUES (DEFAULT, $1, $2, $3, $4, $5, $6) RETURNING id;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION directory.get_entitytype_id(character varying)
  RETURNS integer
AS $$
    SELECT id FROM directory.entitytype WHERE lower(name)=lower($1);
$$ LANGUAGE sql STABLE COST 100;


CREATE OR REPLACE FUNCTION get_entity(character varying)
    RETURNS directory.entity
AS $$
    SELECT entity FROM directory.entity WHERE dn = $1;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION entities_by_type(character varying)
    RETURNS SETOF directory.entity
AS $$
    SELECT e.*
    FROM directory.entity e
    JOIN directory.entitytype et ON et.id = e.entitytype_id
    WHERE et.name = $1;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION entities_by_type(integer)
    RETURNS SETOF directory.entity
AS $$
    SELECT *
    FROM directory.entity
    WHERE entitytype_id = $1;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION get_entitytype(character varying)
    RETURNS directory.entitytype
AS $$
    SELECT entitytype FROM directory.entitytype WHERE lower(name) = lower($1);
$$ LANGUAGE SQL STABLE STRICT;


CREATE OR REPLACE FUNCTION get_datasource(character varying)
    RETURNS directory.datasource
AS $$
    SELECT datasource FROM directory.datasource WHERE name = $1;
$$ LANGUAGE SQL STABLE STRICT;


CREATE OR REPLACE FUNCTION create_datasource(character varying)
    RETURNS directory.datasource
AS $$
    INSERT INTO directory.datasource
        (name, description, timezone)
    VALUES ($1, 'default', COALESCE(system.get_setting_value('default_timezone'), 'UTC'))
    RETURNING datasource;
$$ LANGUAGE SQL VOLATILE STRICT;


CREATE TYPE dn_part AS (type_name character varying, name character varying);


CREATE OR REPLACE FUNCTION dn_part_to_string(dn_part)
    RETURNS character varying
AS $$
    SELECT $1.type_name || '=' || $1.name;
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE CAST (directory.dn_part AS character varying)
    WITH FUNCTION directory.dn_part_to_string (directory.dn_part);


CREATE OR REPLACE FUNCTION array_to_dn_part(character varying[])
    RETURNS directory.dn_part
AS $$
    SELECT CAST(ROW($1[1], $1[2]) AS directory.dn_part);
$$ LANGUAGE SQL IMMUTABLE;


CREATE CAST (character varying[] AS directory.dn_part)
    WITH FUNCTION directory.array_to_dn_part (character varying[]);


CREATE OR REPLACE FUNCTION split_raw_part(character varying)
    RETURNS directory.dn_part
AS $$
    SELECT directory.array_to_dn_part(string_to_array($1, '='));
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION explode_dn(character varying)
    RETURNS dn_part[]
AS $$
    SELECT array_agg(directory.split_raw_part(raw_part)) FROM unnest(string_to_array($1, ',')) AS raw_part;
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION glue_dn(dn_part[])
    RETURNS character varying
AS $$
    SELECT
        array_to_string(b.part_arr, ',')
    FROM (
        SELECT array_agg(parts.p) part_arr
        FROM (
            SELECT directory.dn_part_to_string(part) p FROM unnest($1) part
        ) parts
    ) b;
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION create_entitytype(character varying)
    RETURNS directory.entitytype
AS $$
    INSERT INTO directory.entitytype(name, description) VALUES ($1, '') RETURNING entitytype;
$$ LANGUAGE SQL VOLATILE STRICT;


CREATE OR REPLACE FUNCTION parent_dn_parts(directory.dn_part[])
    RETURNS directory.dn_part[]
AS $$
    SELECT
        CASE
            WHEN array_length($1, 1) > 1 THEN
                $1[1:array_length($1, 1) - 1]
            ELSE
                NULL
        END;
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION parent_dn(character varying)
    RETURNS character varying
AS $$
    SELECT directory.glue_dn(directory.parent_dn_parts(directory.explode_dn($1)));
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION last_dn_part(directory.dn_part[])
    RETURNS directory.dn_part
AS $$
    SELECT $1[array_length($1, 1)];
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION create_entity(character varying)
    RETURNS directory.entity
AS $$
    INSERT INTO directory.entity(first_appearance, name, entitytype_id, dn, parent_id)
        VALUES (
            now(),
            (directory.last_dn_part(directory.explode_dn($1))).name,
            directory.entitytype_id(directory.name_to_entitytype((directory.last_dn_part(directory.explode_dn($1))).type_name)),
            $1,
            directory.entity_id(directory.dn_to_entity(directory.glue_dn(directory.parent_dn_parts(directory.explode_dn($1)))))
        )
        RETURNING entity;
$$ LANGUAGE SQL VOLATILE STRICT;


CREATE OR REPLACE FUNCTION get_alias(entity_id integer, aliastype_name character varying)
    RETURNS character varying
AS $$
    SELECT a.name 
      FROM directory.alias a 
      JOIN directory.aliastype at on at.id = a.type_id
     WHERE a.entity_id = $1 and at.name = $2;
$$ LANGUAGE sql STABLE;


CREATE OR REPLACE FUNCTION dn_to_entity(character varying)
    RETURNS directory.entity
AS $$
    SELECT COALESCE(directory.get_entity($1), directory.create_entity($1));
$$ LANGUAGE SQL VOLATILE STRICT;


CREATE OR REPLACE FUNCTION entity_id(directory.entity)
    RETURNS integer
AS $$
    SELECT $1.id;
$$ LANGUAGE SQL VOLATILE STRICT;


CREATE OR REPLACE FUNCTION entitytype_id(directory.entitytype)
    RETURNS integer
AS $$
    SELECT $1.id;
$$ LANGUAGE SQL VOLATILE STRICT;


CREATE OR REPLACE FUNCTION name_to_datasource(character varying)
    RETURNS directory.datasource
AS $$
    SELECT COALESCE(directory.get_datasource($1), directory.create_datasource($1));
$$ LANGUAGE SQL VOLATILE STRICT;


CREATE OR REPLACE FUNCTION name_to_entitytype(character varying)
    RETURNS directory.entitytype
AS $$
    SELECT COALESCE(directory.get_entitytype($1), directory.create_entitytype($1));
$$ LANGUAGE SQL VOLATILE STRICT;


CREATE OR REPLACE FUNCTION dns_to_entity_ids(character varying[])
    RETURNS SETOF integer
AS $$
    SELECT (directory.dn_to_entity(dn)).id FROM unnest($1) dn;
$$ LANGUAGE SQL VOLATILE STRICT;


CREATE TYPE query_part AS (c text[], s text);

CREATE TYPE query_row AS (id integer, dn text, entitytype_id integer);

------------------------------------------
-- Example usage of run_minerva_query:
--
-- SELECT run_minerva_query(ARRAY[(ARRAY['Cell']::text[], '15000')]::query_part[]);
--
-- SELECT run_minerva_query(ARRAY[(ARRAY['Site']::text[], '4343'), (ARRAY['Cell', '3G']::text[], NULL)]::query_part[]);
------------------------------------------

CREATE OR REPLACE FUNCTION run_minerva_query(query query_part[])
    RETURNS TABLE(id integer, dn varchar, entitytype_id integer)
AS $$
BEGIN
    RETURN QUERY EXECUTE compile_minerva_query(query);
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION sumproduct(query query_part[], value_trend text, weight_trend text)
    RETURNS TABLE("timestamp" timestamp with time zone, wavg float)
AS $$
DECLARE
    sql text;
BEGIN
    sql = compile_minerva_query(query);

    sql = format('SELECT t1.timestamp, CAST(SUM(t1."CCR" * t2."Traffic_Full") AS double precision) FROM (' || sql || ') e
JOIN trend."tnpmw-ccr_cell_day_20120521" t1 ON t1.entity_id = e.id
JOIN trend."tnpmw-traffic_cell_day_20120521" t2 ON t2.entity_id = e.id AND t2.timestamp = t1.timestamp
WHERE t1.timestamp = %L
GROUP BY t1.timestamp;', '2012-06-02');

    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION wavg(query query_part[], value_trend_id integer, weight_trend_id integer)
    RETURNS TABLE("timestamp" timestamp with time zone, wavg float)
AS $$
DECLARE
    sql text;
BEGIN
    sql = compile_minerva_query(query);

    sql = format('SELECT t1.timestamp, CAST(SUM(t1."CCR" * t2."Traffic_Full") / SUM(t2."Traffic_Full") AS double precision) FROM (' || sql || ') e
JOIN trend."tnpmw-ccr_cell_day_20120521" t1 ON t1.entity_id = e.id
JOIN trend."tnpmw-traffic_cell_day_20120521" t2 ON t2.entity_id = e.id AND t2.timestamp = t1.timestamp
WHERE t1.timestamp = %L
GROUP BY t1.timestamp;', '2012-06-02');

    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION compile_minerva_query(query text)
    RETURNS text
AS $$
DECLARE
    parts text[];
    c_str text;
    cs text[];
    s_str text;
    minerva_query query_part[];
BEGIN
    parts = regexp_split_to_array(query, E'(\\w)[ ]+(?=\\w)');

    for i in 1..2 LOOP
        c_str = parts[i];
        cs = regexp_split_to_array(c_str, E'[+ ]+');
        s_str = parts[i + 1];

        minerva_query = minerva_query || (cs, s_str)::query_part;
    end loop;

    return compile_minerva_query(minerva_query);
END;
$$ LANGUAGE plpgsql STABLE STRICT;

------------------------------------------
-- Example usage of compile_minerva_query:
--
-- SELECT compile_minerva_query(ARRAY[(ARRAY['Cell']::text[], '15000')]::query_part[]);
------------------------------------------

CREATE OR REPLACE FUNCTION compile_minerva_query(query directory.query_part[])
    RETURNS text
AS $$
DECLARE
    sql text;
    entity_id_table text;
    entity_id_column text;
    q_part directory.query_part;
    tag text;
BEGIN
    sql = 'SELECT entity.id, entity.dn, entity.entitytype_id FROM directory.entitytaglink tl_1_1';

    entity_id_table = 'tl_1_1';
    entity_id_column = 'entity_id';

    FOR index IN array_lower(query, 1)..array_upper(query, 1) LOOP
        q_part = query[index];

        IF index > 1 THEN
            sql = sql || format(' JOIN directory.relation r_%s ON r_%s.source_id = %I.%I', index, index, entity_id_table, entity_id_column);

            entity_id_table = format('r_%s', index);
            entity_id_column = 'target_id';
        END IF;

        FOR i IN array_lower(q_part.c, 1)..array_upper(q_part.c, 1) LOOP
            tag = q_part.c[i];

            sql = sql || directory.make_c_join(index, entity_id_table, entity_id_column, i, tag);
        END LOOP;

        IF NOT q_part.s IS NULL THEN
            sql = sql || directory.make_s_join(index, entity_id_table, entity_id_column, q_part.s);
        END IF;

    END LOOP;

    RETURN sql || format(' JOIN directory.entity entity ON entity.id = %I.%I', entity_id_table, entity_id_column);
END;
$$ LANGUAGE plpgsql STABLE STRICT;


CREATE OR REPLACE FUNCTION make_c_join(index integer, entity_id_table text, entity_id_column text, tag_index integer, tag text)
    RETURNS text
AS $$
DECLARE
    entitytaglink_alias text;
    entitytag_alias text;
BEGIN
    entitytaglink_alias = 'tl_' || index || '_' || tag_index;
    entitytag_alias = 't_' || index || '_' || tag_index;

    IF NOT entity_id_table = entitytaglink_alias THEN
        RETURN format(' JOIN directory.entitytaglink %I ON %I.%I = %I.entity_id', entitytaglink_alias, entity_id_table, entity_id_column, entitytaglink_alias) ||
            format(' JOIN directory.entitytag %I ON %I.id = %I.entitytag_id AND lower(%I.name) = lower(%L)', entitytag_alias, entitytag_alias, entitytaglink_alias, entitytag_alias, tag);
    ELSE
        RETURN format(' JOIN directory.entitytag %I ON %I.id = %I.entitytag_id AND lower(%I.name) = lower(%L)', entitytag_alias, entitytag_alias, entitytaglink_alias, entitytag_alias, tag);
    END IF;
END;
$$ LANGUAGE plpgsql STABLE STRICT;


CREATE OR REPLACE FUNCTION make_s_join(index integer, entity_id_table text, entity_id_column text, alias text)
    RETURNS text
AS $$
DECLARE
    alias_alias text;
    aliastype_alias text;
BEGIN
    alias_alias = 'a_' || index;
    aliastype_alias = 'at_' || index;

    RETURN format(' JOIN directory.alias %I ON %I.entity_id = %I.%I', alias_alias, alias_alias, entity_id_table, entity_id_column) ||
        format(' JOIN directory.aliastype %I ON %I.id = %I.type_id and %I.name = %L AND lower(%I.name) = lower(%L)', aliastype_alias, aliastype_alias, alias_alias, aliastype_alias, 'name', alias_alias, alias);
END;
$$ LANGUAGE plpgsql STABLE STRICT;


CREATE OR REPLACE FUNCTION tag_entity(entity_id integer, tag character varying)
    RETURNS integer 
AS $$
    INSERT INTO directory.entitytaglink(tag_id, entity_id) SELECT id, $1 FROM directory.tag WHERE name = $2 RETURNING $1;
$$ LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION tag_entity(dn character varying, tag character varying)
    RETURNS character varying 
AS $$
    INSERT INTO directory.entitytaglink(tag_id, entity_id)
    SELECT tag.id, entity.id
    FROM directory.tag, directory.entity
    WHERE tag.name = $2 AND entity.dn = $1 RETURNING $1;
$$ LANGUAGE SQL VOLATILE;