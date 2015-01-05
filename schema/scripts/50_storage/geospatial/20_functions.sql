CREATE OR REPLACE VIEW gis.handover_relation AS
SELECT
    cell_ho.source_id AS cell_entity_id,
    'OUT'::text direction,
    ho_cell.target_id AS neighbour_entity_id,

    cell_ho.source_id AS source_entity_id,
    ho_cell.target_id AS target_entity_id,
    cell_ho.target_id AS ho_entity_id
FROM relation."Cell->HandoverRelation" cell_ho
JOIN relation."HandoverRelation->Cell" ho_cell on cell_ho.target_id = ho_cell.source_id

UNION ALL

SELECT
    ho_cell.target_id AS cell_entity_id,
    'IN'::text direction,
    cell_ho.source_id AS neighbour_entity_id,

    cell_ho.source_id AS source_entity_id,
    ho_cell.target_id AS target_entity_id,
    cell_ho.target_id AS ho_entity_id
FROM relation."Cell->HandoverRelation" cell_ho
JOIN relation."HandoverRelation->Cell" ho_cell on cell_ho.target_id = ho_cell.source_id;


CREATE OR REPLACE VIEW gis.handover_relation_existence AS
SELECT
    handover_relation.source_entity_id source_id,
    handover_relation.ho_entity_id handover_id,
    handover_relation.target_entity_id target_id, 
    trg_et.name target_name,
    src_et.name source_name,
    tag.name tag_name,
    handover_relation.direction, 
    array_agg(
        (existence.exists, date_part('epoch', existence.timestamp))
    ) existence
FROM gis.handover_relation
JOIN directory.entity src_et ON src_et.id = handover_relation.source_entity_id
JOIN directory.entity trg_et ON trg_et.id = handover_relation.target_entity_id
JOIN directory.entitytaglink etl ON etl.entity_id = trg_et.id
JOIN directory.tag ON etl.tag_id = tag.id 
JOIN directory.taggroup etg ON etg.id = tag.taggroup_id AND etg.name = 'generation'
JOIN relation.real_handover ON real_handover.source_id = handover_relation.ho_entity_id
JOIN directory.existence existence ON existence.entity_id = real_handover.target_id
GROUP BY
    handover_relation.source_entity_id,
    handover_relation.ho_entity_id,
    handover_relation.target_entity_id,
    trg_et.name,
    src_et.name,
    tag.name,
    handover_relation.direction;


CREATE TYPE gis.existence_change AS (exists boolean, unix_timestamp double precision);


CREATE OR REPLACE FUNCTION gis.get_handovers_new(integer)
    RETURNS TABLE(
        source_id integer,
        handover_id integer,
        target_id integer,
        target_name character varying, 
        source_name character varying, 
        tag_name character varying, 
        direction text,
        existance gis.existence_change[]
    ) AS
$$
    SELECT
        handover_relation.source_entity_id source_id,
        handover_relation.ho_entity_id handover_id,
        handover_relation.target_entity_id target_id, 
        trg_et.name target_name,
        src_et.name source_name,
        tag.name tag_name,
        'OUT'::text direction, 
        array_agg(
            (existence.exists, date_part('epoch', existence.timestamp))::gis.existence_change
        ) existence
    FROM gis.handover_relation
    JOIN directory.entity src_et ON src_et.id = handover_relation.source_entity_id
    JOIN directory.entity trg_et ON trg_et.id = handover_relation.target_entity_id
    JOIN directory.entitytaglink etl ON etl.entity_id = trg_et.id
    JOIN directory.tag ON etl.tag_id = tag.id 
    JOIN directory.taggroup etg ON etg.id = tag.taggroup_id AND etg.name = 'generation'
    JOIN relation.real_handover ON real_handover.source_id = handover_relation.ho_entity_id
    JOIN directory.existence existence ON existence.entity_id = real_handover.target_id
    WHERE handover_relation.cell_entity_id = $1
    GROUP BY
        handover_relation.source_entity_id,
        handover_relation.ho_entity_id,
        handover_relation.target_entity_id,
        trg_et.name,
        src_et.name,
        tag.name

$$ LANGUAGE sql STABLE;


-- Function: gis.get_handovers

CREATE OR REPLACE FUNCTION gis.get_handovers(integer)
    RETURNS TABLE(
        source_id integer,
        handover_id integer,
        target_id integer,
        target_name character varying, 
        source_name character varying, 
        tag_name character varying, 
        direction text,
        existance text
    ) AS
$$
    SELECT
        src_rel.source_id source_id,
        src_rel.target_id handover_id,
        trg_rel.target_id target_id, 
        trg_et.name target_name,
        src_et.name source_name,
        et.name tag_name,
        'OUT' direction, 
        array_to_string( 
            array_agg(
                ex.exists || ',' || date_part('epoch', ex.timestamp)
            ),
            ';'
        ) existance
    FROM relation."Cell->HandoverRelation" src_rel
    JOIN relation."HandoverRelation->Cell" trg_rel ON src_rel.target_id = trg_rel.source_id
    JOIN directory.entity src_et ON src_et.id = src_rel.source_id
    JOIN directory.entity trg_et ON trg_et.id = trg_rel.target_id
    JOIN directory.entitytaglink etl ON etl.entity_id = trg_et.id
    JOIN directory.taggroup etg ON etg.name = 'generation'
    JOIN directory.tag et ON etl.tag_id = et.id  and etg.id = et.taggroup_id
    JOIN relation.real_handover real_ho ON real_ho.source_id = src_rel.target_id
    JOIN directory.existence ex ON ex.entity_id = real_ho.target_id
    WHERE src_rel.source_id = $1
    GROUP BY
        src_rel.source_id,
        src_rel.target_id,
        trg_rel.target_id,
        trg_et.name,
        src_et.name,
        et.name,
        real_ho.source_id
        
    UNION
        
    SELECT
        src_rel.source_id source_id,
        src_rel.target_id handover_id,
        trg_rel.target_id target_id,
        trg_et.name target_name,
        src_et.name source_name,
        et.name tag_name,
        'IN' direction,
        array_to_string( 
            array_agg(
                ex.exists || ',' || date_part('epoch', ex.timestamp)
            ),
            ';'
        ) existance
    FROM relation."Cell->HandoverRelation" src_rel 
    JOIN relation."HandoverRelation->Cell" trg_rel ON src_rel.target_id=trg_rel.source_id 
    JOIN directory.entity src_et ON src_et.id = src_rel.source_id 
    JOIN directory.entity trg_et ON trg_et.id = trg_rel.target_id 
    JOIN directory.entitytaglink etl ON etl.entity_id = src_et.id 
    JOIN directory.taggroup etg ON etg.name = 'generation'
    JOIN directory.tag et ON etl.tag_id = et.id AND etg.id = et.taggroup_id
    JOIN relation.real_handover real_ho ON real_ho.source_id = src_rel.target_id 
    JOIN directory.existence ex ON ex.entity_id = real_ho.target_id 
    WHERE trg_rel.target_id = $1
    GROUP BY
        src_rel.source_id,
        src_rel.target_id,
        trg_rel.target_id,
        trg_et.name,
        src_et.name,
        et.name,
        real_ho.source_id

$$ LANGUAGE sql STABLE;


-- Function: gis.get_changed_handover_cells(timestamp with time zone)

CREATE OR REPLACE FUNCTION gis.get_changed_handover_cells(timestamp with time zone)
  RETURNS SETOF integer AS
$$
SELECT id FROM (
    SELECT sc.source_id as id FROM (
        SELECT e.id as id 
        FROM directory.entity e
        JOIN directory.entitytype et on et.id = e.entitytype_id
        JOIN relation.real_handover real_ho on real_ho.source_id = e.id
        JOIN directory.existence ex on ex.entity_id = real_ho.target_id
        WHERE et.name = 'HandoverRelation'and ex.timestamp > $1
         ) ho
    JOIN relation."Cell->HandoverRelation" sc on sc.target_id = ho.id
    UNION
    SELECT tc.source_id as id FROM (
        SELECT e.id as id
        FROM directory.entity e
        JOIN directory.entitytype et on et.id = e.entitytype_id
        JOIN relation.real_handover real_ho on real_ho.source_id = e.id
        JOIN directory.existence ex on ex.entity_id = real_ho.target_id
        WHERE et.name = 'HandoverRelation'and ex.timestamp > $1
         ) ho
    JOIN relation."HandoverRelation->Cell" tc on tc.source_id = ho.id
) t group by t.id order by t.id

$$ LANGUAGE sql STABLE;
