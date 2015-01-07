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

GRANT ALL ON TABLE gis.handover_relation TO minerva_admin;
GRANT SELECT ON TABLE gis.handover_relation TO minerva;


CREATE TYPE gis.existence_change AS (exists boolean, unix_timestamp double precision);

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
GROUP BY
    handover_relation.source_entity_id,
    handover_relation.ho_entity_id,
    handover_relation.target_entity_id,
    trg_et.name,
    src_et.name,
    tag.name,
    handover_relation.direction;

GRANT ALL ON TABLE gis.handover_relation_existence TO minerva_admin;
GRANT SELECT ON TABLE gis.handover_relation_existence TO minerva;


CREATE OR REPLACE VIEW gis.handoverrelation_tags AS
    SELECT entitytaglink.entity_id entity_id, array_agg( tag.name ) tags
    FROM directory.entitytaglink
    JOIN directory.tag ON entitytaglink.tag_id = tag.id
    JOIN directory.taggroup ON taggroup.id = tag.taggroup_id AND taggroup.name = 'handover'
    GROUP BY entitytaglink.entity_id;

GRANT ALL ON TABLE gis.handoverrelation_tags TO minerva_admin;
GRANT SELECT ON TABLE gis.handoverrelation_tags TO minerva;

CREATE OR REPLACE FUNCTION gis.get_handovers(integer)
  RETURNS TABLE(source_id integer, handover_id integer, target_id integer, target_name character varying, source_name character varying, tag_name character varying, direction text, existence text[], handover_tags character varying[]) AS
$BODY$
    SELECT
	source_id,
	handover_id,
	target_id,
	target_name,
	source_name,
	tag_name,
	direction,
	existence,
        handoverrelation_tags.tags
    FROM (SELECT
		handover_relation.source_entity_id source_id,
		handover_relation.ho_entity_id handover_id,
		handover_relation.target_entity_id target_id,
		trg_et.name target_name,
		src_et.name source_name,
		tag.name tag_name,
		handover_relation.direction,
	        array_agg(
	            existence.exists || ',' || date_part('epoch', existence.timestamp)
		) existence
	    FROM gis.handover_relation
	    JOIN directory.entity src_et ON src_et.id = handover_relation.source_entity_id
	    JOIN directory.entity trg_et ON trg_et.id = handover_relation.target_entity_id
	    JOIN directory.entitytaglink etl ON etl.entity_id = handover_relation.neighbour_entity_id
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
		tag.name,
		handover_relation.direction ) t
    LEFT JOIN gis.handoverrelation_tags ON handoverrelation_tags.entity_id = t.handover_id
$BODY$
  LANGUAGE sql STABLE;


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
