SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

-- Function: gis.get_handovers
-- DROP FUNCTION gis.get_handovers(integer);
CREATE OR REPLACE FUNCTION gis.get_handovers(IN integer)
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
$BODY$
	SELECT	src_rel.source_id, src_rel.target_id, trg_rel.target_id, 
		trg_et.name, src_et.name , et.name, 'OUT', 
		array_to_string( 
			array_agg(
				ex.exists || ',' || date_part('epoch',ex.timestamp)
			), ';')

	FROM relation."Cell->HandoverRelation" src_rel
	JOIN relation."HandoverRelation->Cell" trg_rel on src_rel.target_id=trg_rel.source_id
	JOIN directory.entity src_et ON src_et.id = src_rel.source_id
	JOIN directory.entity trg_et ON trg_et.id = trg_rel.target_id
	JOIN directory.entitytaglink etl on etl.entity_id = trg_et.id
	JOIN directory.taggroup etg on etg.name = 'generation'
	JOIN directory.tag et on etl.tag_id = et.id  and etg.id = et.taggroup_id
	JOIN relation.real_handover real_ho on real_ho.source_id = src_rel.target_id
	JOIN directory.existence ex on ex.entity_id = real_ho.target_id
	WHERE src_rel.source_id = $1
	GROUP BY src_rel.source_id, src_rel.target_id, trg_rel.target_id, trg_et.name, src_et.name , et.name, real_ho.source_id
		
	UNION
		
	SELECT 	src_rel.source_id, src_rel.target_id, trg_rel.target_id,
		trg_et.name, src_et.name , et.name, 'IN',
		array_to_string( 
			array_agg(
				ex.exists || ',' || date_part('epoch',ex.timestamp)
			), ';')

	FROM relation."Cell->HandoverRelation" src_rel 
	JOIN relation."HandoverRelation->Cell" trg_rel on src_rel.target_id=trg_rel.source_id 
	JOIN directory.entity src_et ON src_et.id=src_rel.source_id 
	JOIN directory.entity trg_et ON trg_et.id = trg_rel.target_id 
	JOIN directory.entitytaglink etl on etl.entity_id=src_et.id 
	JOIN directory.taggroup etg on etg.name = 'generation'
	JOIN directory.tag et on etl.tag_id = et.id  and etg.id = et.taggroup_id
	JOIN relation.real_handover real_ho on real_ho.source_id = src_rel.target_id 
	JOIN directory.existence ex on ex.entity_id = real_ho.target_id 
	WHERE trg_rel.target_id = $1
	GROUP BY src_rel.source_id, src_rel.target_id, trg_rel.target_id, trg_et.name, src_et.name , et.name, real_ho.source_id

$BODY$
  LANGUAGE sql STABLE
  COST 100
  ROWS 1000;


ALTER FUNCTION gis.get_handovers(integer) OWNER TO postgres;

