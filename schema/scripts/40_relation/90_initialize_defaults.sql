SET search_path = relation, pg_catalog;


INSERT INTO "type" (name) VALUES ('self');

-- Dummy relations to satisfy geospatial requirements
SELECT relation.define_reverse(
    'HandoverRelation->Cell',
    relation.define(
        'Cell->HandoverRelation',
        $$SELECT 0 as source_id, 0 as target_id WHERE false;$$
    )
);

SELECT relation.define(
    'real_handover',
    $$SELECT 0 as source_id, 0 as target_id WHERE false;$$
);
