BEGIN;

SELECT plan(3);


SELECT trend_directory.create_trend_store(
    'test-data',
    'Node',
    '900',
    ARRAY[
        ('x', 'integer', 'some column with integer values')
    ]::trend_directory.trend_descr[]
);


INSERT INTO trend."test-data_Node_qtr_staging"(
    entity_id,
    timestamp,
    modified,
    x
)
VALUES
    (id(directory.dn_to_entity('Network=G01,Node=A001')), '2015-01-21 15:00+00', now(), 42),
    (id(directory.dn_to_entity('Network=G01,Node=A002')), '2015-01-21 15:00+00', now(), 43);


SELECT trend_directory.transfer_staged(trend_store)
FROM trend_directory.trend_store
WHERE trend_store::text = 'test-data_Node_qtr';


SELECT materialization.define(
    trend_directory.create_view(
        trend_directory.define_view(
        trend_directory.attributes_to_view_trend_store('vtest', 'Node', '900'),
        $view_def$SELECT
    entity_id,
    timestamp,
    modified,
    x
FROM trend."test-data_Node_qtr"$view_def$
        )
    )
);


SELECT has_table(
    'trend',
    'test_Node_qtr',
    'materialized trend table should exist'
);


SELECT
    is(materialization.materialize(type, '2015-01-21 15:00+00'), 2)
FROM materialization.type
WHERE type::text = 'vtest_Node_qtr -> test_Node_qtr';

SELECT
    is(materialization.materialize(type, '2015-01-22 11:00+00'), 0)
FROM materialization.type
WHERE type::text = 'vtest_Node_qtr -> test_Node_qtr';

SELECT * FROM finish();
ROLLBACK;
