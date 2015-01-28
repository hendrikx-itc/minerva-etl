BEGIN;

SELECT plan(3);


SELECT trend.create_trendstore(
    'test-data',
    'Node',
    '900',
    ARRAY[
        ('x', 'integer', 'some column with integer values')
    ]::trend.trend_descr[]
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


SELECT trend.transfer_staged(trendstore) FROM trend.trendstore WHERE trendstore::text = 'test-data_Node_qtr';


SELECT materialization.define(
    trend.create_view(
        trend.define_view(
        trend.attributes_to_view_trendstore('vtest', 'Node', '900'),
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
