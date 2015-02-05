BEGIN;

SELECT plan(1);

SELECT trend_directory.create_trendstore(
    'test',
    'Node',
    '900',
    ARRAY[
        ('x', 'integer', 'some column with integer values')
    ]::trend_directory.trend_descr[],
    '3600'
);


SELECT
    trend_directory.create_partition(trendstore, 379958)
FROM trend_directory.trendstore
WHERE trendstore::text = 'test_Node_qtr';

SELECT has_table(
    'trend',
    'test_Node_qtr_379958',
    'trend partition table should exist'
);

SELECT * FROM finish();
ROLLBACK;
