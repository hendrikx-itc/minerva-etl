BEGIN;

SELECT plan(3);

SELECT trend_directory.create_trendstore(
    'test1',
    'some_entitytype_name',
    '900',
    ARRAY[
        ('x', 'integer', 'some column with integer values')
    ]::trend_directory.trend_descr[]
);

SELECT has_table(
    'trend',
    'test1_some_entitytype_name_qtr',
    'trendstore table with one trend column should exist'
);

SELECT columns_are(
    'trend',
    'test1_some_entitytype_name_qtr',
    ARRAY[
        'entity_id',
        'timestamp',
        'modified',
        'x'
    ]
);

SELECT trend_directory.create_trendstore(
    'test2',
    'some_entitytype_name',
    '900',
    ARRAY[]::trend_directory.trend_descr[]
);

SELECT has_table(
    'trend',
    'test2_some_entitytype_name_qtr',
    'trendstore table without trend columns should exist'
);


SELECT * FROM finish();
ROLLBACK;
