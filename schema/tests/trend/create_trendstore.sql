BEGIN;

SELECT plan(2);

SELECT is(
	trend.create_trendstore(
        'some_datasource_name',
        'some_entitytype_name',
        '900',
        ARRAY[
            ('x', 'integer', 'some column with integer values')
        ]::trend.trend_descr[]
    )::text,
	'some_datasource_name_some_entitytype_name_qtr',
	'the result of create_notificationstore'
);

SELECT columns_are(
    'trend',
    'some_datasource_name_some_entitytype_name_qtr',
    ARRAY[
        'entity_id',
        'timestamp',
        'modified',
        'x'
    ]
);

SELECT * FROM finish();
ROLLBACK;
