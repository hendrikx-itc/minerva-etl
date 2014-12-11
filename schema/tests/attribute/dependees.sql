BEGIN;

SELECT plan(1);

SELECT attribute_directory.create_attributestore(
	'test',
	'Node',
	ARRAY[
		('x', 'integer', 'some column with integer values')
	]::attribute_directory.attribute_descr[]
);

SELECT is(
    bar.dependees,
    ARRAY[
        'attribute_history."test_Node_at"(pg_catalog.timestamptz)',
        'attribute_history."test_Node_at"(pg_catalog.int4, pg_catalog.timestamptz)',
        'attribute_history.values_hash(attribute_history."test_Node")',
        'attribute_staging."test_Node_new"',
        'attribute_history."test_Node_changes"',
        'attribute_history."test_Node_run_length"'
    ]::text[]
)
FROM (
    SELECT array_agg(d::text) dependees
    FROM (
        SELECT unnest(attribute_directory.dependees(attributestore)) d
        FROM attribute_directory.attributestore
    ) foo
) bar;

SELECT * FROM finish();
ROLLBACK;
