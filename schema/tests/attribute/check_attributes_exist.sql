BEGIN;

SELECT plan(2);

SELECT attribute_directory.create_attributestore(
	'test',
	'Node',
	ARRAY[
		('x', 'integer', 'some column with integer values')
	]::attribute_directory.attribute_descr[]
);

CREATE VIEW uses_attribute AS
SELECT * FROM attribute_history."test_Node";

SELECT columns_are(
    'attribute_history',
    'test_Node',
    ARRAY[
        'entity_id',
        'timestamp',
        'modified',
        'hash',
        'first_appearance',
        'x'
    ]
);

SELECT attribute_directory.check_attributes_exist(
    ARRAY[
       (NULL, attributestore.id, 'some column with floating point values', 'y', 'double precision') 
    ]::attribute_directory.attribute[]
)
FROM attribute_directory.attributestore
WHERE attributestore::text = 'test_Node';

SELECT columns_are(
    'attribute_history',
    'test_Node',
    ARRAY[
        'entity_id',
        'timestamp',
        'modified',
        'hash',
        'first_appearance',
        'x',
        'y'
    ]
);

SELECT * FROM finish();
ROLLBACK;
