CREATE OR REPLACE VIEW virtual_entity."v-site" AS
SELECT name
FROM (VALUES ('1001'), ('1002'), ('1003')) AS s(name);

SELECT directory.create_entity_type('v-site');

INSERT INTO entity."v-site" (name)
SELECT name FROM virtual_entity."v-site"
ON CONFLICT DO NOTHING;
