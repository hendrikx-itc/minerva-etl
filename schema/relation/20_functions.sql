SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = relation, pg_catalog;


CREATE OR REPLACE FUNCTION create_relation_table(name text, type_id int)
	RETURNS void
AS $$
DECLARE
	sql text;
	full_table_name text;
BEGIN
	EXECUTE format('CREATE TABLE %I.%I (
	CHECK (type_id=%L)
	) INHERITS (relation."all");', 'relation', name, type_id);

	EXECUTE format('ALTER TABLE %I.%I OWNER TO minerva_admin;', 'relation', name);

	EXECUTE format('ALTER TABLE ONLY %I.%I
	ADD CONSTRAINT %I
	PRIMARY KEY (source_id, target_id);', 'relation', name, name || '_pkey');

	EXECUTE format('GRANT SELECT ON TABLE %I.%I TO minerva;', 'relation', name);
	EXECUTE format('GRANT INSERT,DELETE,UPDATE ON TABLE %I.%I TO minerva_writer;', 'relation', name);

	EXECUTE format('CREATE INDEX %I ON %I.%I USING btree (source_id);', 'ix_' || name || '_source_id', 'relation', name);
	EXECUTE format('CREATE INDEX %I ON %I.%I USING btree (target_id);', 'ix_' || name || '_target_id', 'relation', name);
END;
$$ LANGUAGE plpgsql VOLATILE STRICT;


CREATE OR REPLACE FUNCTION get_type(character varying)
	RETURNS relation.type
AS $$
	SELECT type FROM relation.type WHERE name = $1;
$$ LANGUAGE SQL STABLE STRICT;


CREATE OR REPLACE FUNCTION create_type(character varying)
	RETURNS relation.type
AS $$
	INSERT INTO relation.type (name) VALUES ($1) RETURNING type;
$$ LANGUAGE SQL VOLATILE STRICT;


CREATE OR REPLACE FUNCTION name_to_type(character varying)
	RETURNS relation.type
AS $$
	SELECT COALESCE(relation.get_type($1), relation.create_type($1));
$$ LANGUAGE SQL VOLATILE STRICT;
