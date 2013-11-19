SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = system, pg_catalog;


CREATE TYPE job_type AS (id int, type character varying, description character varying, size bigint, config text);


CREATE OR REPLACE FUNCTION create_job(type character varying, description character varying, size bigint, job_source_id int)
	RETURNS integer
AS $$
DECLARE
	new_job_id integer;
BEGIN
	INSERT INTO system.job(size, job_source_id, type, description) VALUES (size, job_source_id, type, description) RETURNING id INTO new_job_id;

	INSERT INTO system.job_queue(job_id) VALUES (new_job_id);

	return new_job_id;
END;
$$ LANGUAGE plpgsql VOLATILE STRICT;


CREATE OR REPLACE FUNCTION get_job()
	RETURNS system.job_type
AS $$
DECLARE
	result system.job_type;
BEGIN
	BEGIN
		LOCK TABLE system.job_queue IN SHARE UPDATE EXCLUSIVE MODE NOWAIT;
		LOCK TABLE system.job IN SHARE UPDATE EXCLUSIVE MODE NOWAIT;

		SELECT job.id, job.type, job.description, job.size, js.config INTO result
			FROM system.job_queue
			JOIN system.job ON job_id = id
			JOIN system.job_source js ON js.id = job.job_source_id
			ORDER BY job_id ASC LIMIT 1;

		IF result IS NOT NULL THEN
			UPDATE system.job SET state = 'running', started = NOW() WHERE id = result.id;

			DELETE FROM system.job_queue WHERE job_id = result.id;
		END IF;
	EXCEPTION
		WHEN lock_not_available THEN
			result = NULL;
	END;

	RETURN result;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION finish_job(job_id int)
	RETURNS void
AS $$
DECLARE
BEGIN
	UPDATE system.job SET state = 'finished', finished = NOW() WHERE system.job.id = job_id;
END;
$$ LANGUAGE plpgsql VOLATILE STRICT;


CREATE OR REPLACE FUNCTION fail_job(job_id int)
	RETURNS void
AS $$
DECLARE
BEGIN
	UPDATE system.job SET state = 'failed', finished = NOW() WHERE system.job.id = job_id;
END;
$$ LANGUAGE plpgsql VOLATILE STRICT;


CREATE OR REPLACE FUNCTION fail_job(job_id int, message character varying)
	RETURNS void
AS $$
DECLARE
BEGIN
	UPDATE system.job SET state = 'failed', finished = NOW() WHERE system.job.id = job_id;

	INSERT INTO system.job_error_log (job_id, message) VALUES (job_id, message);
END;
$$ LANGUAGE plpgsql VOLATILE STRICT;


CREATE OR REPLACE FUNCTION add_job_source(character varying, character varying, character varying)
	RETURNS integer
AS $$
	INSERT INTO system.job_source (id, name, job_type, config)
	VALUES (DEFAULT, $1, $2, $3)
	RETURNING id;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION get_job_source(integer)
	RETURNS TABLE(name character varying, job_type character varying, config character varying)
AS $$
	SELECT name, job_type, config FROM system.job_source WHERE id = $1;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION remove_jobs(before timestamp with time zone)
	RETURNS integer
AS $$
DECLARE
	result integer;
BEGIN
	-- Acquire locks
	LOCK TABLE system.job IN ACCESS EXCLUSIVE MODE;
	LOCK TABLE system.job_queue IN ACCESS EXCLUSIVE MODE;
	LOCK TABLE transform.state IN ACCESS EXCLUSIVE MODE;

	-- Drop constraints on dependent tables
	ALTER TABLE system.job_queue DROP CONSTRAINT job_queue_job_id_fkey;
	ALTER TABLE transform.state DROP CONSTRAINT job_id_fkey;

	-- Select rows for deletion
	CREATE TEMP TABLE to_be_deleted ON COMMIT DROP AS SELECT * FROM system.job WHERE created < before;

	-- Actual deleting of jobs
	DELETE FROM system.job USING to_be_deleted WHERE to_be_deleted.id = job.id;

	GET DIAGNOSTICS result = ROW_COUNT;

	-- Update dependent tables
	DELETE FROM system.job_queue USING to_be_deleted WHERE to_be_deleted.id = job_queue.job_id;

	UPDATE transform.state SET job_id = DEFAULT WHERE job_id IN (SELECT id FROM to_be_deleted);

	-- Restore constraints on dependent tables
	ALTER TABLE transform.state
		ADD CONSTRAINT job_id_fkey FOREIGN KEY (job_id) REFERENCES system.job(id)
		MATCH SIMPLE ON UPDATE NO ACTION ON DELETE SET DEFAULT;

	ALTER TABLE system.job_queue
		ADD CONSTRAINT job_queue_job_id_fkey FOREIGN KEY (job_id) REFERENCES system.job(id)
		ON DELETE CASCADE;

	return result;
END;
$$ LANGUAGE plpgsql VOLATILE STRICT;


CREATE OR REPLACE FUNCTION get_setting(name text)
	RETURNS system.setting
AS $$
	SELECT setting FROM system.setting WHERE name = $1;
$$ LANGUAGE SQL STABLE STRICT;


CREATE OR REPLACE FUNCTION add_setting(name text, value text)
	RETURNS system.setting
AS $$
	INSERT INTO system.setting (name, value) VALUES ($1, $2) RETURNING setting;
$$ LANGUAGE SQL VOLATILE STRICT;


CREATE OR REPLACE FUNCTION update_setting(name text, value text)
	RETURNS system.setting
AS $$
	UPDATE system.setting SET value = $2 WHERE name = $1 RETURNING setting;
$$ LANGUAGE SQL VOLATILE STRICT;


CREATE OR REPLACE FUNCTION set_setting(name text, value text)
	RETURNS system.setting
AS $$
	SELECT COALESCE(system.update_setting($1, $2), system.add_setting($1, $2));
$$ LANGUAGE SQL VOLATILE STRICT;


CREATE OR REPLACE FUNCTION get_setting_value(name text)
	RETURNS text
AS $$
	SELECT value FROM system.setting WHERE name = $1;
$$ LANGUAGE SQL STABLE STRICT;


CREATE OR REPLACE FUNCTION get_setting_value(name text, "default" text)
	RETURNS text
AS $$
	SELECT COALESCE(get_setting_value($1), $2);
$$ LANGUAGE SQL STABLE STRICT;
