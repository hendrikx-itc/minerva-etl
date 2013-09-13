===============
Database Schema
===============

The 'partition' table references PostgreSQL tables by name instead of by OID
because PostgreSQL doesn't allow foreign keys to system tables like
'pg_catalog.pg_class'. The OID could still be used, but it doesn't have any
added benefits and the name is easier to read for humans.

Creating a partition record results automatically in the creation of the
corresponding data table::

	INSERT INTO partition (entitytype_id, datasource_id, granularity_period,
	data_start, data_end) VALUES (1, 1, 900, NOW(), NOW());