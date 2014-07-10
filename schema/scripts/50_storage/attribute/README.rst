Attribute storage schema definition
===================================

The attribute storage is built up of a set of 5 database schemas:

:attribute_directory: The catalog/directory of existing attributestores and attributes.
:attribute_base: The parent/base tables for attributestores.
:attribute_history: The tables containing the actual attribute data with full history.
:attribute_staging: The attribute staging area for fast updating of the attribute history.
:attribute: Views for the current state of all attributes.
