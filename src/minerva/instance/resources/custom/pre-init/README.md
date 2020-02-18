# Custom pre-init

Place any SQL scripts here that need to be run BEFORE all regular Minerva
constructs such as trend stores and attribute stores are initialized.

An example of this could be a custom aggregate function that is used in a
materialization.