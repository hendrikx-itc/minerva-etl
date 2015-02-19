CREATE CAST (trend_directory.trend_store AS text) WITH FUNCTION trend_directory.to_char(trend_directory.trend_store) AS IMPLICIT;

CREATE CAST (trend_directory.view AS text) WITH FUNCTION trend_directory.to_char(trend_directory.view) AS IMPLICIT;
