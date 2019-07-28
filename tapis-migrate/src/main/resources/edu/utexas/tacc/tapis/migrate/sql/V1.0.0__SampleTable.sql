--###############################################################
--# Migration: V1.0.0__SampleTable.sql
--#
--# Sample table create.
--#
--#################################################################

CREATE TABLE IF NOT EXISTS sample_tbl 
(
    id serial4 PRIMARY KEY,
    text character varying(512) NOT NULL,
    updated timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc')
);
ALTER TABLE sample_tbl OWNER TO tapis;