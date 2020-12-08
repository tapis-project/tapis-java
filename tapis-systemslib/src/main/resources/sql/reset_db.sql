-- Reset DB for Tapis Systems Service by dropping and re-creating the schema
-- This prepares the DB for flyway to create the initial tables when the service is first started.
DROP SCHEMA IF EXISTS tapis_sys CASCADE;
CREATE SCHEMA IF NOT EXISTS tapis_sys AUTHORIZATION tapis_sys;
ALTER ROLE tapis_sys SET search_path = 'tapis_sys';
SET search_path TO tapis_sys;
