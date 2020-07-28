-- Reset DB for Tapis Systems Service by dropping and re-creating the schema
DROP SCHEMA IF EXISTS tapis_sys CASCADE;
CREATE SCHEMA IF NOT EXISTS tapis_sys AUTHORIZATION tapis;
ALTER ROLE tapis SET search_path = 'tapis_sys';
SET search_path TO tapis_sys;
