-- Drop DB schema for Tapis Systems Service
-- postgres commands to remove all artifacts.

-- DROP SCHEMA IF EXISTS tapis_sys CASCADE;
DROP TABLE IF EXISTS cmd_protocol CASCADE;
DROP TABLE IF EXISTS txf_protocol CASCADE;
DROP TABLE IF EXISTS systems CASCADE;
DROP TYPE IF EXISTS command_mech_type CASCADE;
DROP TYPE IF EXISTS transfer_mech_type CASCADE;
