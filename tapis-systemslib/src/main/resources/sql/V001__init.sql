-- Initial DB schema creation for Tapis Systems Service
-- postgres commands to create all tables, indices and other database artifacts required.
-- Prerequisites:
-- Create DB named tapissysdb and user named tapis
--   CREATE DATABASE tapissysdb;
--   CREATE USER tapis WITH ENCRYPTED PASSWORD '<password>'
--   GRANT ALL PRIVILEGES ON DATABASE tapissysdb TO tapis;
-- Fast way to check for table. Might use this at startup during an init phase.
--   SELECT to_regclass('tapis_sys.systems');
--
--
-- TIMEZONE Convention
----------------------
-- All tables in this application conform to the same timezone usage rule:
--
--   All dates, times and timestamps are stored as UTC WITHOUT TIMEZONE information.
--
-- All temporal values written to the database are required to be UTC, all temporal
-- values read from the database can be assumed to be UTC.

-- Create the schema and set the search path
CREATE SCHEMA IF NOT EXISTS tapis_sys AUTHORIZATION tapis;
ALTER ROLE tapis SET search_path = 'tapis_sys';
SET search_path TO tapis_sys;
-- SET search_path TO public;

-- Set permissions
-- GRANT CONNECT ON DATABASE tapissysdb TO tapis_sys;
-- GRANT USAGE ON SCHEMA tapis_sys TO tapis_sys;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA tapis_sys TO tapis_sys;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA tapis_sys TO tapis_sys;

-- Types
CREATE TYPE system_type_type AS ENUM ('LINUX', 'OBJECT_STORE');
CREATE TYPE access_meth_type AS ENUM ('PASSWORD', 'PKI_KEYS', 'ACCESS_KEY', 'CERT');
CREATE TYPE transfer_meth_type AS ENUM ('SFTP', 'S3');
CREATE TYPE capability_category_type AS ENUM ('SCHEDULER', 'OS', 'HARDWARE', 'SOFTWARE', 'JOB', 'CONTAINER', 'MISC', 'CUSTOM');

-- ----------------------------------------------------------------------------------------
--                                     SYSTEMS
-- ----------------------------------------------------------------------------------------
-- Systems table
CREATE TABLE systems
(
  id          SERIAL PRIMARY KEY,
  tenant      VARCHAR(24) NOT NULL,
  name        VARCHAR(256) NOT NULL,
  description VARCHAR(2048),
  system_type system_type_type NOT NULL,
  owner       VARCHAR(60) NOT NULL,
  host        VARCHAR(256) NOT NULL,
  enabled     BOOLEAN NOT NULL DEFAULT true,
  effective_user_id VARCHAR(60) NOT NULL,
  default_access_method  access_meth_type NOT NULL,
  bucket_name    VARCHAR(63),
  root_dir       VARCHAR(1024),
  transfer_methods transfer_meth_type[] NOT NULL,
  port       INTEGER NOT NULL DEFAULT -1,
  use_proxy  BOOLEAN NOT NULL DEFAULT false,
  proxy_host VARCHAR(256) NOT NULL DEFAULT '',
  proxy_port INTEGER NOT NULL DEFAULT -1,
  job_can_exec   BOOLEAN NOT NULL DEFAULT false,
  job_local_working_dir VARCHAR(1024),
  job_local_archive_dir VARCHAR(1024),
  job_remote_archive_system VARCHAR(256),
  job_remote_archive_dir VARCHAR(1024),
  tags       JSONB NOT NULL,
  notes      JSONB NOT NULL,
  raw_req    VARCHAR NOT NULL,
  created     TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
  updated     TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
  UNIQUE (tenant,name)
);
ALTER TABLE systems OWNER TO tapis;
COMMENT ON COLUMN systems.id IS 'System id';
COMMENT ON COLUMN systems.tenant IS 'Tenant name associated with system';
COMMENT ON COLUMN systems.name IS 'Unique name for the system';
COMMENT ON COLUMN systems.description IS 'System description';
COMMENT ON COLUMN systems.system_type IS 'Type of system';
COMMENT ON COLUMN systems.owner IS 'User name of system owner';
COMMENT ON COLUMN systems.host IS 'System host name or ip address';
COMMENT ON COLUMN systems.enabled IS 'Indicates if system is currently active and available for use';
COMMENT ON COLUMN systems.effective_user_id IS 'User name to use when accessing the system';
COMMENT ON COLUMN systems.default_access_method IS 'Enum for how authorization is handled by default';
COMMENT ON COLUMN systems.bucket_name IS 'Name of the bucket for an S3 system';
COMMENT ON COLUMN systems.root_dir IS 'Name of root directory for a Unix system';
COMMENT ON COLUMN systems.transfer_methods IS 'List of supported transfer methods';
COMMENT ON COLUMN systems.port IS 'Port number used to access a system';
COMMENT ON COLUMN systems.use_proxy IS 'Indicates if system should accessed through a proxy';
COMMENT ON COLUMN systems.proxy_host IS 'Proxy host name or ip address';
COMMENT ON COLUMN systems.proxy_port IS 'Proxy port number';
COMMENT ON COLUMN systems.job_can_exec IS 'Indicates if system will be used to execute jobs';
COMMENT ON COLUMN systems.job_local_working_dir IS 'Parent directory from which a job is run and where inputs and application assets are staged';
COMMENT ON COLUMN systems.job_local_archive_dir IS 'Parent directory used for archiving job output files';
COMMENT ON COLUMN systems.job_remote_archive_system IS 'Remote system on which job output files will be archived';
COMMENT ON COLUMN systems.job_remote_archive_dir IS 'Parent directory used for archiving job output files on a remote system';
COMMENT ON COLUMN systems.tags IS 'Tags for user supplied key:value pairs';
COMMENT ON COLUMN systems.notes IS 'Notes for general information stored as JSON';
COMMENT ON COLUMN systems.raw_req IS 'Raw text data used to create the item';
COMMENT ON COLUMN systems.created IS 'UTC time for when record was created';
COMMENT ON COLUMN systems.updated IS 'UTC time for when record was last updated';

-- ----------------------------------------------------------------------------------------
--                               CAPABILITIES
-- ----------------------------------------------------------------------------------------
-- Capabilities table
-- All columns are specified NOT NULL to make queries easier. <col> = null is not the same as <col> is null
CREATE TABLE capabilities
(
    id     SERIAL PRIMARY KEY,
    tenant VARCHAR(24) NOT NULL,
    system_id SERIAL REFERENCES systems(id) ON DELETE CASCADE,
    category   capability_category_type NOT NULL,
    name   VARCHAR(256) NOT NULL DEFAULT '',
    value  VARCHAR(256) NOT NULL DEFAULT '',
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    updated TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    UNIQUE (tenant, system_id, category, name)
);
ALTER TABLE capabilities OWNER TO tapis;
COMMENT ON COLUMN capabilities.id IS 'Capability id';
COMMENT ON COLUMN capabilities.tenant IS 'Name of tenant';
COMMENT ON COLUMN capabilities.system_id IS 'Id of system supporting the capability';
COMMENT ON COLUMN capabilities.category IS 'Category for grouping of capabilities';
COMMENT ON COLUMN capabilities.name IS 'Name of capability';
COMMENT ON COLUMN capabilities.value IS 'Value for the capability';
COMMENT ON COLUMN capabilities.created IS 'UTC time for when record was created';
COMMENT ON COLUMN capabilities.updated IS 'UTC time for when record was last updated';

-- ******************************************************************************
--                         PROCEDURES and TRIGGERS
-- ******************************************************************************

-- Auto update of updated column
CREATE OR REPLACE FUNCTION trigger_set_updated() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER system_updated
  BEFORE UPDATE ON systems
  EXECUTE PROCEDURE trigger_set_updated();
CREATE TRIGGER capability_updated
    BEFORE UPDATE ON capabilities
EXECUTE PROCEDURE trigger_set_updated();
