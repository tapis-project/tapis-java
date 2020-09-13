-- Initial DB schema creation for Tapis Systems Service
-- postgres commands to create all tables, indices and other database artifacts required.
-- Prerequisites:
-- Create DB named tapissysdb and user named tapis
--   CREATE DATABASE tapissysdb ENCODING='UTF8' LC_COLLATE='en_US.utf8' LC_CTYPE='en_US.utf8';
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

-- NOTES for jOOQ
--   When a POJO has a default constructor (which is needed for jersey's SelectableEntityFilteringFeature)
--     then column names must match POJO attributes (with convention an_attr -> anAttr)
--     in order for jOOQ to set the attribute during Record.into()
--     Possibly another option would be to create a custom mapper to be used by Record.into()
--
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
CREATE TYPE operation_type AS ENUM ('create', 'modify', 'softDelete', 'hardDelete', 'changeOwner',
                                    'grantPerms', 'revokePerms', 'setCred', 'removeCred');
CREATE TYPE access_meth_type AS ENUM ('PASSWORD', 'PKI_KEYS', 'ACCESS_KEY', 'CERT');
CREATE TYPE capability_category_type AS ENUM ('SCHEDULER', 'OS', 'HARDWARE', 'SOFTWARE', 'JOB', 'CONTAINER', 'MISC', 'CUSTOM');

-- ----------------------------------------------------------------------------------------
--                                     SYSTEMS
-- ----------------------------------------------------------------------------------------
-- Systems table
-- Basic system attributes
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
  transfer_methods TEXT[],
  port       INTEGER NOT NULL DEFAULT -1,
  use_proxy  BOOLEAN NOT NULL DEFAULT false,
  proxy_host VARCHAR(256) NOT NULL DEFAULT '',
  proxy_port INTEGER NOT NULL DEFAULT -1,
  job_can_exec   BOOLEAN NOT NULL DEFAULT false,
  job_local_working_dir VARCHAR(1024),
  job_local_archive_dir VARCHAR(1024),
  job_remote_archive_system VARCHAR(256),
  job_remote_archive_dir VARCHAR(1024),
  tags       TEXT[] NOT NULL,
  notes      JSONB NOT NULL,
  import_ref_id VARCHAR(256),
  deleted    BOOLEAN NOT NULL DEFAULT false,
  created    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
  updated    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
  UNIQUE (tenant,name)
);
ALTER TABLE systems OWNER TO tapis;
CREATE INDEX sys_tenant_name_idx ON systems (tenant, name);
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
COMMENT ON COLUMN systems.import_ref_id IS 'Optional reference ID for systems created via import';
COMMENT ON COLUMN systems.deleted IS 'Indicates if system has been soft deleted';
COMMENT ON COLUMN systems.created IS 'UTC time for when record was created';
COMMENT ON COLUMN systems.updated IS 'UTC time for when record was last updated';

-- System updates table
-- Track update requests for systems
CREATE TABLE system_updates
(
    id SERIAL PRIMARY KEY,
    system_id SERIAL REFERENCES systems(id) ON DELETE CASCADE,
    user_name VARCHAR(60) NOT NULL,
    user_tenant VARCHAR(24) NOT NULL,
    operation operation_type NOT NULL,
    upd_json JSONB NOT NULL,
    upd_text VARCHAR,
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
);
ALTER TABLE system_updates OWNER TO tapis;
COMMENT ON COLUMN system_updates.id IS 'System update request id';
COMMENT ON COLUMN system_updates.system_id IS 'Id of system being updated';
COMMENT ON COLUMN system_updates.user_name IS 'Name of user who requested the update';
COMMENT ON COLUMN system_updates.user_tenant IS 'Tenant of user who requested the update';
COMMENT ON COLUMN system_updates.operation IS 'Type of update operation';
COMMENT ON COLUMN system_updates.upd_json IS 'JSON representing the update - with secrets scrubbed';
COMMENT ON COLUMN system_updates.upd_text IS 'Text data supplied by client - secrets should be scrubbed';
COMMENT ON COLUMN system_updates.created IS 'UTC time for when record was created';

-- ----------------------------------------------------------------------------------------
--                               CAPABILITIES
-- ----------------------------------------------------------------------------------------
-- Capabilities table
-- Capabilities associated with a system
-- All columns are specified NOT NULL to make queries easier. <col> = null is not the same as <col> is null
CREATE TABLE capabilities
(
    id     SERIAL PRIMARY KEY,
    system_id SERIAL REFERENCES systems(id) ON DELETE CASCADE,
    category capability_category_type NOT NULL,
    name   VARCHAR(256) NOT NULL DEFAULT '',
    value  VARCHAR(256) NOT NULL DEFAULT '',
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    updated TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    UNIQUE (system_id, category, name)
);
ALTER TABLE capabilities OWNER TO tapis;
COMMENT ON COLUMN capabilities.id IS 'Capability id';
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
