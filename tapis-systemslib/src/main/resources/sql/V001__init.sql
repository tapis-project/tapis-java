-- Initial DB schema creation for Tapis Systems Service
-- postgres commands to create all tables, indices and other database artifacts required.
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
CREATE SCHEMA IF NOT EXISTS AUTHORIZATION tapis_sys;
SET search_path TO tapis_sys;

-- Types
CREATE TYPE access_mech_type AS ENUM ('SSH_ANONYMOUS', 'SSH_PASSWORD', 'SSH_KEYS', 'SSH_CERT');
CREATE TYPE transfer_mech_type AS ENUM ('SFTP', 'S3', 'LOCAL');

-- ----------------------------------------------------------------------------------------
--                               PROTOCOLS 
-- ----------------------------------------------------------------------------------------
-- Access Protocol table
CREATE TABLE acc_protocol
(
  id         SERIAL PRIMARY KEY,
  mechanism  access_mech_type NOT NULL,
  port       INTEGER,
  use_proxy  BOOLEAN NOT NULL DEFAULT false,
  proxy_host VARCHAR(256),
  proxy_port INTEGER,
  created   TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
  UNIQUE (mechanism, port, use_proxy, proxy_host, proxy_port)
);

COMMENT ON COLUMN acc_protocol.id IS 'Access protocol id';
COMMENT ON COLUMN acc_protocol.mechanism IS 'Enum for how authorization is handled';
COMMENT ON COLUMN acc_protocol.port IS 'Port number used to access a system';
COMMENT ON COLUMN acc_protocol.use_proxy IS 'Indicates if system should accessed through a proxy';
COMMENT ON COLUMN acc_protocol.proxy_host IS 'Proxy host name or ip address';
COMMENT ON COLUMN acc_protocol.proxy_port IS 'Proxy port number';
COMMENT ON COLUMN acc_protocol.created IS 'UTC time for when record was created';

-- Transfer Protocol table
CREATE TABLE txf_protocol
(
  id         SERIAL PRIMARY KEY,
  mechanism  transfer_mech_type NOT NULL,
  port       INTEGER,
  use_proxy  BOOLEAN NOT NULL DEFAULT false,
  proxy_host VARCHAR(256),
  proxy_port INTEGER,
  created   TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
  UNIQUE (mechanism, port, use_proxy, proxy_host, proxy_port)
);
COMMENT ON COLUMN txf_protocol.id IS 'Transfer protocol id';
COMMENT ON COLUMN txf_protocol.mechanism IS 'Enum for how I/O is handled';
COMMENT ON COLUMN txf_protocol.port IS 'Port number used to access a system';
COMMENT ON COLUMN txf_protocol.use_proxy IS 'Indicates if system should accessed through a proxy';
COMMENT ON COLUMN txf_protocol.proxy_host IS 'Proxy host name or ip address';
COMMENT ON COLUMN txf_protocol.proxy_port IS 'Proxy port number';
COMMENT ON COLUMN txf_protocol.created IS 'UTC time for when record was created';

-- ----------------------------------------------------------------------------------------
--                                     SYSTEMS
-- ----------------------------------------------------------------------------------------
-- Systems table
CREATE TABLE systems
(
  id             SERIAL PRIMARY KEY,
  tenant         VARCHAR(24) NOT NULL,
  name           VARCHAR(256) NOT NULL,
  description    VARCHAR(2048) NOT NULL,
  owner          VARCHAR(60) NOT NULL,
  host           VARCHAR(256) NOT NULL,
  available      BOOLEAN NOT NULL DEFAULT true,
  bucket_name    VARCHAR(63),
  root_dir       VARCHAR(1024),
--  job_input_dir  VARCHAR(1024),
--  job_output_dir VARCHAR(1024),
--  work_dir       VARCHAR(1024),
--  scratch_dir    VARCHAR(1024),
  effective_user_id VARCHAR(60) NOT NULL,
--  access_protocol   SERIAL references acc_protocol(id),
--  transfer_protocol SERIAL references txf_protocol(id),
--  access_protocol VARCHAR(32) NOT NULL,
--  transfer_protocol VARCHAR(32) NOT NULL,
  created     TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
  updated     TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
  UNIQUE (tenant,name)
);
COMMENT ON COLUMN systems.id IS 'System id';
COMMENT ON COLUMN systems.tenant IS 'Tenant name associated with system';
COMMENT ON COLUMN systems.name IS 'Unique name for the system';
COMMENT ON COLUMN systems.description IS 'System description';
COMMENT ON COLUMN systems.owner IS 'User name of system owner';
COMMENT ON COLUMN systems.host IS 'System host name or ip address';
COMMENT ON COLUMN systems.available IS 'Indicates if system is currently available';
COMMENT ON COLUMN systems.bucket_name IS 'Name of the bucket for an S3 system';
COMMENT ON COLUMN systems.root_dir IS 'Name of root directory for a Unix system';
--COMMENT ON COLUMN systems.job_input_dir IS '';
--COMMENT ON COLUMN systems.job_output_dir IS '';
--COMMENT ON COLUMN systems.work_dir IS '';
--COMMENT ON COLUMN systems.scratch_dir IS '';
COMMENT ON COLUMN systems.effective_user_id IS 'User name to use when accessing the system';
--COMMENT ON COLUMN systems.access_protocol IS 'Reference to access protocol used for the system';
--COMMENT ON COLUMN systems.transfer_protocol IS 'Reference to transfer protocol used for the system';
COMMENT ON COLUMN systems.created IS 'UTC time for when record was created';
COMMENT ON COLUMN systems.updated IS 'UTC time for when record was last updated';

-- ******************************************************************************
--                         PROCEDURES and TRIGGERS
-- ******************************************************************************
-- ----------------------------------------------------------------------------------------
--         systems
-- ----------------------------------------------------------------------------------------

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
