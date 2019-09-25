-- Initial DB schema creation for Tapis Systems Service
-- posgres commands to create all tables, indices and other database artifacts required.
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

-- Create the schema set the search path
CREATE SCHEMA IF NOT EXISTS AUTHORIZATION tapis_sys;
SET search_path TO tapis_sys;

-- Types
CREATE TYPE access_mech_type AS ENUM ('SSH-ANONYMOUS', 'SSH-PASSWORD', 'SSH-KEYS', 'SSH-CERT');
CREATE TYPE transfer_mech_type AS ENUM ('SFTP', 'S3', 'LOCAL');

-- ----------------------------------------------------------------------------------------
--                               PROTOCOLS 
-- ----------------------------------------------------------------------------------------
-- Access Protocol table
CREATE TABLE acc_protocol
(
  id         serial4 PRIMARY KEY,
  mechanism  access_mech_type NOT NULL,
  port       integer,
  use_proxy  boolean NOT NULL DEFAULT false,
  proxy_host character varying(256) NOT NULL,
  proxy_port integer,
  created   timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  updated   timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc')
);
COMMENT ON COLUMN acc_protocol.id IS 'Access protocol id';
COMMENT ON COLUMN acc_protocol.mechanism IS 'Enum for how authorization is handled';
COMMENT ON COLUMN acc_protocol.port IS 'Port number used to access a system';
COMMENT ON COLUMN acc_protocol.use_proxy IS 'Indicates if system should accessed through a proxy';
COMMENT ON COLUMN acc_protocol.proxy_host IS 'Proxy host name or ip address';
COMMENT ON COLUMN acc_protocol.proxy_port IS 'Proxy port number';
COMMENT ON COLUMN acc_protocol.created IS 'UTC time for when record was created';
COMMENT ON COLUMN acc_protocol.updated IS 'UTC time for when record was updated';

-- Transfer Protocol table
CREATE TABLE txf_protocol
(
  id         serial4 PRIMARY KEY,
  mechanism  transfer_mech_type NOT NULL,
  port       integer,
  use_proxy  boolean NOT NULL DEFAULT false,
  proxy_host character varying(256) NOT NULL,
  proxy_port integer,
  created   timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  updated   timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc')
);
COMMENT ON COLUMN txf_protocol.id IS 'Transfer protocol id';
COMMENT ON COLUMN txf_protocol.mechanism IS 'Enum for how I/O is handled';
COMMENT ON COLUMN txf_protocol.port IS 'Port number used to access a system';
COMMENT ON COLUMN txf_protocol.use_proxy IS 'Indicates if system should accessed through a proxy';
COMMENT ON COLUMN txf_protocol.proxy_host IS 'Proxy host name or ip address';
COMMENT ON COLUMN txf_protocol.proxy_port IS 'Proxy port number';
COMMENT ON COLUMN txf_protocol.created IS 'UTC time for when record was created';
COMMENT ON COLUMN txf_protocol.updated IS 'UTC time for when record was updated';

-- ----------------------------------------------------------------------------------------
--                                     SYSTEMS
-- ----------------------------------------------------------------------------------------
-- Systems table
CREATE TABLE systems
(
  id             serial4 PRIMARY KEY,
  tenant         character varying(24) NOT NULL,
  name           character varying(256) NOT NULL,
  description    character varying(2048) NOT NULL,
  owner          character varying(60) NOT NULL,
  host           character varying(256) NOT NULL,
  available      boolean NOT NULL DEFAULT true,
  bucket_name    character varying(63),
  root_dir       character varying(1024),
  job_input_dir  character varying(1024),
  job_output_dir character varying(1024),
  work_dir       character varying(1024),
  scratch_dir    character varying(1024),
  effective_user_id character varying(60) NOT NULL,
  access_protocol   serial4 references acc_protocol(id),
  transfer_protocol serial4 references txf_protocol(id),
  created     timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  updated     timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
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
COMMENT ON COLUMN systems.job_input_dir IS '';
COMMENT ON COLUMN systems.job_output_dir IS '';
COMMENT ON COLUMN systems.work_dir IS '';
COMMENT ON COLUMN systems.scratch_dir IS '';
COMMENT ON COLUMN systems.effective_user_id IS 'User name to use when accessing the system';
-- COMMENT ON COLUMN systems.access_protocl IS 'Reference to access protocol used for the system';
-- COMMENT ON COLUMN systems.transfer_protocl IS 'Reference to transfer protocol used for the system';
COMMENT ON COLUMN systems.created IS 'UTC time for when record was created';
COMMENT ON COLUMN systems.updated IS 'UTC time for when record was last updated';

-- ALTER TABLE systems OWNER TO tapis_sys;
-- ALTER TABLE acc_protocol OWNER TO tapis_sys;
-- ALTER TABLE txf_protocol OWNER TO tapis_sys;
