-- This file contains the data definitions required by the Tapis Security Kernel.
-- The content is a valid postgres command file that defines all the
-- tables, indices and other database artifacts needed by the Security Kernel.
--
-- Domain tables are optimized for reading; audit tables for writing.
--
-- TIMEZONE Convention
----------------------
-- All tables in this application conform to the same timezone usage rule:
--
--      All dates, times and timestamps are stored as UTC WITHOUT TIMEZONE information.
--
-- All temporal values written to the database are required to be UTC, all temporal
-- values read from the database can be assumed to be UTC.

-- Types
CREATE TYPE job_status AS ENUM ('PENDING', 'PROCESSING_INPUTS', 'STAGING_INPUTS', 'STAGING_JOB', 'SUBMITTING_JOB', 'QUEUED', 'RUNNING', 'ARCHIVING', 'FINISHED','CANCELLED', 'FAILED', 'PAUSED', 'BLOCKED');
CREATE TYPE remote_outcome_enum AS ENUM ('FINISHED', 'FAILED', 'FAILED_SKIP_ARCHIVE');

-- ----------------------------------------------------------------------------------------
--                                          ROLE
-- ----------------------------------------------------------------------------------------
-- Role table
CREATE TABLE jb_jobs
(
  id                          serial4 PRIMARY KEY,
  name                        character varying(64) NOT NULL, 
  owner                       character varying(64) NOT NULL,
  owner_tenant                character varying(24) NOT NULL,
  description                 character varying(2048) NOT NULL, 
  status                      job_status NOT NULL,
  last_message                character varying(4096) NOT NULL, 
  
  created                     timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  ended                       timestamp without time zone,       
  last_updated                timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  
  uuid                        character varying(64) NOT NULL,
  
  app_id                      character varying(80) NOT NULL,
  app_version                 character varying(64) NOT NULL,
  archive_on_app_error        boolean NOT NULL DEFAULT FALSE,
  
  input_system_id             character varying(80),
  exec_system_id              character varying(80) NOT NULL,
  exec_system_exec_path       character varying(4096), 
  exec_system_input_path      character varying(4096), 
  archive_system_id           character varying(80),
  archive_system_path         character varying(4096),
  
  nodes                       integer NOT NULL,
  processors_per_node         integer NOT NULL,
  memory_mb                   integer NOT NULL,
  max_minutes                 integer NOT NULL,
  
  inputs                      character varying(65536),
  parameters                  character varying(65536),
  
  remote_job_id               character varying(126),
  remote_job_id2              character varying(126),
  remote_outcome              remote_outcome_enum,
  remote_result_info          character varying(4096),
  remote_queue                character varying(126),
  remote_submitted            timestamp without time zone,
  remote_started              timestamp without time zone,
  remote_ended                timestamp without time zone,
  remote_submit_retries       integer NOT NULL DEFAULT 0,
  remote_status_checks        integer NOT NULL DEFAULT 0,
  remote_failed_checks        integer NOT NULL DEFAULT 0,
  remote_status_checks_total  integer NOT NULL DEFAULT 0,
  remote_failed_checks_total  integer NOT NULL DEFAULT 0,
  remote_last_status_check    timestamp without time zone,
  blocked_count               integer NOT NULL DEFAULT 0,
 
  tapis_queue                 character varying(64) NOT NULL,
  visible                     boolean NOT NULL DEFAULT TRUE,
  createdby                   character varying(60) NOT NULL,  
  createdby_tenant            character varying(24) NOT NULL
);
ALTER TABLE jb_jobs OWNER TO tapis;
ALTER SEQUENCE jb_jobs_id_seq RESTART WITH 1;
CREATE UNIQUE INDEX jb_jobs_uuid_idx ON jb_jobs (uuid);
CREATE INDEX jb_jobs_tenant_owner_idx ON jb_jobs (owner_tenant, owner);
CREATE INDEX jb_jobs_created_idx ON jb_jobs (created);
CREATE INDEX jb_jobs_createdby_idx ON jb_jobs (createdby);
CREATE INDEX jb_jobs_status_idx ON jb_jobs (status);
CREATE INDEX jb_jobs_app_id_idx ON jb_jobs (app_id);
CREATE INDEX jb_jobs_input_system_idx ON jb_jobs (input_system_id);
CREATE INDEX jb_jobs_exec_system_idx ON jb_jobs (exec_system_id);
CREATE INDEX jb_jobs_archive_system_idx ON jb_jobs (archive_system_id);
   