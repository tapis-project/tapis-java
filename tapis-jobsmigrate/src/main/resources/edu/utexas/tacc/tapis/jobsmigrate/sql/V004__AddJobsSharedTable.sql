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
CREATE TYPE job_resource_enum AS ENUM ('JOB_HISTORY', 'JOB_RESUBMIT_REQUEST', 'JOB_OUTPUT', 'JOB_INPUT');
CREATE TYPE job_permission_enum AS ENUM ('READ');

-- ----------------------------------------------------------------------------------------
--                                          Jobs
-- ----------------------------------------------------------------------------------------
-- Jobs Shared table
CREATE TABLE jobs_shared
(
  id                          serial4 PRIMARY KEY,
  tenant                      character varying(24) NOT NULL,
  user_shared_with            character varying(64) NOT NULL,
  
  job_resource                job_resource_enum NOT NULL,
  job_permission              job_permission_enum NOT NULL,
  job_uuid                    character varying(64) NOT NULL,
  createdby                   character varying(64) NOT NULL,
  created                     timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  last_updated                timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  FOREIGN KEY (job_uuid) REFERENCES jobs (uuid) ON DELETE CASCADE ON UPDATE CASCADE
 );

ALTER TABLE jobs_shared OWNER TO tapis;
ALTER SEQUENCE jobs_id_seq RESTART WITH 1;
CREATE INDEX jobs_uuid_shared_with_idx ON jobs_shared (tenant, job_uuid, createdby, user_shared_with);
CREATE INDEX jobs_uuid_idx ON jobs_shared (job_uuid);
CREATE INDEX jobs_tenant_user_idx ON jobs_shared (tenant,user_shared_with);
CREATE INDEX jobs_shared_idx ON jobs_shared (tenant, created);
CREATE INDEX jobs_createdby_idx ON jobs_shared (tenant, createdby);
CREATE INDEX jobs_uuid_jobresource ON jobs_shared (job_uuid, job_resource);

