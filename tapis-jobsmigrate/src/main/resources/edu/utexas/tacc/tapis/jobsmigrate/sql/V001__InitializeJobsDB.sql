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
CREATE TYPE job_status_enum AS ENUM ('PENDING', 'PROCESSING_INPUTS', 'STAGING_INPUTS', 'STAGING_JOB', 'SUBMITTING_JOB', 'QUEUED', 'RUNNING', 'ARCHIVING', 'FINISHED','CANCELLED', 'FAILED', 'PAUSED', 'BLOCKED');
CREATE TYPE job_remote_outcome_enum AS ENUM ('FINISHED', 'FAILED', 'FAILED_SKIP_ARCHIVE');

-- ----------------------------------------------------------------------------------------
--                                          Jobs
-- ----------------------------------------------------------------------------------------
-- Jobs table
CREATE TABLE jobs
(
  id                          serial4 PRIMARY KEY,
  name                        character varying(64) NOT NULL, 
  owner                       character varying(64) NOT NULL,
  tenant                      character varying(24) NOT NULL,
  description                 character varying(2048) NOT NULL, 
  status                      job_status_enum NOT NULL,
  last_message                character varying(16384) NOT NULL, 
  
  created                     timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  ended                       timestamp without time zone,       
  last_updated                timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  
  uuid                        character varying(64) NOT NULL,
  
  app_id                      character varying(80) NOT NULL,
  app_version                 character varying(64) NOT NULL,
  archive_on_app_error        boolean NOT NULL DEFAULT TRUE,
  dynamic_exec_system         boolean NOT NULL DEFAULT FALSE,
  
  exec_system_id              character varying(80) NOT NULL,
  exec_system_exec_dir        character varying(4096), 
  exec_system_input_dir       character varying(4096), 
  exec_system_output_dir      character varying(4096), 
  exec_system_logical_queue   character varying(80), 
  
  archive_system_id           character varying(80) NOT NULL,
  archive_system_dir          character varying(4096),
  
  dtn_system_id               character varying(80),
  dtn_mount_source_path       character varying(1024),
  dtn_mount_point             character varying(1024),
  
  node_count                  integer NOT NULL,
  cores_per_node              integer NOT NULL,
  memory_mb                   integer NOT NULL,
  max_minutes                 integer NOT NULL,
  
  file_inputs                 jsonb NOT NULL,
  parameter_set               jsonb NOT NULL,
  exec_system_constraints     character varying(16384),
  subscriptions               jsonb NOT NULL,
  
  blocked_count               integer NOT NULL DEFAULT 0,
  
  remote_job_id               character varying(126),
  remote_job_id2              character varying(126),
  remote_outcome              job_remote_outcome_enum,
  remote_result_info          character varying(16384),
  remote_queue                character varying(126),
  remote_submitted            timestamp without time zone,
  remote_started              timestamp without time zone,
  remote_ended                timestamp without time zone,
  remote_submit_retries       integer NOT NULL DEFAULT 0,
  remote_checks_success       integer NOT NULL DEFAULT 0,
  remote_checks_failed        integer NOT NULL DEFAULT 0,
  remote_last_status_check    timestamp without time zone,
  
  input_transaction_id        character varying(64),
  input_correlation_id        character varying(64),
  archive_transaction_id      character varying(64),
  archive_correlation_id      character varying(64),
 
  tapis_queue                 character varying(255) NOT NULL,
  visible                     boolean NOT NULL DEFAULT TRUE,
  createdby                   character varying(60) NOT NULL,  
  createdby_tenant            character varying(24) NOT NULL,
  tags                        text[]
);
ALTER TABLE jobs OWNER TO tapis;
ALTER SEQUENCE jobs_id_seq RESTART WITH 1;
CREATE UNIQUE INDEX jobs_uuid_idx ON jobs (uuid);
CREATE INDEX jobs_tenant_owner_idx ON jobs (tenant, owner);
CREATE INDEX jobs_created_idx ON jobs (created);
CREATE INDEX jobs_tenant_createdby_idx ON jobs (createdby_tenant, createdby);
CREATE INDEX jobs_status_idx ON jobs (status);
CREATE INDEX jobs_app_id_idx ON jobs (app_id, tenant);
CREATE INDEX jobs_exec_system_idx ON jobs (exec_system_id);
CREATE INDEX jobs_archive_system_idx ON jobs (archive_system_id);
CREATE INDEX jobs_dtn_system_idx ON jobs (dtn_system_id);
CREATE INDEX jobs_tags_idx ON jobs USING gin (tags);

-- ----------------------------------------------------------------------------------------
--                                       Job Resubmit
-- ----------------------------------------------------------------------------------------
-- Job Resubmit table
CREATE TABLE job_resubmit
(
  id                          serial4 PRIMARY KEY,
  job_uuid                    character varying(64) NOT NULL,
  job_definition              text NOT NULL,
  FOREIGN KEY (job_uuid) REFERENCES jobs (uuid) ON DELETE CASCADE ON UPDATE CASCADE
);
ALTER TABLE job_resubmit OWNER TO tapis;
ALTER SEQUENCE job_resubmit_id_seq RESTART WITH 1;
CREATE UNIQUE INDEX job_resubmit_job_uuid_idx ON job_resubmit (job_uuid);

-- ----------------------------------------------------------------------------------------
--                                       Job Events
-- ----------------------------------------------------------------------------------------
-- Job Events table
CREATE TABLE job_events
(
  id                          serial4 PRIMARY KEY,
  created                     timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  job_uuid                    character varying(64) NOT NULL,
  oth_uuid                    character varying(64),
  event                       character varying(32) NOT NULL,
  description                 character varying(2048) NOT NULL,
  FOREIGN KEY (job_uuid) REFERENCES jobs (uuid) ON DELETE CASCADE ON UPDATE CASCADE
);
ALTER TABLE job_events OWNER TO tapis;
ALTER SEQUENCE job_events_id_seq RESTART WITH 1;
CREATE INDEX job_events_created_idx ON job_events (created);
CREATE INDEX job_events_job_uuid_idx ON job_events (job_uuid);
CREATE INDEX job_events_oth_uuid_idx ON job_events (oth_uuid);
CREATE INDEX job_events_event_idx ON job_events (event);

-- ----------------------------------------------------------------------------------------
--                                       Job Queues
-- ----------------------------------------------------------------------------------------
-- Job Queues table
CREATE TABLE job_queues
(
  id                          serial4 PRIMARY KEY,
  name                        character varying(255) NOT NULL,
  priority                    integer NOT NULL,
  filter                      character varying(4096) NOT NULL,
  uuid                        character varying(64) NOT NULL,
  created                     timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  last_updated                timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc')
);
ALTER TABLE job_queues OWNER TO tapis;
ALTER SEQUENCE job_queues_id_seq RESTART WITH 1;
CREATE UNIQUE INDEX job_queues_name_idx ON job_queues (name);
CREATE UNIQUE INDEX job_queues_priority_idx ON job_queues (priority desc);
CREATE UNIQUE INDEX job_queues_uuid_idx ON job_queues (uuid);

-- ----------------------------------------------------------------------------------------
--                                       Job Recovery
-- ----------------------------------------------------------------------------------------
-- Job Recovery table
CREATE TABLE job_recovery
(
  id                          serial4 PRIMARY KEY,
  tenant_id                   character varying(24) NOT NULL,
  condition_code              character varying(64) NOT NULL,
  tester_type                 character varying(64) NOT NULL,
  tester_parms                character varying(2048) NOT NULL,
  policy_type                 character varying(64) NOT NULL,
  policy_parms                character varying(2048) NOT NULL,
  num_attempts                integer NOT NULL,
  next_attempt                timestamp without time zone NOT NULL,
  created                     timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  last_updated                timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  tester_hash                 character varying(64) NOT NULL
);  
ALTER TABLE job_recovery OWNER TO tapis;
ALTER SEQUENCE job_recovery_id_seq RESTART WITH 1;
CREATE UNIQUE INDEX job_recovery_tenant_hash_idx ON job_recovery (tenant_id, tester_hash);
CREATE INDEX job_recovery_next_attempt_idx ON job_recovery (next_attempt DESC);

-- ----------------------------------------------------------------------------------------
--                                       Job Blocked
-- ----------------------------------------------------------------------------------------
-- Job Recovery table
CREATE TABLE job_blocked
(
  id                          serial4 PRIMARY KEY,
  recovery_id                 integer NOT NULL,
  created                     timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  success_status              job_status_enum NOT NULL,
  job_uuid                    character varying(64) NOT NULL,
  status_message              character varying(2048) NOT NULL,
  FOREIGN KEY (job_uuid) REFERENCES jobs (uuid) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (recovery_id) REFERENCES job_recovery (id) ON DELETE CASCADE ON UPDATE CASCADE
);
ALTER TABLE job_blocked OWNER TO tapis;
ALTER SEQUENCE job_blocked_id_seq RESTART WITH 1;
CREATE UNIQUE INDEX job_blocked_job_uuid_idx ON job_blocked (job_uuid);
CREATE INDEX job_blocked_recovery_id_idx ON job_blocked (recovery_id);

