-- Add new job event type to the job_event_enum in the job_events table.
ALTER TYPE job_event_enum ADD VALUE 'JOB_USER_EVENT';
ALTER TABLE job_events ALTER COLUMN event_detail TYPE character varying(16384);