-- Generalize the job_events table.

DROP INDEX IF EXISTS job_events_job_status_idx;
ALTER TABLE job_events ALTER COLUMN job_status TYPE character varying(64);
ALTER TABLE job_events ALTER COLUMN job_status SET NOT NULL;
ALTER TABLE job_events RENAME COLUMN job_status TO event_detail;
CREATE INDEX job_events_event_detail_idx ON job_events (event_detail);