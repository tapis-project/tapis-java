-- Increase character limit on event_detail columin in job_events table.
ALTER TABLE job_events ALTER COLUMN event_detail TYPE character varying(16384);