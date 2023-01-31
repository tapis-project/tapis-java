-- Increase character limit on event_detail columin in job_events table.
DELETE FROM job_events WHERE LENGTH(event_detail) > 64;
ALTER TABLE job_events ALTER COLUMN event_detail TYPE character varying(64);