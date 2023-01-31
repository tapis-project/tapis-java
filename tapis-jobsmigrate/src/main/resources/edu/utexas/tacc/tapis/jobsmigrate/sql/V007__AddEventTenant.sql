-- Add tenant field to job events.

ALTER TABLE job_events ADD COLUMN IF NOT EXISTS tenant character varying(24) NOT NULL DEFAULT 'unknown';
