-- Add support for jobType by adding the job_type character field.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS job_type character varying(32) NOT NULL DEFAULT 'FORK';