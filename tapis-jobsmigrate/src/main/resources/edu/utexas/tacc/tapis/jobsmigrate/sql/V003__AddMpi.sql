-- Add support for jobType by adding the job_type character field.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS is_mpi boolean NOT NULL DEFAULT FALSE;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS mpi_cmd character varying(126);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS cmd_prefix character varying(126);