ALTER TABLE jobs ALTER COLUMN shared_app_ctx TYPE character varying(64);
ALTER TABLE jobs ALTER COLUMN shared_app_ctx SET DEFAULT '';
UPDATE jobs SET shared_app_ctx='' WHERE shared_app_ctx='false';