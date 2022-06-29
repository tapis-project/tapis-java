-- Add support for shared application contexts.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS shared_app_ctx boolean NOT NULL DEFAULT FALSE;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS shared_app_ctx_attribs text[];
