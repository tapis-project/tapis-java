-- Add support for shared application contexts.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS notes jsonb NOT NULL DEFAULT '{}'::json;
