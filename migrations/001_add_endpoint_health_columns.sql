-- Migration: Add health tracking columns to endpoint table
-- Date: 2026-01-31
-- Description: Adds columns for tracking endpoint health status from harvesting attempts
--
-- This migration supports the simplified harvester that runs all endpoints daily
-- instead of the old 4-tier system (core/reliable/other/abandoned).
--
-- New columns:
--   - last_health_status: Current status from most recent harvest attempt
--   - last_health_check: Timestamp of last harvest attempt
--   - last_response_time: Response time in seconds
--   - last_error_message: Error details if harvest failed

-- Add health tracking columns
ALTER TABLE endpoint ADD COLUMN IF NOT EXISTS last_health_status TEXT;
ALTER TABLE endpoint ADD COLUMN IF NOT EXISTS last_health_check TIMESTAMP;
ALTER TABLE endpoint ADD COLUMN IF NOT EXISTS last_response_time FLOAT;
ALTER TABLE endpoint ADD COLUMN IF NOT EXISTS last_error_message TEXT;

-- Add comment to document the column meanings
COMMENT ON COLUMN endpoint.last_health_status IS 'Status from last harvest attempt: success, blocked, timeout, connection_error, malformed, oai_error';
COMMENT ON COLUMN endpoint.last_health_check IS 'Timestamp of when we last attempted to harvest this endpoint';
COMMENT ON COLUMN endpoint.last_response_time IS 'Response time in seconds for the last harvest attempt';
COMMENT ON COLUMN endpoint.last_error_message IS 'Error message from last failed harvest attempt';

-- Add comments to mark legacy columns as deprecated
COMMENT ON COLUMN endpoint.retry_interval IS 'LEGACY - No longer used. Was used for exponential backoff in old tiering system.';
COMMENT ON COLUMN endpoint.retry_at IS 'LEGACY - No longer used. Was used for scheduling retries in old tiering system.';
COMMENT ON COLUMN endpoint.is_core IS 'LEGACY - No longer used. Was used for tiering endpoints in old system.';
