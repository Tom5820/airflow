-- Bitbucket Ingestion State Tracking Table (Simplified)
-- partition = execution date, used with entity_type and repo_slug as unique key

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.bitbucket_ingestion_state (
    id SERIAL PRIMARY KEY,
    partition_date DATE NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    repo_slug VARCHAR(255) NOT NULL,
    is_completed BOOLEAN DEFAULT FALSE,
    UNIQUE(partition_date, entity_type, repo_slug)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_state_lookup 
ON staging.bitbucket_ingestion_state(partition_date, entity_type, is_completed);
