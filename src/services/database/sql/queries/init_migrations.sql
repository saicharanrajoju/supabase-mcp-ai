-- Initialize migrations infrastructure
-- Create the migrations schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS supabase_migrations;

-- Create the migrations table if it doesn't exist
CREATE TABLE IF NOT EXISTS supabase_migrations.schema_migrations (
    version TEXT PRIMARY KEY,
    statements TEXT[] NOT NULL,
    name TEXT NOT NULL
);
