-- Postgres initialisation — runs once on first container start
-- Creates two databases: `crm` (application) and `temporal` (Temporal Server)

-- Create the temporal database
CREATE DATABASE temporal;

-- The `crm` database already exists (it is the default POSTGRES_DB from compose)
-- Switch to it and create the schema

\c crm

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS users (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    nickname        TEXT         UNIQUE NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_workflow_id TEXT         NULL
);
