-- ============================================================
-- GCP Billing Tool — PostgreSQL Schema
-- Runs automatically on first Docker startup
-- ============================================================

-- ── Users ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    username      VARCHAR(80)  NOT NULL UNIQUE,
    password_hash VARCHAR(512) NOT NULL,
    role          VARCHAR(20)  NOT NULL DEFAULT 'viewer',
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_login    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);

-- ── Billing Runs ─────────────────────────────────────────────
-- One row per uploaded CSV / calculation run
CREATE TABLE IF NOT EXISTS billing_runs (
    id                   SERIAL PRIMARY KEY,
    run_name             VARCHAR(255),          -- original CSV filename
    uploaded_by          VARCHAR(80),           -- username
    billing_date         DATE,                  -- exact billing date chosen by user
    processed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    total_rows           INTEGER,
    matched_rows         INTEGER,
    total_list_cost      NUMERIC(20,4),         -- up to 10^16 — handles any billing value
    total_after_discount NUMERIC(20,4),
    total_savings        NUMERIC(20,4)
);

CREATE INDEX IF NOT EXISTS idx_runs_processed_at ON billing_runs(processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_uploaded_by  ON billing_runs(uploaded_by);
CREATE INDEX IF NOT EXISTS idx_runs_billing_date ON billing_runs(billing_date);

-- ── Service-level breakdown per run ──────────────────────────
CREATE TABLE IF NOT EXISTS billing_run_services (
    id             SERIAL PRIMARY KEY,
    run_id         INTEGER NOT NULL REFERENCES billing_runs(id) ON DELETE CASCADE,
    service_name   VARCHAR(255),
    list_cost      NUMERIC(20,4),
    after_discount NUMERIC(20,4),
    savings        NUMERIC(20,4),
    row_count      INTEGER
);

CREATE INDEX IF NOT EXISTS idx_run_services_run_id ON billing_run_services(run_id);

-- ── Row-level detail per run ──────────────────────────────────
CREATE TABLE IF NOT EXISTS billing_run_rows (
    id               BIGSERIAL PRIMARY KEY,
    run_id           INTEGER NOT NULL REFERENCES billing_runs(id) ON DELETE CASCADE,
    service_desc     VARCHAR(255),
    sku_desc         VARCHAR(512),
    usage_amount     NUMERIC(24,6),             -- large usage amounts too
    usage_unit       VARCHAR(80),
    list_cost        NUMERIC(20,4),
    discount_pct     NUMERIC(8,4),
    discounted_value NUMERIC(20,4),
    after_discount   NUMERIC(20,4),
    cost_per_unit    NUMERIC(20,8)              -- after_discount / usage_amount
);

-- Migration for existing deployments
ALTER TABLE billing_run_rows ADD COLUMN IF NOT EXISTS cost_per_unit NUMERIC(20,8);

CREATE INDEX IF NOT EXISTS idx_run_rows_run_id ON billing_run_rows(run_id);

-- ============================================================
-- Default seed users  (change passwords after first login!)
-- admin  → password: admin123
-- viewer → password: viewer123
-- ============================================================

INSERT INTO users (username, password_hash, role) VALUES
  ('admin',
   'scrypt:32768:8:1$lhhR6Rzjll3gtl4B$d7a97e2c430f096e96a37be0986c6db03a7d6d8ffa22fde77efe756f47e1a80eb21edfdd8210585ca1a35809f6fe8a1a7d921d588cab72f58a1c1688e45c3c58',
   'admin')
ON CONFLICT (username) DO NOTHING;

INSERT INTO users (username, password_hash, role) VALUES
  ('viewer',
   'scrypt:32768:8:1$Hna2QG3qlo7Cqpd3$21f82095eb23285befcbb5e8076f36a5fdcc43dc3f5ea7f06513766bfc5e496d126a6209221dc35af51aa01360c787d230e68fbfb64506f3c252b97fb9d9d299',
   'viewer')
ON CONFLICT (username) DO NOTHING;

-- ============================================================
-- Useful admin commands
-- (run via: docker exec -it gcp_billing_db psql -U gcpuser -d gcpauth)
-- ============================================================
-- List all billing runs:
--   SELECT id, run_name, uploaded_by, billing_date, processed_at,
--          total_list_cost, total_savings FROM billing_runs ORDER BY processed_at DESC;
--
-- Delete a run (cascades to rows + services):
--   DELETE FROM billing_runs WHERE id = <run_id>;
--
-- List users:
--   SELECT id, username, role, is_active, created_at FROM users;

-- ── Email Alert Configuration ─────────────────────────────────
CREATE TABLE IF NOT EXISTS email_alert_config (
    id              SERIAL PRIMARY KEY,
    enabled         BOOLEAN      NOT NULL DEFAULT FALSE,
    smtp_host       VARCHAR(255) NOT NULL DEFAULT '',
    smtp_port       INTEGER      NOT NULL DEFAULT 587,
    smtp_user       VARCHAR(255) NOT NULL DEFAULT '',
    smtp_password   VARCHAR(512) NOT NULL DEFAULT '',
    smtp_use_tls    BOOLEAN      NOT NULL DEFAULT TRUE,
    from_address    VARCHAR(255) NOT NULL DEFAULT '',
    recipients      TEXT         NOT NULL DEFAULT '',   -- comma-separated emails
    schedule_time   VARCHAR(10)  NOT NULL DEFAULT '08:00', -- HH:MM daily trigger
    send_mode       VARCHAR(20)  NOT NULL DEFAULT 'manual', -- 'manual' | 'daily' | 'weekly' | 'monthly'
    schedule_days   VARCHAR(64)  NOT NULL DEFAULT '',        -- comma-sep day-of-week for weekly (0=Mon)
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_by      VARCHAR(80)
);

-- one singleton row
INSERT INTO email_alert_config (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- ── Discount SKUs (replaces GCP_Discount_applied.xlsx) ───────
CREATE TABLE IF NOT EXISTS discount_skus (
    id                  SERIAL PRIMARY KEY,
    service_description VARCHAR(255) NOT NULL,
    sku_description     VARCHAR(512) NOT NULL UNIQUE,
    pricing_model       VARCHAR(20)  NOT NULL DEFAULT 'discount',  -- 'discount' | 'unit'
    discount            NUMERIC(8,6) NOT NULL DEFAULT 0,   -- stored as fraction e.g. 0.15 = 15%
    unit_price          NUMERIC(20,8)         DEFAULT NULL, -- fixed unit price (used when pricing_model='unit')
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_by          VARCHAR(80)
);

-- Migration: add new columns if upgrading existing deployment
ALTER TABLE discount_skus ADD COLUMN IF NOT EXISTS pricing_model VARCHAR(20) NOT NULL DEFAULT 'discount';
ALTER TABLE discount_skus ADD COLUMN IF NOT EXISTS unit_price    NUMERIC(20,8) DEFAULT NULL;

CREATE INDEX IF NOT EXISTS idx_discount_skus_sku ON discount_skus(sku_description);
CREATE INDEX IF NOT EXISTS idx_discount_skus_svc ON discount_skus(service_description);

-- ── Discount Import Log ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS discount_import_log (
    id           SERIAL PRIMARY KEY,
    imported_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    imported_by  VARCHAR(80),
    filename     VARCHAR(255),
    total_rows   INTEGER,
    inserted     INTEGER,
    updated      INTEGER,
    skipped      INTEGER
);

-- ── CDN Formula Buckets ──────────────────────────────────────
-- Admin-configurable SKU pattern groups for CDN cost calculator.
-- Each bucket defines which SKUs belong to a metric (hit/miss/lookup).
CREATE TABLE IF NOT EXISTS cdn_formula_buckets (
    id             SERIAL PRIMARY KEY,
    bucket_name    VARCHAR(100) NOT NULL,          -- display name, e.g. "Total Hit GB"
    bucket_role    VARCHAR(20)  NOT NULL,           -- 'hit' | 'miss' | 'lookup'
    sku_patterns   TEXT         NOT NULL DEFAULT '', -- newline-separated substring patterns (case-insensitive)
    require_list_cost_gt_zero BOOLEAN NOT NULL DEFAULT TRUE,
    lookup_divisor NUMERIC(12,2) DEFAULT 10000,    -- only used when bucket_role='lookup'
    sort_order     INTEGER      NOT NULL DEFAULT 0,
    is_active      BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_by     VARCHAR(80)
);

ALTER TABLE cdn_formula_buckets ADD COLUMN IF NOT EXISTS require_list_cost_gt_zero BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE cdn_formula_buckets ADD COLUMN IF NOT EXISTS lookup_divisor NUMERIC(12,2) DEFAULT 10000;
ALTER TABLE cdn_formula_buckets ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0;

-- Default buckets matching your January 2026 sheet
INSERT INTO cdn_formula_buckets (bucket_name, bucket_role, sku_patterns, require_list_cost_gt_zero, lookup_divisor, sort_order, updated_by)
VALUES
  ('Total Hit GB',      'hit',    'networking cloud cdn traffic cache data transfer
network internet data transfer out
network inter zone data transfer out', TRUE, NULL, 1, 'system'),
  ('Total Miss GB',     'miss',   'cache fill
cdn cache fill
download', TRUE, NULL, 2, 'system'),
  ('Total Cache Lookup','lookup', 'cloud cdn cache lookup
cdn cache lookup', FALSE, 10000, 3, 'system')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS email_alert_log (
    id          SERIAL PRIMARY KEY,
    sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status      VARCHAR(20) NOT NULL,   -- 'sent' | 'failed'
    recipients  TEXT,
    subject     VARCHAR(512),
    error_msg   TEXT,
    run_id      INTEGER REFERENCES billing_runs(id) ON DELETE SET NULL,
    prev_run_id INTEGER REFERENCES billing_runs(id) ON DELETE SET NULL,
    -- snapshot of key numbers at send time
    total_after_discount  NUMERIC(20,4),
    prev_after_discount   NUMERIC(20,4),
    delta                 NUMERIC(20,4)
);
