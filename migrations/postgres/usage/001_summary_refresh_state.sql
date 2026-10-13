-- Planned phase-1 usage-worker coordination; no archive database or S3 dependency.
CREATE SCHEMA IF NOT EXISTS metering_usage;
CREATE TABLE IF NOT EXISTS metering_usage.summary_refresh_state (
    period_type TEXT NOT NULL CHECK (period_type IN ('DAY', 'MONTH')),
    period_start DATE NOT NULL,
    dirty_generation BIGINT NOT NULL DEFAULT 0 CHECK (dirty_generation >= 0),
    published_generation BIGINT NOT NULL DEFAULT 0 CHECK (published_generation >= 0),
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RUNNING', 'PUBLISHED', 'FAILED')),
    published_refresh_id UUID,
    publication_uncertain BOOLEAN NOT NULL DEFAULT FALSE,
    source_watermarks JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_code TEXT,
    refreshed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (period_type, period_start),
    CHECK (published_generation <= dirty_generation),
    CHECK (period_type <> 'MONTH' OR EXTRACT(DAY FROM period_start) = 1)
);
CREATE TABLE IF NOT EXISTS metering_usage.summary_refresh_runs (
    refresh_id UUID PRIMARY KEY,
    period_type TEXT NOT NULL,
    period_start DATE NOT NULL,
    target_generation BIGINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'PUBLISHED', 'FAILED')),
    source_watermarks JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_code TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    FOREIGN KEY (period_type, period_start) REFERENCES metering_usage.summary_refresh_state(period_type, period_start)
);
CREATE INDEX IF NOT EXISTS summary_refresh_pending_idx
ON metering_usage.summary_refresh_state(period_type, period_start) WHERE status <> 'PUBLISHED';
