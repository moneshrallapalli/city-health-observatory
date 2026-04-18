-- Smart-city leaderboard: durable score history & manual overrides (PostgreSQL)

CREATE TABLE IF NOT EXISTS score_history (
    id UUID PRIMARY KEY,
    district_id TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL DEFAULT 'stream',
    meta JSONB
);

CREATE INDEX IF NOT EXISTS idx_score_history_district ON score_history (district_id);
CREATE INDEX IF NOT EXISTS idx_score_history_submitted ON score_history (submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_score_history_source ON score_history (source);
