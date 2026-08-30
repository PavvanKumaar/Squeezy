-- Enable TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ─────────────────────────────────────────────
-- POSTGRESQL TABLES
-- ─────────────────────────────────────────────

-- Master stock entity (source of truth for state)
CREATE TABLE IF NOT EXISTS stocks (
    id                  SERIAL PRIMARY KEY,
    ticker              VARCHAR(10) UNIQUE NOT NULL,
    company_name        VARCHAR(200),
    sector              VARCHAR(100),
    industry            VARCHAR(100),
    status              VARCHAR(20) DEFAULT 'UNTRACKED',
    -- UNTRACKED / CANDIDATE / ACTIVE / COOLING / ARCHIVED
    fast_tracked        BOOLEAN DEFAULT FALSE,
    options_eligible    BOOLEAN DEFAULT TRUE,
    float_shares        BIGINT,
    market_cap          BIGINT,
    promoted_at         TIMESTAMP,
    activated_at        TIMESTAMP,
    cooled_at           TIMESTAMP,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- TYPE A: slow snapshots (short interest, borrow rate etc)
CREATE TABLE IF NOT EXISTS stock_snapshots (
    id                      SERIAL PRIMARY KEY,
    ticker                  VARCHAR(10) REFERENCES stocks(ticker),
    short_interest_pct      FLOAT,
    days_to_cover           FLOAT,
    borrow_rate             FLOAT,
    available_shares        BIGINT,
    float_shares            BIGINT,
    shares_outstanding      BIGINT,
    short_volume            BIGINT,
    short_volume_ratio      FLOAT,
    si_stale_days           INTEGER DEFAULT 14,
    data_source             VARCHAR(50),
    collected_at            TIMESTAMP DEFAULT NOW()
);

-- TYPE D: immutable event log
CREATE TABLE IF NOT EXISTS stock_events (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10) REFERENCES stocks(ticker),
    event_type      VARCHAR(50),
    -- Values: phase_transition / catalyst / status_change /
    --         fast_track / sec_filing / earnings
    event_data      JSONB,
    occurred_at     TIMESTAMP DEFAULT NOW()
);

-- Pipeline health tracking
CREATE TABLE IF NOT EXISTS collection_log (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10),
    source          VARCHAR(50),
    status          VARCHAR(20),
    -- success / failed / skipped / rate_limited
    rows_collected  INTEGER DEFAULT 0,
    error_message   TEXT,
    duration_ms     INTEGER,
    collected_at    TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- TIMESCALEDB HYPERTABLES (time-series)
-- ─────────────────────────────────────────────

-- OHLCV price and volume data
CREATE TABLE IF NOT EXISTS stock_timeseries (
    time        TIMESTAMPTZ NOT NULL,
    ticker      VARCHAR(10) NOT NULL,
    open        FLOAT,
    high        FLOAT,
    low         FLOAT,
    close       FLOAT,
    volume      BIGINT,
    vwap        FLOAT,
    interval    VARCHAR(10)
);
SELECT create_hypertable(
    'stock_timeseries', 'time',
    if_not_exists => TRUE
);
CREATE INDEX IF NOT EXISTS idx_ts_ticker_time
    ON stock_timeseries (ticker, time DESC);

-- ML squeeze scores over time
CREATE TABLE IF NOT EXISTS stock_scores (
    time                    TIMESTAMPTZ NOT NULL,
    ticker                  VARCHAR(10) NOT NULL,
    squeeze_score           FLOAT,
    phase_label             VARCHAR(30),
    phase_probabilities     JSONB,
    hype_acceleration       FLOAT,
    mention_velocity        FLOAT,
    sentiment_score         FLOAT,
    volume_ratio            FLOAT,
    borrow_rate             FLOAT,
    call_put_ratio          FLOAT,
    shap_top_drivers        JSONB,
    explanation_text        TEXT,
    model_version           VARCHAR(20)
);
SELECT create_hypertable(
    'stock_scores', 'time',
    if_not_exists => TRUE
);
CREATE INDEX IF NOT EXISTS idx_scores_ticker_time
    ON stock_scores (ticker, time DESC);

-- Reddit mention counts over time
CREATE TABLE IF NOT EXISTS mention_timeseries (
    time            TIMESTAMPTZ NOT NULL,
    ticker          VARCHAR(10) NOT NULL,
    mention_count   INTEGER,
    avg_sentiment   FLOAT,
    subreddit_count INTEGER,
    wsb_count       INTEGER,
    shortsqueeze_count INTEGER,
    interval        VARCHAR(10)
);
SELECT create_hypertable(
    'mention_timeseries', 'time',
    if_not_exists => TRUE
);

-- Continuous aggregate: hourly score summary
-- (auto-updated by TimescaleDB)
CREATE MATERIALIZED VIEW IF NOT EXISTS score_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    ticker,
    AVG(squeeze_score)          AS avg_score,
    MAX(squeeze_score)          AS max_score,
    MIN(squeeze_score)          AS min_score,
    LAST(phase_label, time)     AS latest_phase
FROM stock_scores
GROUP BY hour, ticker
WITH NO DATA;

-- ─────────────────────────────────────────────
-- INDEXES
-- ─────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_stocks_status
    ON stocks (status);
CREATE INDEX IF NOT EXISTS idx_snapshots_ticker
    ON stock_snapshots (ticker, collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_ticker
    ON stock_events (ticker, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_collection_log_ticker
    ON collection_log (ticker, collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_collection_log_source
    ON collection_log (source, status, collected_at DESC);