# SqueezeRadar — Architecture Document

**Version:** 1.0  
**Last Updated:** 2026  
**Authors:** Partner A, Partner B  
**Status:** Active Development

---

## 1. System Overview

SqueezeRadar is a real-time behavioral finance intelligence platform that detects emerging short squeeze conditions by fusing alternative social data streams with market microstructure signals. The system is designed as a distributed, polyglot data pipeline that ingests, processes, scores, and serves squeeze intelligence across 400+ monitored stocks.

The platform is divided into six functional layers:

```
┌─────────────────────────────────────────────────────────────────────┐
│  LAYER 6 — PRESENTATION         React Dashboard + WebSocket Alerts  │
├─────────────────────────────────────────────────────────────────────┤
│  LAYER 5 — API                  FastAPI REST + WebSocket Server      │
├─────────────────────────────────────────────────────────────────────┤
│  LAYER 4 — ML SCORING           LightGBM + FinBERT + Isolation Forest│
├─────────────────────────────────────────────────────────────────────┤
│  LAYER 3 — FEATURE ENGINEERING  Market + Social + NLP + Options      │
├─────────────────────────────────────────────────────────────────────┤
│  LAYER 2 — DATA COLLECTION      10 collectors, tiered refresh rates  │
├─────────────────────────────────────────────────────────────────────┤
│  LAYER 1 — INFRASTRUCTURE       Kafka + 5 databases + Docker         │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. High-Level Data Flow

```
External Sources                  Internal Pipeline
────────────────                  ─────────────────

Reddit API    ──┐
StockTwits    ──┤
Google Trends ──┤──► Collectors ──► Kafka Topics ──► Feature Engine
yfinance      ──┤                                          │
iborrowdesk   ──┤                                          ▼
FINRA         ──┤                                    ML Scoring Engine
SEC EDGAR     ──┤                                          │
NewsAPI       ──┤                                          ▼
Options chain ──┘                              Score Storage + Alerts
                                                           │
                                                           ▼
                                               Redis Cache + TimescaleDB
                                                           │
                                                           ▼
                                               FastAPI ──► React Dashboard
```

---

## 3. Component Architecture

### 3.1 Universe Screener

Runs daily at 9:31 AM ET (one minute after market open).

**Responsibility:** Determine which stocks enter the monitoring pipeline.

**Process:**
1. Pull all ~8,000 US equities via Finviz screener
2. Apply filters: Float < 200M, Short Interest > 15%, Avg Volume > 200K
3. Compare result against existing database state
4. Promote new tickers from UNTRACKED → CANDIDATE
5. Move tickers that fail the screen from ACTIVE → COOLING

**Output:** 200–400 CANDIDATE tickers per day

**Technology:** Python, finvizfinance, yfinance, PostgreSQL

---

### 3.2 Data Collection Layer

Ten independent collectors, each owning one data source. Each collector follows the same interface:

```
Input:  ticker (string)
Output: structured dict saved to correct database
        + entry written to collection_log table
```

**Collector inventory:**

| Collector | Source | Refresh Rate | Database | Partner |
|---|---|---|---|---|
| yfinance_collector | Yahoo Finance | 15 min (market hours) | TimescaleDB | A |
| iborrowdesk_collector | iborrowdesk.com | 3x per day | Cassandra | A |
| stockanalysis_collector | stockanalysis.com | Daily | PostgreSQL | A |
| finra_collector | FINRA short sale files | Daily (next day) | Cassandra | A |
| sec_edgar_collector | SEC EDGAR API | Daily | PostgreSQL | A |
| reddit_collector | Reddit API (PRAW) | Adaptive (30s–15min) | MongoDB | B |
| stocktwits_collector | StockTwits API | Every 30 min | MongoDB | B |
| pytrends_collector | Google Trends | Hourly | PostgreSQL | B |
| options_collector | yfinance options chain | Every 15 min | PostgreSQL | B |
| news_collector | yfinance news + RSS | Every 15 min | MongoDB | B |

**Adaptive polling** — Reddit polling interval changes based on lifecycle phase:

```
Early Setup  → 10 min
Heating Up   → 5 min
Squeeze Zone → 1 min
Danger Zone  → 30 seconds
Cooldown     → 15 min
```

---

### 3.3 Stock State Machine

Every stock in the system has exactly one status at all times. Status transitions are the core mechanism that controls pipeline compute allocation.

```
              Daily screen passes + Gate 1 + Gate 2
UNTRACKED ──────────────────────────────────────► CANDIDATE ──► ACTIVE
    ▲                                                               │
    │                                                    Signal drops
    │                                                    3+ cycles  │
    │                                                               ▼
    │                                                           COOLING
    │                                                               │
    └───────────────── 3 days COOLING, no recovery ────────────────┘
                                                                    │
                                                                ARCHIVED
```

**Gate 1 — Data Sufficiency** (must pass before Gate 2):
- At least 2 days of time-series data collected
- At least 1 complete TYPE A snapshot exists
- No data quality failures in last 3 collection runs

**Gate 2 — Signal Threshold** (must meet any 2 of 4 categories):
- Market structure: Short interest > 20% OR DTC > 5 OR borrow rate > 25%
- Momentum: Volume > 3x avg OR price up > 10% in 5 days
- Social: Reddit mentions > 50 in 24h OR velocity > 100% increase
- Options: Call/put ratio > 2.5 OR IV rank > 60%

**Fast-track override:** Stocks with sudden viral events (>500 mentions/hour) skip CANDIDATE and promote directly to ACTIVE, with data backfill running in the background.

---

### 3.4 Feature Engineering Layer

Triggered (not scheduled) — runs whenever new data arrives for a stock.

**Feature taxonomy:**

```
TYPE A — Slow snapshots (fetched, stored as latest value)
  short_interest_pct, days_to_cover, borrow_rate,
  float_shares, shares_outstanding

TYPE B — Time-series (append only, never overwrite)
  price, volume, reddit_mentions, sentiment_score,
  options_flow, google_trends

TYPE C — Derived features (computed from A + B on trigger)
  float_rotation, mention_velocity, volume_acceleration,
  borrow_rate_delta, si_proxy_estimate, squeeze_score

TYPE D — Events (immutable, append only)
  phase_transitions, catalysts, sec_filings,
  earnings_dates, fast_track_events
```

**28 features in the final vector:**

Market (12): short_interest_pct, days_to_cover, borrow_rate, si_staleness_penalty, volume_ratio, float_rotation, volume_acceleration, price_change_1d, price_change_5d, rsi_14, distance_from_52w_high, borrow_rate_delta

Options (5): call_put_ratio, iv_rank, options_available, max_pain_distance, unusual_sweep_flag

Social (8): mentions_24h, mention_velocity_1h, mention_velocity_6h, mention_acceleration, subreddit_diversity, weighted_sentiment, google_trend_score, stocktwits_bull_ratio

NLP (3): narrative_squeeze_prob, keyword_urgency_score, sentiment_finbert

---

### 3.5 NLP Pipeline

Runs as a dedicated Celery worker queue, consuming unprocessed Reddit posts from MongoDB continuously.

```
MongoDB (nlp_processed=False)
        │
        ▼
FinBERT sentiment analysis
        │
        ▼
KeyBERT keyword extraction
        │
        ▼
Narrative classifier (rule-based → LightGBM)
        │
        ▼
BERTopic topic clustering
        │
        ▼
MongoDB (nlp_processed=True, fields updated)
        │
        ▼
Feature assembler reads NLP fields
```

**Narrative categories:**
- squeeze_play
- meme_hype
- fundamental_buy
- momentum_trade
- fear_uncertainty

---

### 3.6 ML Scoring Engine

Four models running in ensemble. Triggered by `features.ready` Kafka topic event.

**Model 1 — Lifecycle Classifier**
- Algorithm: LightGBM (multiclass)
- Input: 28-feature normalized vector
- Output: Phase label + probability distribution across 5 phases
- Training data: ~2,500 labeled rows from 14 historical squeeze events

**Model 2 — Hype Anomaly Detector**
- Algorithm: Isolation Forest
- Input: Mention velocity time-series features (rolling windows)
- Output: Anomaly score 0–1 (1 = highly anomalous spike)
- No labels needed — unsupervised

**Model 3 — Outcome Similarity Predictor**
- Algorithm: XGBoost + Platt scaling
- Input: Full 28-feature vector
- Output: Calibrated probability that conditions resemble pre-squeeze events
- Framed as similarity score, not price prediction

**Model 4 — NLP Narrative Classifier**
- Algorithm: FinBERT (pre-trained) + LightGBM on embeddings
- Input: Raw Reddit post text
- Output: Narrative category + squeeze_prob score

**Score aggregation:**
```
squeeze_score = base_score(phase)
              + hype_acceleration × 10
              + borrow_rate_bonus
              + options_bonus
              (capped at 100)
```

**SHAP explainability:** Every score output includes a SHAP explanation identifying the top 5 features driving the classification, rendered as human-readable text on the dashboard.

---

### 3.7 Streaming Layer (Month 2)

Apache Kafka connects all pipeline stages. Each stage publishes to a topic and consumes from upstream topics, making stages fully independent.

**Topics:**

| Topic | Producer | Consumers | Retention |
|---|---|---|---|
| raw.reddit.posts | Reddit collector | MongoDB writer, NLP pipeline | 7 days |
| raw.market.ticks | yfinance collector | Cassandra writer, feature engine | 48 hours |
| raw.options.flow | Options collector | TimescaleDB writer | 7 days |
| pipeline.features.ready | Feature engine | ML scoring engine | 24 hours |
| pipeline.scores.updated | ML engine | Redis writer, TimescaleDB, WebSocket | 24 hours |
| alerts.phase.transitions | Score aggregator | WebSocket broadcaster | 7 days |

---

## 4. Database Architecture

SqueezeRadar uses five databases, each chosen for a specific data access pattern.

### 4.1 PostgreSQL + TimescaleDB (port 5432)

**Used for:** Stock entity state, score history, OHLCV time-series, options snapshots, audit log

**Why PostgreSQL:** ACID guarantees for stock status transitions. Relational integrity between stocks and events.

**Why TimescaleDB:** Automatic time-series partitioning on stock_scores and stock_timeseries. Continuous aggregates for hourly score summaries without manual queries.

**Key tables:**
- `stocks` — master entity table with status state machine
- `stock_snapshots` — TYPE A slow feature snapshots
- `stock_timeseries` — hypertable for OHLCV data
- `stock_scores` — hypertable for ML score history
- `stock_events` — TYPE D immutable event log
- `collection_log` — pipeline health tracking

### 4.2 MongoDB (port 27017)

**Used for:** Raw Reddit posts, news articles, StockTwits messages

**Why MongoDB:** Schema-free documents — Reddit posts vary in structure, have nested entities, awards, flair, and metadata that changes frequently. 90-day TTL indexes automatically expire old documents.

**Key collections:**
- `reddit_posts` — raw posts with NLP outputs written back
- `news_articles` — headlines with catalyst flags
- `stocktwits` — messages with bull/bear sentiment

### 4.3 Redis (port 6379)

**Used for:** Live score cache, squeeze leaderboard, Celery task broker

**Why Redis:** Sub-millisecond reads for dashboard. Sorted sets for real-time leaderboard (ZADD/ZRANGE). TTL-based cache invalidation (5 minute TTL on scores).

**Key structures:**
- `score:{ticker}` — latest squeeze score dict (TTL: 5 min)
- `phase:{ticker}` — current phase label (TTL: 5 min)
- `leaderboard:squeeze_score` — sorted set of all active tickers by score
- Celery broker and result backend

### 4.4 Apache Cassandra (port 9042)

**Used for:** High-write market tick data, borrow rate history, short volume daily

**Why Cassandra:** Handles 10,000+ writes per second without degradation. Time-ordered data within partitions. Automatic TTL expiration. The 400-stock × 1-minute OHLCV feed produces ~400 writes/minute — Cassandra handles this trivially.

**Key tables:**
- `market_ticks` — partitioned by (ticker, date), ordered by timestamp
- `borrow_rate_history` — 3x daily updates per ticker
- `short_volume_daily` — FINRA daily short sale volume

### 4.5 Elasticsearch (port 9200)

**Used for:** Full-text search on Reddit posts and news, narrative trend analysis

**Why Elasticsearch:** Full-text search across millions of Reddit posts is not possible with SQL. Allows queries like "find all posts mentioning naked shorts AND gamma squeeze in the last 6 hours." Also powers the dashboard search bar.

**Key indices:**
- `reddit_posts` — English analyzer, keyword fields for ticker/subreddit
- `news_articles` — headline text search with catalyst flag filtering

### 4.6 Apache Parquet on MinIO (Month 2)

**Used for:** Cold storage of data older than 90 days, ML training datasets

**Why Parquet + MinIO:** Columnar format reads 10-100x faster than row-based formats for analytical queries. MinIO is S3-compatible and self-hosted. Apache Spark reads Parquet natively for distributed model training.

---

## 5. API Layer

FastAPI application serving the React dashboard.

**REST endpoints:**

| Method | Endpoint | Description |
|---|---|---|
| GET | /api/leaderboard | Top candidates sorted by squeeze score |
| GET | /api/stock/{ticker} | Full stock detail with current score |
| GET | /api/stock/{ticker}/history | Score history for chart |
| GET | /api/stock/{ticker}/reddit | Recent posts + sentiment |
| GET | /api/stock/{ticker}/options | Options summary |
| GET | /api/stock/{ticker}/events | Phase transitions + catalysts |
| GET | /api/screener/run | Trigger manual screen run |
| WS | /ws | WebSocket for real-time score updates |

**Read pattern:** All dashboard reads go to Redis cache first. Cache miss falls back to TimescaleDB. Raw social data reads go to MongoDB. The API never reads directly from Cassandra (too slow for request/response) — Cassandra data flows through the feature engine into TimescaleDB scores.

---

## 6. Infrastructure

**Local development:** Docker Compose with all 5 databases as containers.

**Production (Phase 2):**

```
AWS Architecture:
  EC2 (t3.large)        — FastAPI + Celery workers
  RDS (db.t3.medium)    — PostgreSQL + TimescaleDB
  ElastiCache           — Redis
  MSK                   — Managed Kafka
  EC2 (r6g.large)       — Cassandra (or AWS Keyspaces)
  OpenSearch            — Elasticsearch managed
  S3                    — Parquet cold storage
  ECR                   — Docker image registry
```

**CI/CD:**
- GitHub Actions for automated testing on every PR
- No merge to main without passing test suite
- Docker images built and tagged on merge

---

## 7. Security Considerations

- API keys stored in environment variables, never in code
- `.env` in `.gitignore`, only `.env.template` committed
- Database credentials rotated per environment
- No user financial data stored (read-only intelligence platform)
- Rate limiting on all public API endpoints
- WebSocket connections authenticated via JWT

---

## 8. Scalability Limits and Upgrade Path

| Current Limit | Upgrade Path |
|---|---|
| 400 monitored stocks | Add more Celery workers horizontally |
| 2-week SI data staleness | Upgrade to Fintel API ($40/mo) |
| 15-min price delay | Upgrade to Alpaca/Polygon real-time feed |
| Single Cassandra node | Add nodes to cluster (zero downtime) |
| Local Docker databases | Migrate to AWS managed services |
| LightGBM on 2,500 rows | Retrain monthly as data accumulates |
