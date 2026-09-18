# SqueezeRadar — Technical Specification

**Version:** 1.0  
**Last Updated:** 2026  
**Authors:** Partner A, Partner B  
**Status:** Active Development

---

## 1. Product Definition

### 1.1 What SqueezeRadar Is

SqueezeRadar is a behavioral finance intelligence platform that identifies, monitors, and classifies potential short squeeze conditions across US equities. It fuses real-time social sentiment data with market microstructure signals to produce an interpretable squeeze lifecycle classification for each monitored stock.

### 1.2 What SqueezeRadar Is Not

- It is not a trading platform. It executes no trades.
- It is not a price prediction system. It predicts conditions, not prices.
- It is not a recommendation engine. Output is labeled as analytical intelligence only.
- It is not a real-time ticker (15-minute data delay on free tier).

### 1.3 Core Value Proposition

Traditional financial analytics platforms ignore social coordination and retail attention dynamics. SqueezeRadar treats these as quantifiable signals, detecting the conditions that precede squeeze events before they become obvious to the broader market.

---

## 2. Functional Requirements

### 2.1 Universe Screener

**FR-001** The system shall screen all US equities daily at 9:31 AM ET on trading days.

**FR-002** The screener shall apply the following filters to determine candidates:
- Float shares < 200,000,000
- Short interest > 15% of float
- Average daily volume (30d) > 200,000 shares

**FR-003** The screener shall compare screen output against the current database state and take the following actions:
- New tickers passing the screen → promote to CANDIDATE
- Existing ACTIVE tickers still passing → no action
- Existing ACTIVE tickers failing the screen → move to COOLING
- COOLING tickers failing for > 3 consecutive days → move to ARCHIVED

**FR-004** The screener shall support fast-track promotion for tickers with sudden viral Reddit activity (>500 mentions in 1 hour) regardless of screener schedule.

### 2.2 Data Collection

**FR-010** The system shall collect data from the following sources:

| Source | Data | Frequency |
|---|---|---|
| yfinance | OHLCV, float, market cap, 52w high/low | 15 min (market hours) |
| iborrowdesk | Borrow rate, available shares | 3x daily |
| stockanalysis.com | Short interest %, days to cover | Daily |
| FINRA | Daily short sale volume | Daily (T+1) |
| SEC EDGAR | 13D/13G filings, Form 4 insider trades | Daily |
| Reddit API | Posts, comments, scores from 7 subreddits | Adaptive (30s–15min) |
| StockTwits | Message stream, bull/bear ratios | Every 30 min |
| Google Trends | Search interest score (0–100) | Hourly |
| yfinance options | Options chain, OI, IV | Every 15 min |
| yfinance news + RSS | Headlines, publication time | Every 15 min |

**FR-011** Every collection run shall write a record to `collection_log` indicating ticker, source, status (success/failed/skipped), rows collected, error message if applicable, and duration in milliseconds.

**FR-012** The system shall handle collection failures gracefully — a failed collection for one source shall not block collection from other sources for the same ticker.

**FR-013** The system shall implement retry logic with exponential backoff for transient network failures (max 3 retries).

**FR-014** The system shall implement adaptive Reddit polling rates based on current lifecycle phase of each stock.

### 2.3 Feature Engineering

**FR-020** The system shall compute a 28-feature normalized vector for each ACTIVE stock whenever new data is received.

**FR-021** The feature vector shall include features from four categories: market microstructure (12), options (5), social (8), and NLP (3).

**FR-022** The system shall compute the following derived features (not directly available from any API):
- float_rotation = today_volume / float_shares
- mention_velocity_1h = (mentions_1h - baseline_per_hour) / baseline_per_hour
- mention_acceleration = (velocity_now - velocity_6h_ago) / |velocity_6h_ago|
- volume_acceleration = volume_1h / avg_hourly_volume_30d
- borrow_rate_delta = borrow_rate_now - borrow_rate_7d_ago
- si_staleness_penalty = min(stale_days / 14, 1.0)

**FR-023** The system shall implement an SI proxy estimator that infers short interest direction from borrow rate trend, short volume ratio, and price action when official SI data is stale.

**FR-024** Feature vectors shall have no NaN values. Missing features shall be filled with zero and flagged in the feature quality score.

### 2.4 NLP Pipeline

**FR-030** The system shall analyze all Reddit posts using FinBERT (ProsusAI/finbert) for finance-specific sentiment.

**FR-031** Sentiment analysis shall assign each post:
- sentiment_label: positive / negative / neutral
- sentiment_score: float in range [-1, 1]

**FR-032** The system shall extract keywords from Reddit posts using KeyBERT and compute a keyword_urgency_score (0–1) based on the presence of squeeze-related vocabulary.

**FR-033** The system shall classify the dominant narrative around each ticker into one of five categories:
- squeeze_play
- meme_hype
- fundamental_buy
- momentum_trade
- fear_uncertainty

**FR-034** NLP processing shall be incremental — only posts with `nlp_processed = False` shall be processed in each batch.

**FR-035** NLP results shall be written back to the MongoDB post document, setting `nlp_processed = True` and `nlp_processed_at` timestamp.

### 2.5 ML Scoring Engine

**FR-040** The system shall classify each ACTIVE stock into one of five lifecycle phases:
- Early Setup
- Heating Up
- Squeeze Zone
- Danger Zone
- Cooldown

**FR-041** Phase classification shall output a probability distribution across all five phases, not just the predicted class.

**FR-042** The system shall compute a unified squeeze score (0–100) combining lifecycle phase, hype anomaly score, and signal strength bonuses.

**FR-043** The system shall generate a SHAP-based explanation for every score, identifying the top 5 features contributing to the classification.

**FR-044** The system shall generate a human-readable explanation string from the SHAP values, suitable for display on the dashboard.

**FR-045** The ML engine shall NOT run for stocks in CANDIDATE or ARCHIVED status.

**FR-046** The ML engine SHALL run when:
- A `features.ready` Kafka event is received
- More than 15 minutes have elapsed since the last score for an ACTIVE stock
- A manual override is requested via the API

**FR-047** When a phase transition is detected (new phase ≠ last stored phase), the system shall:
- Insert a TYPE D event into `stock_events`
- Push a WebSocket alert to all connected dashboard clients
- Increase the polling frequency for that stock immediately

### 2.6 API Requirements

**FR-050** The API shall expose REST endpoints for leaderboard, stock detail, score history, Reddit data, options data, and events.

**FR-051** The API shall expose a WebSocket endpoint at `/ws` for real-time score updates and phase transition alerts.

**FR-052** The API shall read live scores from Redis cache. Cache miss shall fall back to TimescaleDB.

**FR-053** All endpoints shall return responses in under 200ms for cached data.

**FR-054** The API shall support CORS for local development (origin: localhost:3000).

### 2.7 Dashboard Requirements

**FR-060** The dashboard shall display:
- Top 20 squeeze candidates sorted by squeeze score
- Per-stock lifecycle phase with visual progression indicator
- Squeeze score with historical trend chart
- Signal breakdown with per-signal contribution display
- SHAP-based explanation text
- Reddit mention count and sentiment trend
- Subreddit activity distribution
- Options summary (call/put ratio, IV rank, max pain)
- News and catalyst feed
- Real-time phase transition alerts (toast notifications)

**FR-061** The dashboard shall receive real-time updates via WebSocket without page refresh.

**FR-062** The dashboard shall display data staleness warnings for features older than:
- 3 days → yellow warning
- 7 days → red warning

---

## 3. Non-Functional Requirements

### 3.1 Performance

**NFR-001** The feature engineering pipeline shall complete within 5 seconds of receiving new data for a stock.

**NFR-002** The ML scoring engine shall score a single stock in under 500ms.

**NFR-003** The Redis cache shall serve dashboard reads in under 10ms.

**NFR-004** The system shall support 400 simultaneously monitored ACTIVE stocks without degradation.

**NFR-005** The Reddit collector shall process 50 subreddit posts per collection cycle per stock.

### 3.2 Reliability

**NFR-010** The system shall continue operating if any single data source becomes unavailable, using last known values with staleness flags.

**NFR-011** Collection failures shall be logged to `collection_log` and not propagate to the scoring engine as null features.

**NFR-012** Docker containers shall have health checks configured with automatic restart policies.

**NFR-013** The ML model shall degrade gracefully when feature vector has more than 30% missing values — output a low-confidence score rather than erroring.

### 3.3 Data Retention

**NFR-020** MongoDB documents (Reddit posts, news) shall expire after 90 days via TTL index.

**NFR-021** Cassandra market tick data shall expire after 90 days via default_time_to_live.

**NFR-022** TimescaleDB score history shall be retained indefinitely for backtesting purposes.

**NFR-023** Raw data older than 90 days shall be archived to Parquet files on MinIO before TTL expiration.

### 3.4 Security

**NFR-030** No API keys or secrets shall be committed to the Git repository.

**NFR-031** The `.env` file shall be in `.gitignore`. Only `.env.template` shall be committed.

**NFR-032** Database credentials shall differ between development and production environments.

**NFR-033** The platform shall not store any personally identifiable information about Reddit users beyond their username and post content.

---

## 4. Data Specifications

### 4.1 Stock Status Values

| Status | Description | ML Runs | Data Collected |
|---|---|---|---|
| UNTRACKED | Never passed screen | No | No |
| CANDIDATE | Passed screen, in data sufficiency period | No | Yes (slow rate) |
| ACTIVE | Fully monitored, ML scoring live | Yes | Yes (adaptive rate) |
| COOLING | Failed screen recently, grace period | No | Yes (slow rate) |
| ARCHIVED | Long-term inactive | No | No |

### 4.2 Lifecycle Phase Definitions

| Phase | Squeeze Score Range | Key Conditions |
|---|---|---|
| Early Setup | 20–40 | High SI, low social awareness |
| Heating Up | 40–60 | SI rising, Reddit activity increasing |
| Squeeze Zone | 60–80 | High SI + volume surge + Reddit mania |
| Danger Zone | 80–95 | All signals maxed, mania peak |
| Cooldown | 0–30 | Momentum declining, shorts covered |

### 4.3 Feature Vector Schema

```
Index  Feature                  Range      Source
─────────────────────────────────────────────────
0      short_interest_pct       0–1        stockanalysis (normalized)
1      days_to_cover            0–1        stockanalysis (normalized)
2      borrow_rate              0–1        iborrowdesk (normalized)
3      si_staleness_penalty     0–1        computed
4      volume_ratio             0–1        yfinance (normalized)
5      float_rotation           0–1        computed
6      volume_acceleration      0–1        computed
7      price_change_1d          -1–1       yfinance
8      price_change_5d          -1–1       yfinance
9      rsi_14                   0–1        computed from yfinance
10     distance_from_52w_high   0–1        yfinance
11     borrow_rate_delta        -1–1       computed
12     call_put_ratio           0–1        yfinance options (normalized)
13     iv_rank                  0–1        computed from options chain
14     options_available        0 or 1     yfinance
15     max_pain_distance        0–1        computed
16     unusual_sweep_flag       0 or 1     computed
17     mentions_24h             0–1        Reddit API (normalized)
18     mention_velocity_1h      -1–1       computed
19     mention_velocity_6h      -1–1       computed
20     mention_acceleration     -1–1       computed
21     subreddit_diversity      0–1        computed (1–7 subs, normalized)
22     weighted_sentiment       -1–1       FinBERT + upvote weighting
23     google_trend_score       0–1        pytrends
24     stocktwits_bull_ratio    0–1        StockTwits API
25     narrative_squeeze_prob   0–1        narrative classifier
26     keyword_urgency_score    0–1        KeyBERT
27     sentiment_finbert        -1–1       FinBERT aggregate
```

### 4.4 Collection Log Schema

```sql
collection_log (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10),
    source          VARCHAR(50),    -- collector name
    status          VARCHAR(20),    -- success/failed/skipped/rate_limited
    rows_collected  INTEGER,
    error_message   TEXT,           -- null on success
    duration_ms     INTEGER,
    collected_at    TIMESTAMP
)
```

### 4.5 Score Output Schema

```python
{
    "ticker": "GME",
    "squeeze_score": 87.3,
    "phase_label": "Squeeze Zone",
    "phase_probabilities": {
        "Early Setup": 0.03,
        "Heating Up": 0.11,
        "Squeeze Zone": 0.73,
        "Danger Zone": 0.11,
        "Cooldown": 0.02
    },
    "hype_acceleration": 0.84,
    "top_drivers": [
        {"feature": "borrow_rate", "contribution": 0.34, "raw_value": 47.2},
        {"feature": "mention_velocity_1h", "contribution": 0.28, "raw_value": 3.4},
        {"feature": "call_put_ratio", "contribution": 0.19, "raw_value": 4.8},
        {"feature": "short_interest_pct", "contribution": 0.12, "raw_value": 0.284},
        {"feature": "volume_ratio", "contribution": 0.07, "raw_value": 6.2}
    ],
    "explanation_text": "Score driven by: borrow_rate (47.2%, +0.34); mention_velocity_1h (+340%, +0.28); call_put_ratio (4.8x, +0.19)",
    "model_version": "v1.0",
    "scored_at": "2026-05-07T14:23:11Z"
}
```

---

## 5. External API Specifications

### 5.1 Reddit API (PRAW)

- Authentication: OAuth2 script app
- Rate limit: 100 requests per minute
- Endpoints used: subreddit.search(), subreddit.hot()
- Subreddits monitored: wallstreetbets, shortsqueeze, stocks, pennystocks, investing, StockMarket, WallStreetBetsELITE
- Data fields extracted: id, title, selftext, score, upvote_ratio, num_comments, created_utc, link_flair_text

### 5.2 StockTwits API

- Authentication: None required for public stream (100 requests/hour)
- Endpoint: GET /api/2/streams/symbol/{ticker}.json
- Data fields extracted: message body, sentiment.basic, likes.total, created_at
- Rate limit handling: 429 response triggers 60-second backoff

### 5.3 SEC EDGAR API

- Authentication: None required (public API)
- Base URL: https://efts.sec.gov/LATEST/search-index
- Forms collected: SC 13D, SC 13G, Form 4
- Rate limit: 10 requests per second (enforced by User-Agent header requirement)
- User-Agent format: SqueezeRadar/1.0 your@email.com

### 5.4 Google Trends (pytrends)

- Authentication: None (unofficial API wrapper)
- Rate limiting: Aggressive — 2 second sleep between requests minimum
- 429 error triggers 60-second backoff
- Keywords per request: ["${ticker}", "{ticker} stock"]
- Timeframe: "now 7-d"

### 5.5 FINRA Short Sale Data

- URL pattern: https://cdn.finra.org/equity/regsho/daily/CNMSshvol{YYYYMMDD}.txt
- Format: pipe-delimited text
- Fields: Symbol, ShortVolume, TotalVolume
- Availability: Next business day by 6:00 AM ET
- Authentication: None required

---

## 6. Error Handling Specification

### 6.1 Collector Error Codes

| Code | Meaning | Action |
|---|---|---|
| RATE_LIMITED | Source returned 429 | Exponential backoff, log, continue |
| NOT_FOUND | Ticker not found in source | Mark as error, log, use last known |
| NETWORK_ERROR | Connection timeout or refused | Retry 3x with backoff |
| PARSE_ERROR | Unexpected response format | Log full response, skip collection |
| AUTH_ERROR | API credentials rejected | Alert immediately, stop collection |
| NO_DATA | Source returned empty data | Log as skipped, use last known |

### 6.2 Feature Engineering Errors

If a feature cannot be computed (missing source data), the system shall:
1. Use 0 as the feature value
2. Increment the feature's null counter
3. If null_count / total_features > 0.30, set confidence = LOW
4. Include null_features list in the score output

### 6.3 ML Scoring Errors

If the feature vector fails validation (NaN values present after filling), the system shall:
1. Log the error with the full feature vector
2. Skip scoring for this cycle
3. Return the last valid cached score with a `stale=true` flag
4. Do not push a phase transition alert based on stale data

---

## 7. Testing Specification

### 7.1 Required Tests Per Collector

Every collector must have a test file covering:
1. Successful collection for a known real ticker (GME)
2. Graceful failure for a fake ticker (FAKE123)
3. Data landed in the correct database
4. Entry written to collection_log with correct status
5. Return dict contains all expected keys

### 7.2 Integration Test Requirements

A week-level integration test must:
1. Run all collectors for 3 test tickers
2. Verify data exists in all 5 databases
3. Verify collection_log has success entries for all sources
4. Complete in under 3 minutes

### 7.3 ML Model Acceptance Criteria

Before a model is promoted to production:
- Overall phase classification accuracy > 55% on held-out test set
- Squeeze Zone recall > 60% (must catch most squeeze setups)
- Cooldown precision > 70% (must not false alarm on cooling stocks)
- Walk-forward backtest on 2022+ events must outperform a simple high-SI baseline

---

## 8. Monitoring Specification

### 8.1 Pipeline Health Metrics

The following metrics shall be tracked and alertable:

| Metric | Warning Threshold | Critical Threshold |
|---|---|---|
| Collection success rate (per source) | < 90% | < 70% |
| Feature engineering lag | > 30 seconds | > 5 minutes |
| ML scoring lag | > 5 minutes | > 15 minutes |
| Redis cache hit rate | < 80% | < 60% |
| MongoDB document write rate | < 100/min | < 10/min |
| Cassandra write latency | > 50ms | > 200ms |

### 8.2 Data Quality Metrics

| Metric | Warning | Critical |
|---|---|---|
| Average feature vector completeness | < 85% | < 70% |
| Stale SI data (> 7 days) | > 20% of ACTIVE stocks | > 50% |
| Reddit collection gap (> 2h without data) | Any ACTIVE stock | — |
| Score generation gap (> 30 min) | Any ACTIVE stock | — |
