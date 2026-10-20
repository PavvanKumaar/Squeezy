# SqueezeRadar — Design Document

**Version:** 1.0  
**Last Updated:** 2026  
**Authors:** Partner A, Partner B  
**Status:** Active Development

---

## 1. Design Philosophy

### 1.1 Core Principles

**Explainability over accuracy.** A score that traders understand and can sanity-check is more valuable than a marginally more accurate black box. Every score must come with a human-readable explanation of why it was generated.

**Behavioral finance, not price prediction.** The platform detects conditions and patterns, not outcomes. Language like "this stock will squeeze" is never used. Language like "conditions are similar to pre-squeeze historical events" is preferred.

**Data honesty.** Stale data is displayed with explicit staleness warnings. Missing features are flagged. Confidence levels are shown. The platform never pretends to know more than it does.

**Real signal over noise.** The value of SqueezeRadar is multi-signal fusion. Each individual indicator is available elsewhere for free. The combination, the lifecycle framing, and the explainability layer are the product.

---

## 2. System Design Decisions

### 2.1 Why a Lifecycle Model Instead of a Score

Most squeeze detection tools output a single number — a "squeeze score." This is wrong for two reasons.

First, a stock that is in the Danger Zone (score: 90) and a stock in Early Setup (score: 30) require completely different responses. A score alone does not convey this.

Second, squeeze events have temporal structure. They build, peak, and collapse. A lifecycle model captures this directionality. Knowing a stock just transitioned from Heating Up → Squeeze Zone is more actionable than knowing its score went from 55 to 75.

The five lifecycle phases are defined as distinct behavioral states, not score ranges. The ML model classifies the phase directly from the feature vector. The score is derived from the phase, not the other way around.

### 2.2 Why Five Phases

```
Early Setup   → squeeze fuel exists but no fire yet
Heating Up    → retail starting to notice, momentum building
Squeeze Zone  → all signals firing, active squeeze pressure
Danger Zone   → mania peak, most dangerous to enter
Cooldown      → event over, shorts covered or retail left
```

Five phases is the minimum granularity needed to be actionable. Three phases (pre/during/post) loses critical distinction between Early Setup (safe to watch) and Danger Zone (dangerous to enter). Seven or more phases creates false precision — the feature data cannot support that resolution.

### 2.3 Why LightGBM for Phase Classification

The training dataset has approximately 2,500 rows — too small for neural networks, appropriate for gradient boosted trees. LightGBM specifically was chosen over XGBoost because:

- Faster training on small datasets
- Better handling of imbalanced classes natively
- Native SHAP support (same library, no compatibility issues)
- Slightly better calibrated probability outputs on small datasets

The model will be replaced or augmented as training data grows. The feature engineering and database layers are designed to be model-agnostic — swapping LightGBM for XGBoost or a neural net requires only changing the model file.

### 2.4 Why Triggered Feature Engineering

Feature engineering could be scheduled (run every 5 minutes for all ACTIVE stocks) or triggered (run when new data arrives). We chose triggered because:

- A stock with 30-second Reddit polling needs features computed every 30 seconds during a squeeze. A scheduled 5-minute job would miss the inflection.
- A stock in Cooldown phase barely changes in 15 minutes. Running features every 5 minutes wastes compute.
- Triggered architecture naturally implements adaptive compute intensity — busy stocks get more compute, quiet stocks get less.

The trigger mechanism is a Kafka event (`pipeline.features.ready`) emitted by each collector after saving data. The feature engine subscribes and runs.

### 2.5 Why MongoDB for Reddit Posts

Reddit posts are schema-free. A post may have awards, flair, distinguished status, crosspost data, media metadata, and user flair — all optional, all variable. Storing this in PostgreSQL requires either a JSONB column (losing query-ability) or a wide table with 50+ nullable columns.

MongoDB stores the document exactly as received and allows querying any field via indexes. The NLP pipeline writes sentiment and keyword results back to the same document — this is a natural document update pattern that fits MongoDB perfectly.

The 90-day TTL index handles data retention automatically without a separate archival job.

### 2.6 Why Cassandra for Market Ticks

At 400 ACTIVE stocks with 1-minute OHLCV resolution during market hours (6.5 hours × 60 minutes × 400 stocks = 156,000 rows per day), write throughput is the primary concern. PostgreSQL degrades significantly above ~10,000 writes per minute. Cassandra handles this trivially with its LSM-tree storage.

The partition key `(ticker, date)` means all ticks for a given stock on a given day are stored together — the most common query pattern. The clustering key `ts DESC` means the most recent data is read first.

### 2.7 Why Not Real-Time Price Data

The free version uses yfinance with 15-minute delayed prices. For the purposes of squeeze detection:

- Short interest changes daily
- Borrow rate changes 3x per day
- Reddit velocity changes by the minute
- Options OI rebalances daily
- Price is the outcome, not the input

The most valuable features (borrow rate, Reddit velocity, options flow) are all available faster than the 15-minute price delay. Price momentum is useful but not the primary driver of phase classification. The upgrade path to real-time prices (Alpaca, Polygon.io) is documented but not required for MVP.

---

## 3. Database Schema Design Decisions

### 3.1 Stocks Table Design

The `stocks` table is the master entity table. It deliberately does not store time-series data — that goes in `stock_timeseries`. It stores only the current state of the entity.

The `status` field implements the state machine. It is an unconstrained VARCHAR (not an enum) to allow adding new states without schema migration.

The `fast_tracked` boolean flag is permanent — once a stock is fast-tracked, that fact is retained in its history even after it moves to ACTIVE or ARCHIVED.

### 3.2 Feature Type Taxonomy

The four feature types (A/B/C/D) are a deliberate design that determines storage strategy:

```
TYPE A (slow snapshots) — one row per collection, value + timestamp
  → Stores current value, no full history needed
  → Query: SELECT * FROM stock_snapshots WHERE ticker = 'GME' ORDER BY collected_at DESC LIMIT 1

TYPE B (time-series) — one row per interval, append only
  → Full history needed for feature computation and charting
  → Query: SELECT * FROM stock_timeseries WHERE ticker = 'GME' AND time > NOW() - INTERVAL '30 days'

TYPE C (derived) — one row per scoring cycle, generated values
  → Full history needed for score chart on dashboard
  → Query: SELECT time, squeeze_score, phase_label FROM stock_scores WHERE ticker = 'GME'

TYPE D (events) — one row per event, immutable
  → Complete audit trail of what happened and when
  → Query: SELECT * FROM stock_events WHERE ticker = 'GME' ORDER BY occurred_at DESC
```

### 3.3 Collection Log Design

The `collection_log` table is the operational heartbeat of the system. It answers the question "what has the pipeline done in the last hour" without needing to query five different databases.

Every collector writes to this table regardless of success or failure. This means:
- A failing source is detectable by querying `WHERE status = 'failed'`
- Collection gaps are detectable by querying for expected sources that haven't logged recently
- Duration tracking enables performance monitoring per source over time

### 3.4 Redis Key Design

```
score:{ticker}              → Hash: full score dict, TTL 5 min
phase:{ticker}              → String: phase label, TTL 5 min
feature_vector:{ticker}     → Hash: latest features, TTL 2 min
leaderboard:squeeze_score   → Sorted Set: all ACTIVE tickers by score
watchlist:active            → Set: all tickers in ACTIVE status
alert_channel               → Pub/Sub: phase transition broadcasts
```

The leaderboard sorted set allows O(log N) insertion and O(log N + K) range queries — getting the top 20 tickers by score is a single `ZREVRANGE leaderboard:squeeze_score 0 19` command.

---

## 4. ML Design Decisions

### 4.1 Training Data Strategy

The squeeze event dataset is small (14 confirmed events). Augmentation strategies:

**Temporal sampling:** Each squeeze event generates 30–60 labeled training rows (one per trading day from 60 days before to 30 days after the squeeze). This multiplies 14 events into ~2,500 rows.

**Negative examples:** Failed squeezes (high SI + Reddit hype but no price move) are explicitly included. Without these, the model overfits to the pattern "high SI + social activity = Squeeze Zone" regardless of whether the squeeze actually materialized.

**Phase boundary definition:** Phase boundaries are defined by days-to-squeeze, not by price action. This prevents label leakage — the model trains on the conditions before the squeeze, not the fact that a squeeze occurred.

### 4.2 Class Imbalance Handling

The five phases are not equally represented in historical data:

```
Estimated distribution in training data:
  Early Setup   → 35% (most common — stocks spend weeks here)
  Heating Up    → 20%
  Squeeze Zone  → 10% (rare — squeezes are brief)
  Danger Zone   → 8%
  Cooldown      → 27%
```

LightGBM's `class_weight='balanced'` parameter automatically adjusts loss function weights to compensate. Squeeze Zone recall is tracked separately as the primary metric — it is more important to catch squeezes (recall) than to avoid false alarms (precision) at this phase.

### 4.3 Feature Normalization Strategy

All features are normalized to [-1, 1] or [0, 1] before entering the model. The normalization ranges are fit on the training data and saved alongside the model. At inference time, the same scaler is applied.

Short interest % is normalized against historical maximum observed (80% for meme stocks). Volume ratio is log-normalized because volume distributions are heavily right-skewed. Social features are normalized against 7-day rolling baseline per ticker, not absolute values.

### 4.4 SHAP Explanation Design

SHAP values are computed using TreeExplainer (O(T × L) complexity, fast for tree models). The explanation pipeline:

```
1. Run SHAP on the feature vector for the predicted class
2. Sort features by |SHAP value| descending
3. Take top 5 features
4. For each feature, retrieve the raw (un-normalized) value
5. Determine direction (positive SHAP = pushing score up)
6. Generate English string per feature:
   "borrow_rate (47.2%) is pushing score up strongly"
7. Concatenate into explanation paragraph
```

The raw value is always displayed alongside the SHAP contribution — users see both "what is the value" and "how much is it contributing" simultaneously.

### 4.5 Model Versioning

Every trained model is given a version string (v1.0, v1.1, v2.0). This version is stored in every `stock_scores` row. When the model is retrained:

- Historical scores are not retroactively changed
- New scores use the new model version
- Dashboard can filter score history by model version
- MLflow tracks all experiments, parameters, and metrics

---

## 5. API Design Decisions

### 5.1 Read Architecture

```
Dashboard request for leaderboard:
  1. API reads from Redis sorted set (< 1ms)
  2. Returns top 20 with cached scores
  Total: < 10ms

Dashboard request for score history chart:
  1. Redis miss (history not cached)
  2. API queries TimescaleDB hypertable
  3. Returns 7-day hourly aggregated data
  Total: < 100ms

Dashboard request for Reddit posts:
  1. API queries MongoDB with compound index (ticker + time)
  2. Returns last 50 posts with NLP fields
  Total: < 50ms
```

The API never makes requests to Cassandra directly. Cassandra stores raw ticks — these are consumed by the feature engine and transformed into TimescaleDB score records. The API layer only reads from Redis (hot) and TimescaleDB (warm) and MongoDB (social data).

### 5.2 WebSocket Design

Phase transition alerts are the primary real-time output. The WebSocket message format:

```json
{
  "type": "phase_transition",
  "ticker": "GME",
  "from_phase": "Heating Up",
  "to_phase": "Squeeze Zone",
  "squeeze_score": 76.4,
  "explanation": "Borrow rate spiked to 47%, mention velocity +340% in 1h",
  "timestamp": "2026-05-07T14:23:11Z"
}
```

The dashboard renders this as a toast notification with the ticker, transition direction, and key driver visible. The notification persists for 10 seconds and is dismissable.

---

## 6. Dashboard Design Decisions

### 6.1 Information Hierarchy

The dashboard is designed around a single question: "Which stock do I watch right now, and why?"

**Level 1 — Leaderboard (answer: which stocks):**
Shows all ACTIVE stocks sorted by squeeze score. Each row shows enough to make a decision without clicking: phase, score, short interest, Reddit velocity, float, risk level.

**Level 2 — Stock Detail (answer: why this stock):**
Shows the lifecycle progression for the selected stock, the signal breakdown with per-signal contribution, the SHAP explanation, and the score history chart. This is the "understand the thesis" view.

**Level 3 — Raw Data (answer: what is the underlying evidence):**
Shows Reddit posts, news feed, options chain details, and subreddit activity. This is the "verify for yourself" view.

### 6.2 Phase Color System

Phase colors are consistent across all components — lifecycle pills, score bars, timeline markers, and toast alerts all use the same palette:

```
Early Setup  → Green   (#639922 / #EAF3DE)
Heating Up   → Amber   (#BA7517 / #FAEEDA)
Squeeze Zone → Red     (#E24B4A / #FCEBEB)
Danger Zone  → Pink    (#D4537E / #FBEAF0)
Cooldown     → Blue    (#378ADD / #E6F1FB)
```

Green → Amber → Red → Pink follows intuitive heat progression. Blue for Cooldown signals "lower temperature."

### 6.3 Score Bar Design

The squeeze score (0–100) is displayed as both a number and a filled bar. The bar color matches the phase color — not a single gradient. This prevents the misleading implication that a score of 75 is "more dangerous" than a score of 70 — what matters is the phase, not the precise score within the phase.

### 6.4 Staleness Indicators

Data staleness is communicated through a three-level system:

```
Fresh (< 3 days)   → No indicator
Stale (3–7 days)   → ⚠️ Yellow warning badge with age
Very stale (7+ days) → 🔴 Red warning badge with age
```

The SI data staleness indicator is always shown because 2-week SI lag is a known limitation. This maintains trust with users — they know exactly how fresh each data point is.

### 6.5 Explainability Display

The SHAP explanation is displayed in two formats:

**Machine format (for analysts):**
A ranked list of the top 5 features with their raw values and SHAP contributions shown as horizontal bars with positive/negative direction.

**Human format (for everyone):**
A single paragraph in plain English generated by the explanation module:
> "GME is in Squeeze Zone primarily because borrow rate has reached 47.2% (strongly bullish signal), Reddit mention velocity increased 340% in the last hour, and unusual call option sweeps totaling $2.4M were detected today."

Both formats are shown simultaneously on the stock detail page.

---

## 7. Coding Standards

### 7.1 Python Standards

```python
# Every collector follows this interface exactly:
class XCollector:
    def __init__(self):
        # Initialize DB connections
        # Initialize session/client
        pass

    def collect(self, ticker: str) -> dict:
        # Main method
        # Always returns a dict
        # Always sets error: bool in return dict
        # Always calls self._log() before returning
        pass

    def _save(self, ticker: str, data: dict):
        # Database write
        # Private method, called by collect()
        pass

    def _log(self, ticker: str, status: str,
              rows: int, error: str, duration_ms: int):
        # Writes to collection_log
        # Called on both success and failure
        pass
```

### 7.2 Return Dict Standards

Every `collect()` method must return a dict with at minimum:

```python
{
    'ticker': str,           # the ticker that was collected
    'collected_at': datetime, # UTC timestamp
    'error': bool,           # False on success, True on any failure
    'error_message': str,    # None on success, error text on failure
}
```

Plus source-specific fields for all collected metrics.

### 7.3 Branch Naming

```
scraper/{source_name}     → data collection work
feature/{feature_name}    → feature engineering work
ml/{model_name}           → ML model work
api/{endpoint_name}       → API work
dashboard/{component}     → frontend work
fix/{description}         → bug fixes
chore/{description}       → setup, config, documentation
```

### 7.4 Commit Message Standards

```
feat: add iborrowdesk borrow rate scraper
fix: handle StockTwits 429 rate limit with backoff
test: add options collector integration test
chore: update requirements.txt with pandas-ta
db: add index on collection_log source column
docs: update ARCHITECTURE.md with Kafka topics
refactor: extract _log() method to base collector class
```

### 7.5 Test Requirements

Every module must have a corresponding test file. Tests must:
- Be runnable independently (`python tests/test_X.py`)
- Print clear ✓ or ✗ for each assertion
- Complete in under 30 seconds
- Not require any manual setup beyond `docker-compose up -d`
- Not depend on the order of other tests

---

## 8. Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Reddit API rate limit exceeded | Medium | High | Adaptive polling, exponential backoff, Pushshift fallback |
| iborrowdesk HTML structure changes | High | Medium | HTML selector versioning, alert on parse failure |
| 2-week SI staleness misleads scoring | High | Medium | SI proxy estimator, staleness display, reduced SI feature weight |
| Elasticsearch memory issues on Docker | High | Low | Set ES_JAVA_OPTS to 512MB, document in README |
| Kaggle WSB dataset not covering needed events | Medium | Medium | Arctic Shift fallback, synthetic augmentation |
| Small training dataset overfits | High | High | Time-based validation split, class weights, cross-validation |
| Regulatory questions about "squeeze detection" | Low | High | Frame as research intelligence, not financial advice, add disclaimer |
| Cassandra slow start breaks setup scripts | High | Low | Add 90-second wait in setup documentation |
| pytrends aggressive rate limiting | High | Medium | 2s sleep between requests, retry on 429, hourly max frequency |
| Partner code conflicts | Medium | Low | Branch per feature, PR review, daily sync |
