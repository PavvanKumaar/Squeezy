# SqueezeRadar — Task Tracker

**Version:** 1.0  
**Last Updated:** 2026  
**Format:** Phase → Week → Task → Subtasks  
**Status keys:** ⬜ Not started | 🔄 In progress | ✅ Done | ❌ Blocked

---

## Phase 0 — Project Foundation

### Week 0 (Pre-week — Do Together)

#### TASK-001 — Repository and Environment Setup
**Owner:** Both  
**Status:** ⬜

- ⬜ Create GitHub repository (private)
- ⬜ Add partner as collaborator
- ⬜ Both partners clone repo
- ⬜ Create folder structure
- ⬜ Create `.gitignore` with `.env` excluded
- ⬜ Create `.env.template` with all variable names
- ⬜ Agree on Python 3.11
- ⬜ Create `venv` and `requirements.txt`
- ⬜ Both partners run `pip install -r requirements.txt` successfully
- ⬜ Add branch protection rule on `main`

#### TASK-002 — Docker Environment
**Owner:** Both  
**Status:** ⬜

- ⬜ Write `docker-compose.yml` with all 5 databases
- ⬜ Write `Dockerfile` for Python services
- ⬜ `docker-compose up -d` succeeds on both machines
- ⬜ All 5 containers show healthy status
- ⬜ Document port mapping in README

#### TASK-003 — Database Schema Setup
**Owner:** Both  
**Status:** ⬜

- ⬜ Write and apply `db/schema.sql` to PostgreSQL
- ⬜ Verify all 7 tables created
- ⬜ Verify 2 hypertables created (TimescaleDB)
- ⬜ Run `db/mongo_indexes.py` — all indexes created
- ⬜ Run `db/cassandra_setup.py` — keyspace and tables created
- ⬜ Run `db/elastic_setup.py` — indices created
- ⬜ `python tests/test_connections.py` shows 5/5 on both machines

---

## Phase 1 — Data Collection Pipeline

### Week 1 — Core Collectors

#### TASK-101 — yfinance Collector
**Owner:** Partner A  
**Branch:** `scraper/yfinance`  
**Status:** ⬜

- ⬜ `get_stock_info(ticker)` returns dict with all expected fields
- ⬜ `get_ohlcv(ticker, period, interval)` returns DataFrame
- ⬜ `save_stock(ticker)` upserts to `stocks` table
- ⬜ `save_ohlcv(ticker)` writes to `stock_timeseries` hypertable
- ⬜ `get_options_chain(ticker)` returns chains for 4 expiries
- ⬜ `get_earnings_date(ticker)` returns datetime or None
- ⬜ `get_recent_news(ticker)` returns list of dicts
- ⬜ Handles ticker not found gracefully
- ⬜ Writes to `collection_log` on success and failure
- ⬜ `tests/test_yfinance.py` passes all assertions
- ⬜ PR merged to main

#### TASK-102 — iborrowdesk Collector
**Owner:** Partner A  
**Branch:** `scraper/iborrowdesk`  
**Status:** ⬜

- ⬜ `get_borrow_rate(ticker)` returns borrow rate float
- ⬜ `get_available_shares(ticker)` returns int
- ⬜ `save_borrow_rate(ticker)` writes to `stock_snapshots`
- ⬜ Handles ticker not found on site
- ⬜ Handles site down gracefully (returns last known)
- ⬜ HTML selector verified against live page
- ⬜ Writes to `collection_log`
- ⬜ `tests/test_iborrowdesk.py` passes
- ⬜ PR merged to main

#### TASK-103 — Options Collector
**Owner:** Partner B  
**Branch:** `scraper/options`  
**Status:** ⬜

- ⬜ `collect(ticker)` fetches chains for 4 nearest expiries
- ⬜ `_call_put_ratio()` computes correctly from OI
- ⬜ `_max_pain()` iterates over all strikes
- ⬜ `_iv_rank()` returns 0–100 value
- ⬜ Handles tickers with no options (returns options_eligible=False)
- ⬜ `_save()` creates `options_snapshots` table and inserts
- ⬜ Writes to `collection_log`
- ⬜ `tests/test_options.py` passes all 4 assertions
- ⬜ PR merged to main

#### TASK-104 — News Collector
**Owner:** Partner B  
**Branch:** `scraper/news`  
**Status:** ⬜

- ⬜ `_fetch_yfinance(ticker)` returns article list
- ⬜ `_fetch_google_rss(ticker)` returns article list from RSS
- ⬜ Deduplication by URL hash works correctly
- ⬜ `_detect_catalyst(headline)` returns correct flags for known keywords
- ⬜ Articles saved to MongoDB `news_articles` collection
- ⬜ `has_catalyst` field computed correctly
- ⬜ Writes to `collection_log`
- ⬜ `tests/test_news.py` passes
- ⬜ PR merged to main

### Week 2 — Remaining Collectors

#### TASK-201 — stockanalysis Collector
**Owner:** Partner A  
**Branch:** `scraper/stockanalysis`  
**Status:** ⬜

- ⬜ `get_short_interest(ticker)` returns short_interest_pct and DTC
- ⬜ `si_stale_days` set to 14 in all outputs
- ⬜ HTML selectors verified against live page
- ⬜ `save_snapshot(ticker)` writes to `stock_snapshots`
- ⬜ Handles ticker not found on site
- ⬜ `tests/test_stockanalysis.py` passes
- ⬜ PR merged to main

#### TASK-202 — FINRA Collector
**Owner:** Partner A  
**Branch:** `scraper/finra`  
**Status:** ⬜

- ⬜ `download_daily_file(date)` fetches pipe-delimited file
- ⬜ `get_short_volume(ticker, date)` parses volume for one ticker
- ⬜ `get_short_volume_history(ticker, days=30)` returns DataFrame
- ⬜ Handles missing files (weekend/holiday dates)
- ⬜ Saves to Cassandra `short_volume_daily` table
- ⬜ `tests/test_finra.py` passes
- ⬜ PR merged to main

#### TASK-203 — SEC EDGAR Collector
**Owner:** Partner A  
**Branch:** `scraper/sec_edgar`  
**Status:** ⬜

- ⬜ `get_recent_13d_filings(ticker)` returns activist filings list
- ⬜ `get_insider_transactions(ticker)` returns Form 4 data
- ⬜ `save_filings(ticker)` writes to `stock_events` as TYPE D events
- ⬜ User-Agent header set correctly (EDGAR requirement)
- ⬜ Rate limit respected (10 req/sec max)
- ⬜ `tests/test_sec_edgar.py` passes
- ⬜ PR merged to main

#### TASK-204 — StockTwits Collector
**Owner:** Partner B  
**Branch:** `scraper/stocktwits`  
**Status:** ⬜

- ⬜ `collect(ticker)` returns bull_ratio, bear_ratio, message_count, watchers
- ⬜ Bull/bear counted correctly from message sentiment field
- ⬜ `get_trending()` returns list of trending ticker symbols
- ⬜ Handles 429 rate limit with 60-second backoff
- ⬜ Messages saved to MongoDB `stocktwits` collection
- ⬜ Summary saved to `stocktwits_snapshots` PostgreSQL table
- ⬜ `tests/test_stocktwits.py` passes
- ⬜ PR merged to main

#### TASK-205 — pytrends Collector
**Owner:** Partner B  
**Branch:** `scraper/pytrends`  
**Status:** ⬜

- ⬜ `collect(ticker)` returns trend_score (0–100)
- ⬜ `trend_spike` computed correctly (recent > 3x weekly avg)
- ⬜ 2-second sleep between requests implemented
- ⬜ 10-second sleep on rate limit error implemented
- ⬜ Saves to `trends_snapshots` PostgreSQL table
- ⬜ `tests/test_pytrends.py` passes (test one ticker only)
- ⬜ PR merged to main

#### TASK-206 — Reddit Collector
**Owner:** Partner B  
**Branch:** `scraper/reddit`  
**Status:** ⬜

- ⬜ Reddit API credentials obtained from reddit.com/prefs/apps
- ⬜ Credentials added to `.env`
- ⬜ PRAW authentication verified (`reddit.auth.scopes()` works)
- ⬜ `collect(ticker, lookback_hours)` searches all 7 subreddits
- ⬜ Duplicate posts handled with `insert_many(ordered=False)`
- ⬜ `get_hot_tickers(limit)` extracts tickers from WSB hot posts with regex
- ⬜ `get_mention_count(ticker, hours)` queries MongoDB correctly
- ⬜ `nlp_processed=False` set on all new posts
- ⬜ `per_subreddit` breakdown in return dict
- ⬜ Saves to MongoDB `reddit_posts` collection
- ⬜ `tests/test_reddit.py` passes all assertions
- ⬜ PR merged to main

#### TASK-207 — Universe Screener
**Owner:** Partner A  
**Branch:** `feature/universe-screener`  
**Status:** ⬜

- ⬜ `run()` returns list of tickers passing all 3 filters
- ⬜ `compare_with_db(candidates)` returns action dict with 4 keys
- ⬜ `promote_candidates(new_tickers)` updates `stocks` table status
- ⬜ COOLING logic implemented (ACTIVE → COOLING after failing screen)
- ⬜ ARCHIVED logic implemented (COOLING > 3 days → ARCHIVED)
- ⬜ `tests/test_screener.py` passes
- ⬜ PR merged to main

### Week 3 — Scheduler and State Management

#### TASK-301 — Celery Scheduler
**Owner:** Partner A  
**Branch:** `feature/celery-scheduler`  
**Status:** ⬜

- ⬜ Celery app configured with Redis broker
- ⬜ All collectors wrapped as Celery tasks
- ⬜ Beat schedule configured with correct intervals for each source
- ⬜ Universe screen task fires at 9:31 AM ET weekdays
- ⬜ Worker starts without errors (`celery worker --loglevel=info`)
- ⬜ Beat starts without errors (`celery beat --loglevel=info`)
- ⬜ `tests/test_celery.py` verifies tasks are registered
- ⬜ PR merged to main

#### TASK-302 — Stock State Manager
**Owner:** Partner A  
**Branch:** `feature/state-manager`  
**Status:** ⬜

- ⬜ `get_stocks_by_status(status)` queries PostgreSQL
- ⬜ `transition_status(ticker, new_status, reason)` updates DB and logs event
- ⬜ `save_score(ticker, score_dict)` writes to TimescaleDB and Redis
- ⬜ `get_cached_score(ticker)` reads from Redis, returns None on miss
- ⬜ `detect_phase_transition(ticker, new_phase)` compares against last stored phase
- ⬜ `should_collect_reddit(ticker)` returns bool based on adaptive interval
- ⬜ Redis TTL correctly set to 300 seconds
- ⬜ `tests/test_state_manager.py` passes all assertions
- ⬜ PR merged to main

#### TASK-303 — Candidate Evaluator
**Owner:** Partner A  
**Branch:** `feature/candidate-evaluator`  
**Status:** ⬜

- ⬜ `check_data_sufficiency(ticker)` validates Gate 1 (2+ days data)
- ⬜ `check_signal_threshold(ticker)` evaluates Gate 2 (2+ signal categories)
- ⬜ `evaluate(ticker)` returns PROMOTE, STAY, or DEMOTE
- ⬜ PROMOTE triggers status transition in state manager
- ⬜ Fast-track override implemented for viral tickers
- ⬜ `tests/test_candidate_evaluator.py` passes
- ⬜ PR merged to main

#### TASK-304 — Data Quality Validator
**Owner:** Partner B  
**Branch:** `feature/data-quality`  
**Status:** ⬜

- ⬜ `validate_snapshot(ticker)` returns quality report dict
- ⬜ `check_staleness(ticker, source)` returns days_stale int
- ⬜ `flag_missing_features(ticker)` returns list of None-valued features
- ⬜ `generate_quality_score(ticker)` returns 0–100 completeness score
- ⬜ Staleness warnings: > 3 days = yellow, > 7 days = red
- ⬜ `tests/test_data_quality.py` passes
- ⬜ PR merged to main

#### TASK-305 — Adaptive Reddit Polling
**Owner:** Partner B  
**Branch:** `feature/adaptive-polling`  
**Status:** ⬜

- ⬜ Phase-to-interval mapping defined (30s to 900s)
- ⬜ Celery task checks `should_collect_reddit()` before dispatching
- ⬜ Phase change updates polling interval within one cycle
- ⬜ `tests/test_adaptive_polling.py` passes
- ⬜ PR merged to main

#### TASK-306 — Week 3 End-to-End Test
**Owner:** Both  
**Status:** ⬜

- ⬜ `tests/test_week3_e2e.py` written
- ⬜ Full pipeline runs automatically for 3 tickers
- ⬜ All databases have data after 1 hour of pipeline running
- ⬜ `collection_log` shows mix of sources and tickers
- ⬜ Both partners' machines pass the test

---

## Phase 2 — Feature Engineering

### Week 4 — Feature Computation

#### TASK-401 — Market Feature Engine
**Owner:** Partner A  
**Branch:** `feature/market-features`  
**Status:** ⬜

- ⬜ `compute_volume_features()` returns 4 volume metrics
- ⬜ `compute_price_features()` returns RSI, ATR, momentum
- ⬜ `compute_si_features()` returns normalized SI metrics
- ⬜ `compute_staleness_features()` returns penalty value
- ⬜ No NaN values in any output dict
- ⬜ pandas-ta installed and working
- ⬜ `tests/test_market_features.py` passes
- ⬜ PR merged to main

#### TASK-402 — Options Feature Engine
**Owner:** Partner A  
**Branch:** `feature/options-features`  
**Status:** ⬜

- ⬜ `compute_iv_rank()` returns 0–100
- ⬜ `compute_gamma_exposure()` computes net gamma
- ⬜ `compute_max_pain()` returns strike price
- ⬜ `detect_unusual_sweep()` returns bool + details
- ⬜ `tests/test_options_features.py` passes
- ⬜ PR merged to main

#### TASK-403 — SI Proxy Estimator
**Owner:** Partner A  
**Branch:** `feature/si-proxy`  
**Status:** ⬜

- ⬜ `estimate(ticker)` returns direction + confidence
- ⬜ Borrow rate delta computed correctly
- ⬜ Short volume trend computed from FINRA data
- ⬜ Price + volume signal computed
- ⬜ Confidence levels: high / medium / low / unknown
- ⬜ `tests/test_si_proxy.py` passes
- ⬜ PR merged to main

#### TASK-404 — Social Feature Engine
**Owner:** Partner B  
**Branch:** `feature/social-features`  
**Status:** ⬜

- ⬜ `compute_mention_velocity(ticker)` returns float (-1 to +inf)
- ⬜ `compute_mention_acceleration(ticker)` returns float
- ⬜ `compute_subreddit_diversity(ticker)` returns int (1–7)
- ⬜ `compute_weighted_sentiment(ticker)` weighted by upvotes
- ⬜ Baseline computed from 7-day rolling average
- ⬜ `tests/test_social_features.py` passes
- ⬜ PR merged to main

#### TASK-405 — Feature Assembler
**Owner:** Partner B  
**Branch:** `feature/assembler`  
**Status:** ⬜

- ⬜ `FEATURE_ORDER` list defined with all 28 features
- ⬜ `assemble(ticker)` returns dict with all 28 keys
- ⬜ `to_vector(feature_dict)` returns numpy array shape (28,)
- ⬜ `normalize(vector, scaler)` returns scaled array
- ⬜ `validate_vector(vector)` returns False if any NaN
- ⬜ Feature order is identical across every call (critical)
- ⬜ `tests/test_feature_assembler.py` passes for 5 different tickers
- ⬜ PR merged to main

---

## Phase 3 — NLP Pipeline

### Week 5 — Sentiment and Narrative

#### TASK-501 — FinBERT Sentiment Analyzer
**Owner:** Partner A  
**Branch:** `feature/finbert-sentiment`  
**Status:** ⬜

- ⬜ transformers and torch installed
- ⬜ ProsusAI/finbert model downloaded
- ⬜ `analyze_batch(posts)` processes in batches of 16
- ⬜ `aggregate_sentiment(posts)` weights by upvote score
- ⬜ `process_unprocessed(ticker)` queries MongoDB for nlp_processed=False
- ⬜ Posts updated in MongoDB with sentiment fields
- ⬜ `nlp_processed=True` set after processing
- ⬜ `tests/test_sentiment.py` passes with known examples
- ⬜ PR merged to main

#### TASK-502 — NLP Celery Worker
**Owner:** Partner A  
**Branch:** `feature/nlp-worker`  
**Status:** ⬜

- ⬜ Dedicated NLP queue configured in Celery
- ⬜ `process_nlp_batch()` task processes 50 posts per run
- ⬜ Task runs every 5 minutes
- ⬜ Backlog processing works (clears unprocessed posts over time)
- ⬜ Failed NLP does not crash worker (try/except per post)
- ⬜ PR merged to main

#### TASK-503 — Keyword Extractor
**Owner:** Partner B  
**Branch:** `feature/keyword-extractor`  
**Status:** ⬜

- ⬜ KeyBERT installed
- ⬜ `extract(posts)` returns top 20 keywords
- ⬜ `detect_squeeze_language(text)` returns bool + matched keywords
- ⬜ `SQUEEZE_KEYWORDS` list has 20+ relevant terms
- ⬜ `keyword_urgency_score` computed correctly (0–1)
- ⬜ `tests/test_keywords.py` passes with known examples
- ⬜ PR merged to main

#### TASK-504 — Narrative Classifier
**Owner:** Partner B  
**Branch:** `feature/narrative-classifier`  
**Status:** ⬜

- ⬜ Rule-based version completed first
- ⬜ All 5 narrative categories covered with keyword patterns
- ⬜ `classify(posts, keywords)` returns dominant_narrative + scores dict
- ⬜ `narrative_shift_detected` flag computed correctly
- ⬜ `narrative_squeeze_prob` between 0 and 1
- ⬜ `tests/test_narrative.py` passes
- ⬜ PR merged to main

#### TASK-505 — NLP Feature Integration
**Owner:** Partner B  
**Branch:** `feature/nlp-integration`  
**Status:** ⬜

- ⬜ `feature_assembler.get_nlp_features(ticker)` queries processed MongoDB posts
- ⬜ Returns all 3 NLP features (sentiment, urgency, squeeze_prob)
- ⬜ Returns zeroed dict if no processed posts exist (not error)
- ⬜ Feature assembler `assemble()` now includes NLP features
- ⬜ Full 28-feature vector test passes
- ⬜ PR merged to main

---

## Phase 4 — ML Scoring Engine

### Week 6 — Models and Scoring

#### TASK-601 — Training Data Pipeline
**Owner:** Partner A  
**Branch:** `feature/training-data`  
**Status:** ⬜

- ⬜ `notebooks/01_build_training_data.ipynb` created
- ⬜ 14+ historical squeeze events defined in HISTORICAL_SQUEEZE_EVENTS
- ⬜ At least 5 failed squeeze events included (for balance)
- ⬜ Price/volume history fetched via yfinance for all events
- ⬜ Phase labels assigned using `label_phase()` function
- ⬜ Kaggle WSB dataset downloaded and loaded into MongoDB
- ⬜ FINRA short volume history fetched for 2021–2022 events
- ⬜ Training parquet file saved to `data/training/v1.parquet`
- ⬜ Dataset has > 2,000 rows minimum
- ⬜ Phase distribution printed and documented

#### TASK-602 — Lifecycle Classifier
**Owner:** Partner A  
**Branch:** `feature/lifecycle-classifier`  
**Status:** ⬜

- ⬜ `notebooks/02_train_lifecycle_classifier.ipynb` created
- ⬜ EDA completed (class distribution, feature correlations)
- ⬜ Time-based train/val/test split implemented
- ⬜ LightGBM trained with class_weight='balanced'
- ⬜ Accuracy > 55% on held-out test set
- ⬜ Squeeze Zone recall > 60%
- ⬜ SHAP feature importance plot saved
- ⬜ Model saved via MLflow
- ⬜ `pipeline/ml/lifecycle_classifier.py` wraps trained model
- ⬜ `predict(feature_vector)` returns phase label + probabilities
- ⬜ PR merged to main

#### TASK-603 — Hype Anomaly Detector
**Owner:** Partner B  
**Branch:** `feature/hype-detector`  
**Status:** ⬜

- ⬜ scikit-learn Isolation Forest configured
- ⬜ `train(ticker_histories)` trains on normal velocity patterns
- ⬜ `score(velocity_features)` returns 0–1 anomaly score
- ⬜ Model saved to `models/hype_detector_v1.pkl`
- ⬜ `tests/test_hype_detector.py` verifies high scores on known spikes
- ⬜ PR merged to main

#### TASK-604 — SHAP Explainer
**Owner:** Partner B  
**Branch:** `feature/shap-explainer`  
**Status:** ⬜

- ⬜ SHAP installed
- ⬜ `SHAPExplainer` initialized with TreeExplainer
- ⬜ `explain(vector, raw_features)` returns shap_values dict
- ⬜ `top_drivers` list has 5 items sorted by |contribution|
- ⬜ `generate_text(top_drivers)` produces readable explanation string
- ⬜ `tests/test_explainer.py` passes
- ⬜ PR merged to main

#### TASK-605 — Score Aggregator + Scoring Engine
**Owner:** Both  
**Branch:** `feature/scoring-engine`  
**Status:** ⬜

- ⬜ `ScoreAggregator.score()` combines lifecycle + hype + bonuses
- ⬜ squeeze_score clamped to 0–100
- ⬜ `scoring_engine.score_ticker(ticker)` orchestrates full pipeline
- ⬜ Score saved to TimescaleDB via state manager
- ⬜ Score cached in Redis via state manager
- ⬜ Phase transition detection triggers WebSocket alert
- ⬜ `tests/test_ml_pipeline.py` passes end-to-end
- ⬜ PR merged to main

---

## Phase 5 — API and Dashboard

### Week 7 — Backend API

#### TASK-701 — FastAPI Setup
**Owner:** Partner A  
**Status:** ⬜

- ⬜ FastAPI app initialized with CORS middleware
- ⬜ `/api/leaderboard` endpoint returns top 20 by score from Redis
- ⬜ `/api/stock/{ticker}` returns full detail
- ⬜ `/api/stock/{ticker}/history` queries TimescaleDB score history
- ⬜ `/api/stock/{ticker}/reddit` queries MongoDB posts
- ⬜ `/api/stock/{ticker}/options` reads options_snapshots
- ⬜ All endpoints return < 200ms for cached data
- ⬜ API runs on port 8000

#### TASK-702 — WebSocket Server
**Owner:** Partner B  
**Status:** ⬜

- ⬜ `/ws` WebSocket endpoint implemented
- ⬜ `broadcast_phase_transition()` sends to all connected clients
- ⬜ Connection and disconnection handled without errors
- ⬜ Message format matches dashboard expectation

### Week 8-9 — React Dashboard

#### TASK-801 — Dashboard Foundation
**Owner:** Partner B  
**Status:** ⬜

- ⬜ Vite + React + TypeScript project created in `/dashboard`
- ⬜ Tailwind CSS configured
- ⬜ shadcn/ui installed
- ⬜ React Query configured for API calls
- ⬜ WebSocket hook implemented
- ⬜ API client (Axios) configured pointing to localhost:8000

#### TASK-802 — Leaderboard Component
**Owner:** Partner B  
**Status:** ⬜

- ⬜ Ticker table with all columns (phase pill, score bar, SI, velocity, float, risk)
- ⬜ Phase pills with correct colors per phase
- ⬜ Score bar with fill width proportional to score
- ⬜ Auto-refreshes every 30 seconds via React Query

#### TASK-803 — Stock Detail Page
**Owner:** Partner A  
**Status:** ⬜

- ⬜ Lifecycle stage progression component
- ⬜ Signal breakdown with colored dots
- ⬜ SHAP explanation text display
- ⬜ Score history chart (Recharts LineChart)
- ⬜ Reddit feed with sentiment badges
- ⬜ Options panel with call/put ratio
- ⬜ Catalyst/news feed

#### TASK-804 — Real-Time Alerts
**Owner:** Partner B  
**Status:** ⬜

- ⬜ WebSocket connection maintained in background
- ⬜ Phase transition toast notifications appear within 2 seconds of event
- ⬜ Connection auto-reconnects on drop
- ⬜ Alert includes ticker, old phase, new phase, timestamp

---

## Phase 6 — Big Data Layer

### Week 10-11 — Kafka and Spark

#### TASK-901 — Kafka Integration
**Owner:** Both  
**Status:** ⬜

- ⬜ Kafka added to docker-compose.yml
- ⬜ All 6 topics created with correct partitions and retention
- ⬜ Collectors updated to publish to Kafka instead of writing directly to DB
- ⬜ Kafka consumers write to databases
- ⬜ Feature engine consumes from Kafka topics
- ⬜ ML engine consumes `pipeline.features.ready`

#### TASK-902 — Apache Spark Batch Processing
**Owner:** Partner A  
**Status:** ⬜

- ⬜ Spark installed and configured
- ⬜ Historical Reddit data loaded from Parquet via Spark
- ⬜ Mention velocity features computed at scale with window functions
- ⬜ Training dataset v2 generated by Spark job
- ⬜ Model retrained on larger dataset

#### TASK-903 — Parquet Cold Storage
**Owner:** Partner B  
**Status:** ⬜

- ⬜ MinIO added to docker-compose.yml
- ⬜ Archival job moves MongoDB data > 90 days to Parquet
- ⬜ Archival job moves Cassandra data > 90 days to Parquet
- ⬜ Parquet files readable by Spark

---

## Phase 7 — Backtesting and Validation

### Week 12 — Backtesting

#### TASK-1001 — Historical Backtest
**Owner:** Both  
**Status:** ⬜

- ⬜ `notebooks/03_backtest.ipynb` created
- ⬜ Historical data replayed through feature engine for GME, AMC, BBBY
- ⬜ Phase transitions correctly detected in historical data
- ⬜ Precision/recall computed for Squeeze Zone detection
- ⬜ Squeeze Zone precision > 50% documented
- ⬜ Results compared against baseline (high SI threshold only)

#### TASK-1002 — Model Retraining Pipeline
**Owner:** Partner A  
**Status:** ⬜

- ⬜ Weekly retraining Celery task created
- ⬜ MLflow experiment tracking configured
- ⬜ New model version promoted only if metrics improve
- ⬜ Old model kept as fallback

---

## Ongoing / Maintenance Tasks

#### TASK-ONGOING-001 — Daily Standup Checklist
**Owner:** Both  
**Frequency:** Daily

- ⬜ `docker-compose up -d` run
- ⬜ `python tests/test_connections.py` passes 5/5
- ⬜ `collection_log` checked for overnight failures
- ⬜ `git pull origin main` run before starting

#### TASK-ONGOING-002 — Weekly Integration Test
**Owner:** Both  
**Frequency:** Every Friday

- ⬜ Current week's integration test passes on both machines
- ⬜ Git tag created: `git tag week-N-complete`
- ⬜ Schedule for next week reviewed and adjusted
- ⬜ Blockers documented in GitHub Issues

---

## Task Summary

| Phase | Total Tasks | Done | In Progress | Not Started |
|---|---|---|---|---|
| Phase 0 — Foundation | 3 | 0 | 0 | 3 |
| Phase 1 — Data Collection | 14 | 0 | 0 | 14 |
| Phase 2 — Feature Engineering | 5 | 0 | 0 | 5 |
| Phase 3 — NLP Pipeline | 5 | 0 | 0 | 5 |
| Phase 4 — ML Scoring | 5 | 0 | 0 | 5 |
| Phase 5 — API + Dashboard | 6 | 0 | 0 | 6 |
| Phase 6 — Big Data Layer | 3 | 0 | 0 | 3 |
| Phase 7 — Backtesting | 2 | 0 | 0 | 2 |
| **Total** | **43** | **0** | **0** | **43** |
