# pipeline/state/stock_state_manager.py
#
# Stock State Manager — the central nervous system of the pipeline.
#
# Every other component reads/writes stock state through this class.
# Nothing writes to the stocks table directly except this manager.
#
# Responsibilities:
#   1. Status transitions (CANDIDATE → ACTIVE → COOLING → ARCHIVED)
#   2. Score caching in Redis (live scores for dashboard)
#   3. Phase transition detection (triggers WebSocket alerts)
#   4. Adaptive polling intervals (phase determines collection rate)
#   5. Feature vector caching (avoids recomputing every request)
#   6. Collection timing (should_collect_X() methods)
#
# Storage:
#   PostgreSQL — stock status, events, snapshots (source of truth)
#   Redis      — live scores, leaderboard, polling timestamps (cache)
#   TimescaleDB — score history (for dashboard charts)

import os
import json
import time
import redis
import psycopg2
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
# ADAPTIVE POLLING INTERVALS (seconds)
# Phase determines how often we collect Reddit and options data
# ─────────────────────────────────────────────────────────────
REDDIT_POLL_INTERVALS = {
    'Early Setup':  600,   # 10 min — quiet, check occasionally
    'Heating Up':   300,   # 5 min  — building, check more often
    'Squeeze Zone': 60,    # 1 min  — active squeeze, near real-time
    'Danger Zone':  30,    # 30 sec — peak mania, maximum frequency
    'Cooldown':     900,   # 15 min — winding down, minimal polling
    'CANDIDATE':    1800,  # 30 min — just watching
    'default':      300,   # 5 min  — safe default
}

OPTIONS_POLL_INTERVALS = {
    'Early Setup':  1800,  # 30 min
    'Heating Up':   900,   # 15 min
    'Squeeze Zone': 300,   # 5 min
    'Danger Zone':  120,   # 2 min
    'Cooldown':     3600,  # 1 hour
    'CANDIDATE':    3600,  # 1 hour
    'default':      900,   # 15 min
}

# Redis cache TTLs (seconds)
SCORE_CACHE_TTL    = 300   # 5 min — live score
PHASE_CACHE_TTL    = 300   # 5 min — current phase
FEATURE_CACHE_TTL  = 120   # 2 min — feature vector
LEADERBOARD_TTL    = 60    # 1 min — leaderboard sorted set


class StockStateManager:
    """
    Central manager for all stock state operations.
    All pipeline components interact with stock state through here.

    Usage:
        mgr = StockStateManager()

        # Transition a stock
        mgr.transition_status('GME', 'ACTIVE', 'gate2_passed')

        # Save a score
        mgr.save_score('GME', score_dict)

        # Check if we should collect Reddit right now
        if mgr.should_collect_reddit('GME'):
            reddit_collector.collect('GME')

        # Detect phase change and alert dashboard
        if mgr.detect_phase_transition('GME', new_phase):
            # WebSocket alert sent automatically
    """

    def __init__(self):
        # PostgreSQL — source of truth
        self.pg = psycopg2.connect(os.getenv('POSTGRES_URL'))

        # Redis — cache layer
        self.redis = redis.Redis.from_url(
            os.getenv('REDIS_URL', 'redis://localhost:6379'),
            decode_responses=True,
        )

        # Verify connections
        self._verify_connections()

    # ═════════════════════════════════════════════════════════
    # STATUS TRANSITIONS
    # ═════════════════════════════════════════════════════════

    def transition_status(self, ticker: str,
                           new_status: str,
                           reason: str) -> bool:
        """
        Transitions a stock to a new status.
        Logs the transition as a TYPE D event.
        Updates Redis cache immediately.

        Valid transitions:
          UNTRACKED  → CANDIDATE  (screener promotes)
          CANDIDATE  → ACTIVE     (evaluator promotes after Gate 1+2)
          ACTIVE     → COOLING    (screener demotes, failed screen)
          COOLING    → ACTIVE     (signal re-triggers)
          COOLING    → ARCHIVED   (grace period expired)
          ANY        → ACTIVE     (fast track override)

        Returns True if transition succeeded, False if no-op.
        """
        ticker     = ticker.upper().strip()
        old_status = self.get_status(ticker)

        if old_status == new_status:
            return False  # No-op — already in target state

        cur = self.pg.cursor()

        # Update status in DB
        update_fields = {
            'status':    new_status,
            'updated_at': 'NOW()',
        }
        if new_status == 'CANDIDATE':
            update_fields['promoted_at']   = 'NOW()'
        if new_status == 'ACTIVE':
            update_fields['activated_at']  = 'NOW()'
        if new_status == 'COOLING':
            update_fields['cooled_at']     = 'NOW()'

        cur.execute("""
            UPDATE stocks
            SET    status      = %s,
                   updated_at  = NOW()
            WHERE  ticker = %s
        """, (new_status, ticker))

        # Set timestamp fields conditionally
        if new_status == 'CANDIDATE':
            cur.execute("""
                UPDATE stocks SET promoted_at = NOW()
                WHERE ticker = %s
            """, (ticker,))
        elif new_status == 'ACTIVE':
            cur.execute("""
                UPDATE stocks SET activated_at = NOW()
                WHERE ticker = %s
            """, (ticker,))
        elif new_status == 'COOLING':
            cur.execute("""
                UPDATE stocks SET cooled_at = NOW()
                WHERE ticker = %s
            """, (ticker,))

        # Log as TYPE D event
        cur.execute("""
            INSERT INTO stock_events
                (ticker, event_type, event_data, occurred_at)
            VALUES (%s, %s, %s, NOW())
        """, (
            ticker,
            'status_change',
            json.dumps({
                'from':      old_status,
                'to':        new_status,
                'reason':    reason,
                'timestamp': datetime.utcnow().isoformat(),
            }),
        ))

        self.pg.commit()

        # Update Redis cache
        self.redis.setex(
            f'status:{ticker}',
            SCORE_CACHE_TTL,
            new_status,
        )

        # Update active watchlist in Redis
        if new_status == 'ACTIVE':
            self.redis.sadd('watchlist:active', ticker)
        else:
            self.redis.srem('watchlist:active', ticker)

        print(f'  [{ticker}] status: {old_status} → {new_status} '
              f'(reason: {reason})')
        return True

    def get_status(self, ticker: str) -> Optional[str]:
        """
        Returns current status of a stock.
        Checks Redis cache first, falls back to PostgreSQL.
        """
        # Try cache first
        cached = self.redis.get(f'status:{ticker}')
        if cached:
            return cached

        # Fall back to DB
        cur = self.pg.cursor()
        cur.execute("""
            SELECT status FROM stocks WHERE ticker = %s
        """, (ticker,))
        row = cur.fetchone()
        if row:
            # Cache for next time
            self.redis.setex(
                f'status:{ticker}', SCORE_CACHE_TTL, row[0])
            return row[0]
        return None

    def get_stocks_by_status(self, status: str) -> list:
        """
        Returns all ticker strings with a given status.
        Used by Celery tasks to get work queues.
        """
        # Active list has a Redis cache for speed
        if status == 'ACTIVE':
            cached = self.redis.smembers('watchlist:active')
            if cached:
                return list(cached)

        cur = self.pg.cursor()
        cur.execute("""
            SELECT ticker FROM stocks
            WHERE  status = %s
            ORDER  BY updated_at DESC
        """, (status,))
        tickers = [r[0] for r in cur.fetchall()]

        if status == 'ACTIVE':
            # Rebuild Redis set
            if tickers:
                self.redis.delete('watchlist:active')
                self.redis.sadd('watchlist:active', *tickers)

        return tickers

    def get_all_statuses(self) -> dict:
        """Returns {ticker: status} for every stock in DB."""
        cur = self.pg.cursor()
        cur.execute("SELECT ticker, status FROM stocks")
        return {r[0]: r[1] for r in cur.fetchall()}

    # ═════════════════════════════════════════════════════════
    # SCORE MANAGEMENT
    # ═════════════════════════════════════════════════════════

    def save_score(self, ticker: str, score: dict) -> bool:
        """
        Saves an ML score for a ticker.

        Writes to:
          1. Redis — for real-time dashboard reads (< 1ms)
          2. TimescaleDB — for score history charts
          3. Redis leaderboard sorted set — for ranking

        score dict must contain at minimum:
          squeeze_score  — float 0-100
          phase_label    — string
          model_version  — string
        """
        ticker = ticker.upper().strip()

        if 'squeeze_score' not in score:
            print(f'  [state] save_score: missing squeeze_score '
                  f'for {ticker}')
            return False

        score['ticker']   = ticker
        score['saved_at'] = datetime.utcnow().isoformat()

        # 1. Cache in Redis (dashboard reads this)
        self.redis.setex(
            f'score:{ticker}',
            SCORE_CACHE_TTL,
            json.dumps(score, default=str),
        )

        # 2. Cache phase separately for fast polling checks
        if 'phase_label' in score:
            self.redis.setex(
                f'phase:{ticker}',
                PHASE_CACHE_TTL,
                score['phase_label'],
            )

        # 3. Update leaderboard sorted set
        self.redis.zadd(
            'leaderboard:squeeze_score',
            {ticker: float(score['squeeze_score'])},
        )

        # 4. Write to TimescaleDB stock_scores
        self._save_score_timescale(ticker, score)

        return True

    def get_cached_score(self, ticker: str) -> Optional[dict]:
        """
        Returns latest score from Redis cache.
        Returns None if cache miss (score not yet computed).
        """
        cached = self.redis.get(f'score:{ticker}')
        if cached:
            return json.loads(cached)
        return None

    def get_current_phase(self, ticker: str) -> str:
        """
        Returns current lifecycle phase.
        Tries Redis phase cache first.
        Falls back to score cache, then DB.
        """
        # Try phase cache
        phase = self.redis.get(f'phase:{ticker}')
        if phase:
            return phase

        # Try score cache
        score = self.get_cached_score(ticker)
        if score and 'phase_label' in score:
            return score['phase_label']

        # Try DB (last stored score)
        last = self.get_last_score(ticker)
        if last:
            return last.get('phase_label', 'Early Setup')

        return 'Early Setup'

    def get_last_score(self, ticker: str) -> Optional[dict]:
        """Returns most recent stored score from TimescaleDB."""
        cur = self.pg.cursor()
        cur.execute("""
            SELECT squeeze_score, phase_label,
                   phase_probabilities, hype_acceleration,
                   shap_top_drivers, explanation_text,
                   model_version, time
            FROM   stock_scores
            WHERE  ticker = %s
            ORDER  BY time DESC
            LIMIT  1
        """, (ticker,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            'squeeze_score':       row[0],
            'phase_label':         row[1],
            'phase_probabilities': row[2],
            'hype_acceleration':   row[3],
            'shap_top_drivers':    row[4],
            'explanation_text':    row[5],
            'model_version':       row[6],
            'scored_at':           row[7],
        }

    def get_score_history(self, ticker: str,
                           hours: int = 168) -> list:
        """
        Returns score history for a ticker.
        Used by the dashboard score chart.
        Default: last 7 days (168 hours).
        """
        cur = self.pg.cursor()
        cur.execute("""
            SELECT time, squeeze_score, phase_label,
                   hype_acceleration, mention_velocity,
                   sentiment_score, volume_ratio
            FROM   stock_scores
            WHERE  ticker = %s
              AND  time   > NOW() - INTERVAL '%s hours'
            ORDER  BY time ASC
        """, (ticker, hours))
        return [
            {
                'time':              r[0].isoformat(),
                'squeeze_score':     r[1],
                'phase_label':       r[2],
                'hype_acceleration': r[3],
                'mention_velocity':  r[4],
                'sentiment_score':   r[5],
                'volume_ratio':      r[6],
            }
            for r in cur.fetchall()
        ]

    def get_leaderboard(self, limit: int = 20) -> list:
        """
        Returns top N stocks by squeeze score.
        Reads from Redis sorted set — O(log N + K).
        """
        # Get top tickers with scores from Redis
        pairs = self.redis.zrevrangebyscore(
            'leaderboard:squeeze_score',
            '+inf', '-inf',
            start=0, num=limit,
            withscores=True,
        )

        result = []
        for ticker, score in pairs:
            cached = self.get_cached_score(ticker)
            if cached:
                result.append(cached)
            else:
                result.append({
                    'ticker':        ticker,
                    'squeeze_score': score,
                    'phase_label':   self.get_current_phase(ticker),
                })

        return result

    # ═════════════════════════════════════════════════════════
    # PHASE TRANSITION DETECTION
    # ═════════════════════════════════════════════════════════

    def detect_phase_transition(self, ticker: str,
                                  new_phase: str) -> bool:
        """
        Detects if a stock has changed lifecycle phase.
        Called by the ML scoring engine after each score.

        If transition detected:
          - Inserts TYPE D event in stock_events
          - Logs transition for audit
          - Returns True (caller handles WebSocket alert)

        Returns False if no phase change.
        """
        old_phase = self.get_current_phase(ticker)

        if old_phase == new_phase:
            print("harshiiii")
            return False

        self.redis.set(f'phase:{ticker}', new_phase)

        # Log the transition as TYPE D event
        cur = self.pg.cursor()
        cur.execute("""
            INSERT INTO stock_events
                (ticker, event_type, event_data, occurred_at)
            VALUES (%s, %s, %s, NOW())
        """, (
            ticker,
            'phase_transition',
            json.dumps({
                'from_phase':  old_phase,
                'to_phase':    new_phase,
                'timestamp':   datetime.utcnow().isoformat(),
            }),
        ))
        self.pg.commit()

        print(
            f'  🔄 [{ticker}] phase: {old_phase} → {new_phase}'
        )

        return True

    def get_recent_phase_transitions(self,
                                      hours: int = 24) -> list:
        """
        Returns all phase transitions in the last N hours.
        Used by dashboard alert feed.
        """
        cur = self.pg.cursor()
        cur.execute("""
            SELECT ticker, event_data, occurred_at
            FROM   stock_events
            WHERE  event_type  = 'phase_transition'
              AND  occurred_at > NOW() - INTERVAL '%s hours'
            ORDER  BY occurred_at DESC
        """, (hours,))
        return [
            {
                'ticker':      r[0],
                'event_data':  r[1],
                'occurred_at': r[2].isoformat(),
            }
            for r in cur.fetchall()
        ]

    # ═════════════════════════════════════════════════════════
    # ADAPTIVE POLLING
    # ═════════════════════════════════════════════════════════

    def should_collect_reddit(self, ticker: str) -> bool:
        """
        Returns True if enough time has passed since last
        Reddit collection for this ticker.
        Interval is determined by current lifecycle phase.

        Used by Celery task to decide whether to dispatch
        a Reddit collection job for this ticker.
        """
        phase    = self.get_current_phase(ticker)
        interval = REDDIT_POLL_INTERVALS.get(
            phase,
            REDDIT_POLL_INTERVALS['default']
        )
        return self._should_collect(ticker, 'reddit', interval)

    def should_collect_options(self, ticker: str) -> bool:
        """Same as above but for options data."""
        phase    = self.get_current_phase(ticker)
        interval = OPTIONS_POLL_INTERVALS.get(
            phase,
            OPTIONS_POLL_INTERVALS['default']
        )
        return self._should_collect(ticker, 'options', interval)

    def should_collect_news(self, ticker: str) -> bool:
        """News updates every 15 min regardless of phase."""
        return self._should_collect(ticker, 'news', 900)

    def should_collect_borrow_rate(self, ticker: str) -> bool:
        """Borrow rate updates 3x/day."""
        return self._should_collect(ticker, 'borrow_rate', 14400)

    def mark_collected(self, ticker: str, source: str):
        """
        Records that a collection just ran for this ticker+source.
        Stores timestamp in Redis — used by should_collect_X().
        """
        key = f'last_collected:{source}:{ticker}'
        self.redis.setex(key, 86400, str(time.time()))
        # TTL = 24h — if nothing collected in 24h, allow collection

    def get_collection_interval(self, ticker: str,
                                 source: str) -> int:
        """Returns current poll interval in seconds for a source."""
        phase = self.get_current_phase(ticker)
        if source == 'reddit':
            return REDDIT_POLL_INTERVALS.get(
                phase, REDDIT_POLL_INTERVALS['default'])
        if source == 'options':
            return OPTIONS_POLL_INTERVALS.get(
                phase, OPTIONS_POLL_INTERVALS['default'])
        return 900

    def _should_collect(self, ticker: str,
                         source: str, interval: int) -> bool:
        """
        Core polling check.
        Returns True if last collection was > interval seconds ago.
        """
        key     = f'last_collected:{source}:{ticker}'
        last_ts = self.redis.get(key)

        if last_ts is None:
            return True  # Never collected — collect now

        elapsed = time.time() - float(last_ts)
        return elapsed >= interval

    # ═════════════════════════════════════════════════════════
    # FEATURE VECTOR CACHE
    # ═════════════════════════════════════════════════════════

    def cache_feature_vector(self, ticker: str,
                              features: dict):
        """
        Caches the latest feature vector for a ticker.
        Avoids recomputing features on every ML scoring request.
        """
        self.redis.setex(
            f'features:{ticker}',
            FEATURE_CACHE_TTL,
            json.dumps(features, default=str),
        )

    def get_cached_features(self, ticker: str) -> Optional[dict]:
        """Returns cached feature vector or None."""
        cached = self.redis.get(f'features:{ticker}')
        if cached:
            return json.loads(cached)
        return None

    def invalidate_feature_cache(self, ticker: str):
        """Clears feature cache — called when new data arrives."""
        self.redis.delete(f'features:{ticker}')

    # ═════════════════════════════════════════════════════════
    # DATA QUALITY HELPERS
    # ═════════════════════════════════════════════════════════

    def get_last_collection_time(self, ticker: str,
                                  source: str) -> Optional[datetime]:
        """
        Returns when a source was last successfully collected
        for a given ticker.
        Reads from PostgreSQL collection_log.
        """
        cur = self.pg.cursor()
        cur.execute("""
            SELECT MAX(collected_at)
            FROM   collection_log
            WHERE  ticker = %s
              AND  source = %s
              AND  status = 'success'
        """, (ticker, source))
        row = cur.fetchone()
        return row[0] if row and row[0] else None

    def get_data_freshness(self, ticker: str) -> dict:
        """
        Returns staleness of each data source for a ticker.
        Used by data quality validator and dashboard display.
        """
        sources = [
            'yfinance_info', 'yfinance_ohlcv_1d',
            'iborrowdesk', 'finra', 'reddit',
            'stocktwits', 'options_yfinance',
            'news', 'pytrends', 'sec_13d_13g',
        ]
        now       = datetime.utcnow()
        freshness = {}

        for source in sources:
            last = self.get_last_collection_time(ticker, source)
            if last:
                age_hours = (now - last).total_seconds() / 3600
                freshness[source] = {
                    'last_collected': last.isoformat(),
                    'age_hours':      round(age_hours, 2),
                    'stale':          age_hours > 24,
                    'very_stale':     age_hours > 168,
                }
            else:
                freshness[source] = {
                    'last_collected': None,
                    'age_hours':      None,
                    'stale':          True,
                    'very_stale':     True,
                }

        return freshness

    # ═════════════════════════════════════════════════════════
    # PIPELINE HEALTH
    # ═════════════════════════════════════════════════════════

    def get_pipeline_health(self) -> dict:
        """
        Returns overall pipeline health metrics.
        Used by monitoring dashboard and Grafana.
        """
        cur = self.pg.cursor()

        # Count by status
        cur.execute("""
            SELECT status, COUNT(*)
            FROM   stocks
            GROUP  BY status
        """)
        status_counts = {r[0]: r[1] for r in cur.fetchall()}

        # Recent collection failures
        cur.execute("""
            SELECT source, COUNT(*)
            FROM   collection_log
            WHERE  status      = 'failed'
              AND  collected_at > NOW() - INTERVAL '1 hour'
            GROUP  BY source
            ORDER  BY COUNT(*) DESC
        """)
        failures = {r[0]: r[1] for r in cur.fetchall()}

        # Leaderboard size
        lb_size = self.redis.zcard('leaderboard:squeeze_score')

        # Scores in last hour
        cur.execute("""
            SELECT COUNT(*)
            FROM   stock_scores
            WHERE  time > NOW() - INTERVAL '1 hour'
        """)
        recent_scores = cur.fetchone()[0]

        return {
            'status_counts':     status_counts,
            'collection_failures_1h': failures,
            'leaderboard_size':  lb_size,
            'scores_last_hour':  recent_scores,
            'redis_ping':        self.redis.ping(),
            'checked_at':        datetime.utcnow().isoformat(),
        }

    # ═════════════════════════════════════════════════════════
    # INTERNAL HELPERS
    # ═════════════════════════════════════════════════════════

    def _save_score_timescale(self, ticker: str, score: dict):
        """Writes score to TimescaleDB stock_scores hypertable."""
        cur = self.pg.cursor()
        try:
            cur.execute("""
                INSERT INTO stock_scores (
                    time, ticker,
                    squeeze_score, phase_label,
                    phase_probabilities, hype_acceleration,
                    mention_velocity, sentiment_score,
                    volume_ratio, borrow_rate,
                    call_put_ratio, shap_top_drivers,
                    explanation_text, model_version
                ) VALUES (
                    NOW(), %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                ticker,
                score.get('squeeze_score'),
                score.get('phase_label'),
                json.dumps(score.get('phase_probabilities', {})),
                score.get('hype_acceleration'),
                score.get('mention_velocity'),
                score.get('sentiment_score'),
                score.get('volume_ratio'),
                score.get('borrow_rate'),
                score.get('call_put_ratio'),
                json.dumps(score.get('shap_top_drivers', [])),
                score.get('explanation_text'),
                score.get('model_version', 'v1.0'),
            ))
            self.pg.commit()
        except Exception as e:
            print(f'  [state] TimescaleDB score write error: {e}')
            self.pg.rollback()

    def _verify_connections(self):
        """Verifies both DB connections are alive."""
        try:
            cur = self.pg.cursor()
            cur.execute('SELECT 1')
        except Exception as e:
            raise ConnectionError(
                f'PostgreSQL connection failed: {e}')

        try:
            self.redis.ping()
        except Exception as e:
            raise ConnectionError(
                f'Redis connection failed: {e}')

    def close(self):
        self.pg.close()


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    mgr = StockStateManager()

    print('='*55)
    print('  Stock State Manager — Manual Test')
    print('='*55)

    # Test 1: Status transition
    print('\n[1] Status transitions')

    # First ensure GME exists in DB
    cur = mgr.pg.cursor()
    cur.execute("""
        INSERT INTO stocks (ticker, status)
        VALUES ('GME', 'CANDIDATE')
        ON CONFLICT (ticker) DO NOTHING
    """)
    mgr.pg.commit()

    mgr.transition_status('GME', 'ACTIVE', 'test_gate2_passed')
    print(f'  GME status: {mgr.get_status("GME")}')

    # Test 2: Save and retrieve score
    print('\n[2] Score caching')
    test_score = {
        'squeeze_score':    87.3,
        'phase_label':      'Squeeze Zone',
        'phase_probabilities': {
            'Early Setup': 0.03,
            'Heating Up':  0.11,
            'Squeeze Zone': 0.73,
            'Danger Zone':  0.11,
            'Cooldown':    0.02,
        },
        'hype_acceleration':  0.84,
        'explanation_text':   'Borrow rate spiked to 47%...',
        'model_version':      'v1.0',
    }
    mgr.save_score('GME', test_score)
    cached = mgr.get_cached_score('GME')
    print(f'  Cached score: {cached["squeeze_score"]}')
    print(f'  Phase: {cached["phase_label"]}')

    # Test 3: Phase transition detection
    print('\n[3] Phase transition detection')
    changed = mgr.detect_phase_transition('GME', 'Danger Zone')
    print(f'  Phase changed: {changed}')
    no_change = mgr.detect_phase_transition('GME', 'Danger Zone')
    print(f"no change value {no_change}")
    print(f'  No-op (same phase): {not no_change}')

    # Test 4: Adaptive polling
    print('\n[4] Adaptive polling intervals')
    print(f'  Reddit interval (Squeeze Zone): '
          f'{mgr.get_collection_interval("GME", "reddit")}s')
    print(f'  Options interval (Squeeze Zone): '
          f'{mgr.get_collection_interval("GME", "options")}s')
    print(f'  Should collect Reddit: '
          f'{mgr.should_collect_reddit("GME")}')

    # Test 5: Leaderboard
    print('\n[5] Leaderboard')
    lb = mgr.get_leaderboard(limit=5)
    print(f'  Top {len(lb)} stocks:')
    for s in lb:
        print(f'    {s["ticker"]}: {s["squeeze_score"]} '
              f'({s["phase_label"]})')

    # Test 6: Pipeline health
    print('\n[6] Pipeline health')
    health = mgr.get_pipeline_health()
    print(f'  Status counts: {health["status_counts"]}')
    print(f'  Redis ping: {health["redis_ping"]}')
    print(f'  Leaderboard size: {health["leaderboard_size"]}')

    mgr.close()
    print('\n✓ State manager test complete')