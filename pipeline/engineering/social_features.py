# pipeline/engineering/social_features.py
#
# Social Feature Engine
#
# Reads social data from databases and computes all
# social-side features for the ML feature vector.
#
# Data sources (reads from):
#   PostgreSQL  stocktwits_snapshots — bull/bear ratio, message count
#   PostgreSQL  trends_snapshots     — Google Trends score, spike flag
#   PostgreSQL  mention_timeseries   — Reddit mention counts (when available)
#   PostgreSQL  stock_events         — catalyst flags, activist filings
#
# Features produced (6 social features for ML vector, indices 19-24):
#   stocktwits_bull_ratio        — 0-1 (bull sentiment on StockTwits)
#   stocktwits_message_count     — normalized message volume
#   google_trend_score           — normalized 0-100 search interest
#   trend_spike_flag             — binary: sudden search spike
#   news_catalyst_flag           — binary: catalyst detected in last 48h
#   has_activist_filing          — binary: SEC 13D/13G in last 30 days
#
# Additional computed features (not in the 28-vector but used
# by candidate_evaluator Gate 2 and dashboard):
#   mention_velocity_1h          — rate of change vs baseline (Reddit)
#   mention_velocity_6h          — 6-hour velocity
#   mention_acceleration         — is velocity itself accelerating?
#   subreddit_diversity          — distinct subs mentioning ticker
#   weighted_sentiment           — upvote-weighted sentiment score
#   stocktwits_bear_ratio        — bear side of StockTwits
#   stocktwits_watchers          — watchlist count (popularity proxy)
#   trend_avg_7d                 — 7-day trend average
#   trend_peak_7d                — 7-day peak search interest

import os
import time
import psycopg2
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


class SocialFeatureEngine:
    """
    Computes all social-side features for a given ticker.

    Reads from PostgreSQL (StockTwits, Trends, events).
    When Reddit data is available, reads from mention_timeseries
    to compute velocity and acceleration.

    Usage:
        engine = SocialFeatureEngine()
        features = engine.compute('GME')
        # features is a dict with all social features
    """

    def __init__(self):
        self.pg = psycopg2.connect(os.getenv('POSTGRES_URL'))

    # ═════════════════════════════════════════════════════════
    # MAIN METHOD
    # ═════════════════════════════════════════════════════════

    def compute(self, ticker: str) -> dict:
        """
        Computes all social features for one ticker.
        Returns dict with all features. Missing → 0.0/False.
        Never raises — returns partial dict on errors.

        Returns dict with all social features plus metadata.
        """
        ticker = ticker.upper().strip()
        start  = time.time()

        result = {
            'ticker':      ticker,
            'computed_at': datetime.utcnow(),
        }

        # ── 1. StockTwits features ────────────────────────
        result.update(self._get_stocktwits_features(ticker))

        # ── 2. Google Trends features ─────────────────────
        result.update(self._get_trends_features(ticker))

        # ── 3. News catalyst flag ─────────────────────────
        result.update(self._get_catalyst_features(ticker))

        # ── 4. SEC activist filing flag ───────────────────
        result.update(self._get_activist_features(ticker))

        # ── 5. Reddit mention features ────────────────────
        # Returns zeros if Reddit not yet available
        result.update(self._get_mention_features(ticker))

        # ── 6. Composite social score ─────────────────────
        result['social_score'] = self._compute_social_score(result)

        result['compute_duration_ms'] = int(
            (time.time()-start)*1000)

        return result

    # ═════════════════════════════════════════════════════════
    # 1. STOCKTWITS FEATURES
    # ═════════════════════════════════════════════════════════

    def _get_stocktwits_features(self, ticker: str) -> dict:
        """
        Reads StockTwits bull/bear sentiment from stocktwits_snapshots.
        Source: stocktwits_collector.py
        Staleness: real-time (updated every 30 min)

        Features:
          stocktwits_bull_ratio   — 0.0-1.0 (fraction bullish)
          stocktwits_bear_ratio   — 0.0-1.0 (fraction bearish)
          stocktwits_message_count— raw message volume
          stocktwits_watchers     — watchlist_count (popularity)
          stocktwits_bull_bear_diff — bull_ratio - bear_ratio [-1, 1]
          stocktwits_data_age_hours — how old is this reading
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT bull_ratio,
                       bear_ratio,
                       message_count,
                       watchers,
                       collected_at
                FROM   stocktwits_snapshots
                WHERE  ticker = %s
                ORDER  BY collected_at DESC
                LIMIT  1
            """, (ticker,))
            row = cur.fetchone()

            if not row:
                return {
                    'stocktwits_bull_ratio':    0.0,
                    'stocktwits_bear_ratio':    0.0,
                    'stocktwits_message_count': 0,
                    'stocktwits_watchers':      0,
                    'stocktwits_bull_bear_diff': 0.0,
                    'stocktwits_data_available': False,
                    'stocktwits_data_age_hours': None,
                }

            bull_ratio  = float(row[0]) if row[0] is not None else 0.0
            bear_ratio  = float(row[1]) if row[1] is not None else 0.0
            msg_count   = int(row[2])   if row[2] is not None else 0
            watchers    = int(row[3])   if row[3] is not None else 0
            collected   = row[4]

            age_hours = None
            if collected:
                age_hours = round(
                    (datetime.utcnow() - collected.replace(
                        tzinfo=None)).total_seconds() / 3600, 2)

            # Historical comparison — how does current bull_ratio
            # compare to 7-day average?
            bull_7d_avg = self._get_stocktwits_avg(ticker, days=7)
            bull_delta  = round(bull_ratio - bull_7d_avg, 4) \
                          if bull_7d_avg > 0 else 0.0

            return {
                'stocktwits_bull_ratio':    round(bull_ratio, 4),
                'stocktwits_bear_ratio':    round(bear_ratio, 4),
                'stocktwits_message_count': msg_count,
                'stocktwits_watchers':      watchers,
                'stocktwits_bull_bear_diff': round(
                    bull_ratio - bear_ratio, 4),
                'stocktwits_bull_7d_avg':   round(bull_7d_avg, 4),
                'stocktwits_bull_delta':    bull_delta,
                # Rising bull ratio is a stronger signal than
                # absolute bull ratio — delta captures momentum
                'stocktwits_sentiment_rising': bull_delta > 0.10,
                'stocktwits_data_available': True,
                'stocktwits_data_age_hours': age_hours,
            }

        except Exception as e:
            return {
                'stocktwits_bull_ratio':    0.0,
                'stocktwits_bear_ratio':    0.0,
                'stocktwits_message_count': 0,
                'stocktwits_watchers':      0,
                'stocktwits_bull_bear_diff': 0.0,
                'stocktwits_data_available': False,
                'stocktwits_error':          str(e),
            }

    def _get_stocktwits_avg(self, ticker: str,
                             days: int = 7) -> float:
        """Returns average bull_ratio over last N days."""
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT AVG(bull_ratio)
                FROM   stocktwits_snapshots
                WHERE  ticker      = %s
                  AND  collected_at > NOW() - INTERVAL '%s days'
            """, (ticker, days))
            row = cur.fetchone()
            return float(row[0]) if row and row[0] else 0.0
        except Exception:
            return 0.0

    # ═════════════════════════════════════════════════════════
    # 2. GOOGLE TRENDS FEATURES
    # ═════════════════════════════════════════════════════════

    def _get_trends_features(self, ticker: str) -> dict:
        """
        Reads Google Trends data from trends_snapshots.
        Source: pytrends_collector.py
        Staleness: 1-4 hours

        Features:
          google_trend_score  — 0-100 relative search interest
          google_trend_avg_7d — 7-day rolling average
          google_trend_peak   — 7-day peak score
          trend_spike_flag    — bool: recent score > 3x avg
          trend_acceleration  — is trend score rising or falling?
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT trend_score,
                       trend_avg_7d,
                       trend_peak_7d,
                       trend_spike,
                       collected_at
                FROM   trends_snapshots
                WHERE  ticker = %s
                ORDER  BY collected_at DESC
                LIMIT  2
            """, (ticker,))
            rows = cur.fetchall()

            if not rows:
                return {
                    'google_trend_score':  0.0,
                    'google_trend_avg_7d': 0.0,
                    'google_trend_peak':   0.0,
                    'trend_spike_flag':    False,
                    'trend_acceleration':  'unknown',
                    'trends_data_available': False,
                }

            latest = rows[0]
            score     = float(latest[0]) if latest[0] else 0.0
            avg_7d    = float(latest[1]) if latest[1] else 0.0
            peak_7d   = float(latest[2]) if latest[2] else 0.0
            spike     = bool(latest[3])

            # Trend acceleration: is current score > previous?
            acceleration = 'unknown'
            if len(rows) >= 2:
                prev_score = float(rows[1][0]) if rows[1][0] else 0.0
                if score > prev_score * 1.15:
                    acceleration = 'rising'
                elif score < prev_score * 0.85:
                    acceleration = 'falling'
                else:
                    acceleration = 'stable'

            return {
                'google_trend_score':    round(score, 2),
                'google_trend_avg_7d':   round(avg_7d, 2),
                'google_trend_peak':     round(peak_7d, 2),
                'trend_spike_flag':      spike,
                'trend_acceleration':    acceleration,
                # Normalized 0-1 for feature vector
                'trend_score_normalized': round(score / 100, 4),
                'trends_data_available':  True,
            }

        except Exception as e:
            return {
                'google_trend_score':  0.0,
                'google_trend_avg_7d': 0.0,
                'trend_spike_flag':    False,
                'trend_acceleration':  'unknown',
                'trends_data_available': False,
                'trends_error':         str(e),
            }

    # ═════════════════════════════════════════════════════════
    # 3. NEWS CATALYST FEATURES
    # ═════════════════════════════════════════════════════════

    def _get_catalyst_features(self, ticker: str) -> dict:
        """
        Reads catalyst events from stock_events table.
        Written by news_collector.py when catalysts are detected.

        Features:
          news_catalyst_flag     — binary 0/1: any catalyst in 48h
          catalyst_count_48h     — how many catalyst events
          bullish_catalyst_flag  — binary: bullish catalyst present
          bearish_catalyst_flag  — binary: bearish catalyst present
          catalyst_recency_hours — how many hours ago was latest
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT event_data, occurred_at
                FROM   stock_events
                WHERE  ticker      = %s
                  AND  event_type  = 'catalyst'
                  AND  occurred_at > NOW() - INTERVAL '48 hours'
                ORDER  BY occurred_at DESC
                LIMIT  10
            """, (ticker,))
            rows = cur.fetchall()

            if not rows:
                return {
                    'news_catalyst_flag':    0,
                    'catalyst_count_48h':    0,
                    'bullish_catalyst_flag': 0,
                    'bearish_catalyst_flag': 0,
                    'catalyst_recency_hours': None,
                }

            import json
            bullish = 0
            bearish = 0

            for row in rows:
                try:
                    data = row[0] if isinstance(row[0], dict) \
                           else json.loads(row[0])
                    for headline in data.get('headlines', []):
                        for flag in headline.get('flags', []):
                            if flag.startswith('bullish:'):
                                bullish += 1
                            elif flag.startswith('bearish:'):
                                bearish += 1
                except Exception:
                    continue

            latest_occurred = rows[0][1]
            recency_hours   = None
            if latest_occurred:
                recency_hours = round(
                    (datetime.utcnow()
                     - latest_occurred.replace(tzinfo=None)
                     ).total_seconds() / 3600, 2)

            return {
                'news_catalyst_flag':    1,
                'catalyst_count_48h':    len(rows),
                'bullish_catalyst_flag': 1 if bullish > 0 else 0,
                'bearish_catalyst_flag': 1 if bearish > 0 else 0,
                'catalyst_recency_hours': recency_hours,
                'bullish_catalyst_count': bullish,
                'bearish_catalyst_count': bearish,
            }

        except Exception as e:
            return {
                'news_catalyst_flag':    0,
                'catalyst_count_48h':    0,
                'bullish_catalyst_flag': 0,
                'bearish_catalyst_flag': 0,
                'catalyst_error':        str(e),
            }

    # ═════════════════════════════════════════════════════════
    # 4. SEC ACTIVIST FILING FEATURES
    # ═════════════════════════════════════════════════════════

    def _get_activist_features(self, ticker: str) -> dict:
        """
        Reads activist filing events from stock_events.
        Written by sec_edgar_collector.py.

        An activist investor entering a heavily shorted stock
        is a classic squeeze catalyst — they may push for
        management changes that force shorts to cover.

        Features:
          has_activist_filing      — binary 0/1: 13D/13G in 30 days
          activist_filing_count    — number of filings
          has_insider_buying       — binary: insider buy in 30 days
          insider_net_sentiment    — 'net_buying'/'net_selling'/'none'
        """
        cur = self.pg.cursor()
        try:
            # Activist filings (13D/13G)
            cur.execute("""
                SELECT COUNT(*)
                FROM   stock_events
                WHERE  ticker      = %s
                  AND  event_type  = 'activist_filing'
                  AND  occurred_at > NOW() - INTERVAL '30 days'
            """, (ticker,))
            activist_count = cur.fetchone()[0]

            # Insider transactions
            cur.execute("""
                SELECT COUNT(*)
                FROM   stock_events
                WHERE  ticker      = %s
                  AND  event_type  = 'insider_transaction'
                  AND  occurred_at > NOW() - INTERVAL '30 days'
                  AND  event_data::text LIKE '%"transaction_type": "P"%'
            """, (ticker,))
            insider_buy_count = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM   stock_events
                WHERE  ticker      = %s
                  AND  event_type  = 'insider_transaction'
                  AND  occurred_at > NOW() - INTERVAL '30 days'
                  AND  event_data::text LIKE '%"transaction_type": "S"%'
            """, (ticker,))
            insider_sell_count = cur.fetchone()[0]

            # Net insider sentiment
            if insider_buy_count > insider_sell_count * 1.5:
                insider_sentiment = 'net_buying'
            elif insider_sell_count > insider_buy_count * 1.5:
                insider_sentiment = 'net_selling'
            elif insider_buy_count > 0 or insider_sell_count > 0:
                insider_sentiment = 'mixed'
            else:
                insider_sentiment = 'none'

            return {
                'has_activist_filing':   1 if activist_count > 0 else 0,
                'activist_filing_count': int(activist_count),
                'has_insider_buying':    1 if insider_buy_count > 0 else 0,
                'insider_buy_count':     int(insider_buy_count),
                'insider_sell_count':    int(insider_sell_count),
                'insider_net_sentiment': insider_sentiment,
            }

        except Exception as e:
            return {
                'has_activist_filing':   0,
                'activist_filing_count': 0,
                'has_insider_buying':    0,
                'insider_net_sentiment': 'none',
                'activist_error':        str(e),
            }

    # ═════════════════════════════════════════════════════════
    # 5. REDDIT MENTION FEATURES
    # ═════════════════════════════════════════════════════════

    def _get_mention_features(self, ticker: str) -> dict:
        """
        Computes Reddit mention velocity and acceleration.
        Reads from mention_timeseries (TimescaleDB hypertable).
        Written by reddit_collector.py via _save_mention_count().

        Returns zeros when Reddit data is not available.
        When Reddit API access is confirmed, these features
        activate automatically without code changes.

        Features:
          mentions_1h           — raw mention count last 1 hour
          mentions_6h           — raw mention count last 6 hours
          mentions_24h          — raw mention count last 24 hours
          mention_velocity_1h   — rate of change vs 7-day baseline
          mention_velocity_6h   — 6-hour velocity
          mention_acceleration  — is velocity itself rising?
          subreddit_diversity   — distinct subs in last 24h
        """
        cur = self.pg.cursor()
        try:
            # Get mention counts at multiple time windows
            windows = {'1h': 1, '6h': 6, '24h': 24, '168h': 168}
            counts  = {}

            for label, hours in windows.items():
                cur.execute("""
                    SELECT COALESCE(SUM(mention_count), 0)
                    FROM   mention_timeseries
                    WHERE  ticker = %s
                      AND  time   > NOW() - INTERVAL '%s hours'
                """, (ticker, hours))
                row = counts[label] = cur.fetchone()
                counts[label] = int(row[0]) if row else 0

            mentions_1h   = counts['1h']
            mentions_6h   = counts['6h']
            mentions_24h  = counts['24h']
            mentions_168h = counts['168h']  # 7-day total

            # Baseline = 7-day average per hour
            baseline_per_hour = mentions_168h / 168 \
                                if mentions_168h > 0 else 0

            # Velocity = deviation from baseline (normalized)
            vel_1h = self._velocity(mentions_1h,
                                     baseline_per_hour * 1)
            vel_6h = self._velocity(mentions_6h,
                                     baseline_per_hour * 6)

            # Acceleration = is velocity_1h > velocity_6h/6?
            # i.e. is the rate of change itself increasing?
            vel_6h_per_hour = vel_6h / 6 if vel_6h != 0 else 0
            acceleration    = vel_1h - vel_6h_per_hour

            # Subreddit diversity
            cur.execute("""
                SELECT COALESCE(MAX(subreddit_count), 0)
                FROM   mention_timeseries
                WHERE  ticker = %s
                  AND  time   > NOW() - INTERVAL '24 hours'
            """, (ticker,))
            row = cur.fetchone()
            subreddit_diversity = int(row[0]) if row else 0

            reddit_available = mentions_24h > 0

            return {
                'mentions_1h':          mentions_1h,
                'mentions_6h':          mentions_6h,
                'mentions_24h':         mentions_24h,
                'mention_velocity_1h':  round(vel_1h, 4),
                'mention_velocity_6h':  round(vel_6h, 4),
                'mention_acceleration': round(acceleration, 4),
                'subreddit_diversity':  subreddit_diversity,
                'mention_baseline_per_hour': round(
                    baseline_per_hour, 2),
                'reddit_data_available': reddit_available,
            }

        except Exception as e:
            return {
                'mentions_1h':          0,
                'mentions_6h':          0,
                'mentions_24h':         0,
                'mention_velocity_1h':  0.0,
                'mention_velocity_6h':  0.0,
                'mention_acceleration': 0.0,
                'subreddit_diversity':  0,
                'reddit_data_available': False,
                'mention_error':         str(e),
            }

    # ═════════════════════════════════════════════════════════
    # COMPOSITE SOCIAL SCORE
    # ═════════════════════════════════════════════════════════

    def _compute_social_score(self, features: dict) -> float:
        """
        Computes a composite 0-100 social squeeze score.
        Combines all social signals into one number.
        Used for Gate 2 signal threshold in candidate_evaluator.

        Scoring:
          StockTwits bull > 65%      → 20 pts
          StockTwits bull rising     → 10 pts
          Google Trends spike        → 20 pts
          Google Trends rising       → 10 pts
          News catalyst (bullish)    → 20 pts
          Activist filing present    → 10 pts
          Reddit velocity > 2x       → 10 pts (when available)
        """
        score = 0.0

        # StockTwits
        bull = features.get('stocktwits_bull_ratio', 0.0)
        if bull >= 0.65:
            score += 20
        elif bull >= 0.55:
            score += 10
        if features.get('stocktwits_sentiment_rising'):
            score += 10

        # Google Trends
        if features.get('trend_spike_flag'):
            score += 20
        if features.get('trend_acceleration') == 'rising':
            score += 10

        # News catalyst
        if features.get('bullish_catalyst_flag'):
            score += 20
        elif features.get('news_catalyst_flag'):
            score += 10

        # SEC activist
        if features.get('has_activist_filing'):
            score += 10

        # Reddit (when available)
        vel = features.get('mention_velocity_1h', 0.0)
        if vel >= 2.0:
            score += 10
        elif vel >= 1.0:
            score += 5

        return round(min(score, 100), 2)

    # ═════════════════════════════════════════════════════════
    # HELPER METHODS
    # ═════════════════════════════════════════════════════════

    @staticmethod
    def _velocity(current: float, baseline: float) -> float:
        """
        Computes normalized velocity vs baseline.
        Returns 0.0 when baseline is zero.

        velocity = (current - baseline) / baseline
          0.0  = exactly at baseline (normal)
          1.0  = 2x baseline (doubled)
          3.0  = 4x baseline (quadrupled)
         -0.5  = half of baseline (declining)
        """
        if baseline <= 0:
            return 0.0
        return round((current - baseline) / baseline, 4)

    def get_summary_text(self, ticker: str) -> str:
        """
        Returns human-readable social signal summary.
        Used by SHAP explainer and dashboard display.
        """
        features = self.compute(ticker)
        parts    = []

        bull = features.get('stocktwits_bull_ratio', 0.0)
        if bull > 0:
            parts.append(
                f'StockTwits: {bull:.0%} bullish')

        trend = features.get('google_trend_score', 0.0)
        spike = features.get('trend_spike_flag', False)
        if trend > 0:
            parts.append(
                f'Google Trends: {trend:.0f}/100'
                + (' (spike!)' if spike else ''))

        if features.get('news_catalyst_flag'):
            parts.append(
                f'{features.get("catalyst_count_48h", 0)} '
                f'catalyst events in 48h')

        if features.get('has_activist_filing'):
            parts.append('activist filing detected')

        vel = features.get('mention_velocity_1h', 0.0)
        if vel > 0 and features.get('reddit_data_available'):
            parts.append(
                f'Reddit velocity: {vel:+.1f}x baseline')

        if not parts:
            return f'{ticker}: no notable social signals.'

        score = features.get('social_score', 0.0)
        return (f'{ticker} social (score {score:.0f}/100): '
                + ' | '.join(parts) + '.')

    def close(self):
        self.pg.close()


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    engine = SocialFeatureEngine()

    print('='*55)
    print('  Social Feature Engine — Manual Test')
    print('='*55)

    test_tickers = ['GME', 'AAPL', 'XYZFAKE']

    for ticker in test_tickers:
        print(f'\n── {ticker} ──')
        features = engine.compute(ticker)

        # StockTwits
        print(f'  stocktwits_bull_ratio:      '
              f'{features.get("stocktwits_bull_ratio")}')
        print(f'  stocktwits_bear_ratio:      '
              f'{features.get("stocktwits_bear_ratio")}')
        print(f'  stocktwits_message_count:   '
              f'{features.get("stocktwits_message_count")}')
        print(f'  stocktwits_bull_delta:      '
              f'{features.get("stocktwits_bull_delta")}')
        print(f'  stocktwits_data_available:  '
              f'{features.get("stocktwits_data_available")}')

        # Google Trends
        print(f'  google_trend_score:         '
              f'{features.get("google_trend_score")}')
        print(f'  trend_spike_flag:           '
              f'{features.get("trend_spike_flag")}')
        print(f'  trend_acceleration:         '
              f'{features.get("trend_acceleration")}')

        # Catalyst
        print(f'  news_catalyst_flag:         '
              f'{features.get("news_catalyst_flag")}')
        print(f'  catalyst_count_48h:         '
              f'{features.get("catalyst_count_48h")}')
        print(f'  bullish_catalyst_flag:      '
              f'{features.get("bullish_catalyst_flag")}')

        # SEC
        print(f'  has_activist_filing:        '
              f'{features.get("has_activist_filing")}')
        print(f'  insider_net_sentiment:      '
              f'{features.get("insider_net_sentiment")}')

        # Reddit
        print(f'  mentions_24h:               '
              f'{features.get("mentions_24h")}')
        print(f'  mention_velocity_1h:        '
              f'{features.get("mention_velocity_1h")}')
        print(f'  reddit_data_available:      '
              f'{features.get("reddit_data_available")}')

        # Composite
        print(f'  social_score:               '
              f'{features.get("social_score")}/100')
        print(f'  compute_duration_ms:        '
              f'{features.get("compute_duration_ms")}ms')

        print(f'\n  Summary: {engine.get_summary_text(ticker)}')

    engine.close()
    print('\n✓ Social feature engine test complete')