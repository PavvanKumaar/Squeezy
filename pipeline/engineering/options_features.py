# pipeline/engineering/options_features.py
#
# Options Feature Engine
#
# Reads options snapshot data from PostgreSQL and computes
# all options-side features for the ML feature vector.
#
# Data sources (reads from):
#   PostgreSQL  options_snapshots  — call/put OI, IV, max pain
#   PostgreSQL  stock_timeseries   — current price (for max pain distance)
#
# Features produced (5 options features for ML vector):
#   call_put_ratio      — speculative sentiment indicator
#   iv_rank             — how expensive options are vs history
#   max_pain_distance   — how far price is from max pain strike
#   options_available   — bool: does this stock have options?
#   unusual_sweep_flag  — bool: unusually large single-strike OI
#
# Why options matter for squeeze detection:
#
#   call_put_ratio > 2.5:
#     Retail is buying calls aggressively.
#     Market makers MUST buy shares to hedge (delta hedging).
#     This creates mechanical buying pressure = squeeze amplifier.
#
#   iv_rank > 80%:
#     Market is pricing in a large move.
#     Often precedes the actual squeeze by 1-3 days.
#
#   max_pain_distance:
#     If price is far ABOVE max pain → market makers must sell.
#     If price is far BELOW max pain → market makers must buy.
#     Understanding this direction matters for squeeze dynamics.
#
#   unusual_sweep_flag:
#     A single option contract with OI >> normal levels
#     often indicates institutional positioning before a move.

import os
import time
import psycopg2
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# IV rank thresholds for classification
IV_RANK_TIERS = [
    (0,   25,  'low'),        # cheap options, market calm
    (25,  50,  'moderate'),   # normal conditions
    (50,  75,  'elevated'),   # market expecting movement
    (75,  90,  'high'),       # significant move expected
    (90,  100, 'extreme'),    # GME/AMC-level volatility
]

# Call/put ratio interpretation tiers
CP_RATIO_TIERS = [
    (0.0,  0.7,  'heavy_puts'),    # bearish dominance
    (0.7,  1.2,  'balanced'),      # neutral sentiment
    (1.2,  2.0,  'call_leaning'),  # mild bullish bias
    (2.0,  3.5,  'call_heavy'),    # speculative buying
    (3.5,  99.0, 'extreme_calls'), # squeeze fuel / mania
]


def classify_iv_rank(iv_rank: float) -> str:
    for lo, hi, label in IV_RANK_TIERS:
        if lo <= iv_rank < hi:
            return label
    return 'extreme'


def classify_cp_ratio(ratio: float) -> str:
    for lo, hi, label in CP_RATIO_TIERS:
        if lo <= ratio < hi:
            return label
    return 'extreme_calls'


class OptionsFeatureEngine:
    """
    Computes all options-side features for a given ticker.

    Reads from options_snapshots (written by options_collector).
    Reads current price from stock_timeseries for distance calcs.

    Usage:
        engine = OptionsFeatureEngine()
        features = engine.compute('GME')
        # Returns dict with 5+ options features
    """

    def __init__(self):
        self.pg = psycopg2.connect(os.getenv('POSTGRES_URL'))

    # ═════════════════════════════════════════════════════════
    # MAIN METHOD
    # ═════════════════════════════════════════════════════════

    def compute(self, ticker: str) -> dict:
        """
        Computes all options features for one ticker.
        Returns dict with all options features.
        Missing data → 0.0 defaults (never raises).

        Returns dict with:
          options_available    — bool
          call_put_ratio       — float (0 to ~10)
          call_put_tier        — category string
          iv_rank              — float 0-100
          iv_rank_tier         — category string
          iv_current           — current implied volatility
          max_pain             — max pain strike price
          max_pain_distance    — normalized distance from current price
          max_pain_direction   — 'above_pain' / 'below_pain' / 'at_pain'
          unusual_sweep_flag   — bool
          total_call_oi        — integer
          total_put_oi         — integer
          options_score        — composite 0-100 squeeze signal
        """
        ticker = ticker.upper().strip()
        start  = time.time()

        # Check if options exist for this ticker
        snapshot = self._get_latest_snapshot(ticker)

        if snapshot is None:
            return self._no_options_result(ticker)

        if not snapshot.get('options_eligible', True):
            return self._no_options_result(ticker)

        # Build feature dict from snapshot
        result = {'ticker': ticker, 'options_available': True}

        # ── Core metrics from snapshot ───────────────────
        result.update(self._call_put_features(snapshot))
        result.update(self._iv_features(snapshot, ticker))
        result.update(self._max_pain_features(snapshot, ticker))
        result.update(self._volume_features(snapshot))
        result.update(self._sweep_features(snapshot))

        # ── Composite options squeeze score ──────────────
        result['options_score'] = self._compute_options_score(result)

        result['computed_at']       = datetime.utcnow()
        result['compute_duration_ms'] = int((time.time()-start)*1000)

        return result

    def compute_history(self, ticker: str,
                         hours: int = 168) -> list:
        """
        Returns options feature history for a ticker.
        Used for trend analysis and dashboard charts.
        Default: last 7 days.

        Returns list of dicts with timestamp + key metrics.
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT collected_at,
                       call_put_ratio, iv_rank,
                       total_call_oi, total_put_oi,
                       max_pain
                FROM   options_snapshots
                WHERE  ticker      = %s
                  AND  collected_at > NOW() - INTERVAL '%s hours'
                ORDER  BY collected_at ASC
            """, (ticker, hours))

            return [
                {
                    'time':          r[0].isoformat(),
                    'call_put_ratio': float(r[1]) if r[1] else 0.0,
                    'iv_rank':       float(r[2]) if r[2] else 0.0,
                    'total_call_oi': int(r[3]) if r[3] else 0,
                    'total_put_oi':  int(r[4]) if r[4] else 0,
                    'max_pain':      float(r[5]) if r[5] else 0.0,
                }
                for r in cur.fetchall()
            ]
        except Exception as e:
            return []

    # ═════════════════════════════════════════════════════════
    # FEATURE COMPUTATION METHODS
    # ═════════════════════════════════════════════════════════

    def _call_put_features(self, snapshot: dict) -> dict:
        """
        Computes call/put ratio features.

        call_put_ratio by open interest:
          > 1.0 = more calls than puts (bullish bias)
          > 2.5 = strong speculative call buying
          > 4.0 = extreme — squeeze fuel territory
          < 0.7 = put-heavy — market is hedging downside

        call_put_volume_ratio:
          Same but by daily volume (more current than OI)
        """
        cp_oi  = snapshot.get('call_put_ratio', 0.0) or 0.0

        # Volume-based ratio
        call_vol = snapshot.get('total_call_vol', 0) or 0
        put_vol  = snapshot.get('total_put_vol', 0) or 0
        cp_vol   = round(call_vol / put_vol, 4) \
                   if put_vol > 0 else 0.0

        # Call dominance: what % of total OI is calls?
        call_oi   = snapshot.get('total_call_oi', 0) or 0
        put_oi    = snapshot.get('total_put_oi', 0) or 0
        total_oi  = call_oi + put_oi
        call_pct  = round(call_oi / total_oi, 4) \
                    if total_oi > 0 else 0.5

        return {
            'call_put_ratio':        round(float(cp_oi), 4),
            'call_put_tier':         classify_cp_ratio(cp_oi),
            'call_put_volume_ratio': round(float(cp_vol), 4),
            'call_oi_pct':           call_pct,
            'total_call_oi':         int(call_oi),
            'total_put_oi':          int(put_oi),
            'total_options_oi':      int(total_oi),
            'call_put_bullish':      cp_oi >= 2.0,
        }

    def _iv_features(self, snapshot: dict,
                      ticker: str) -> dict:
        """
        Computes implied volatility features.

        iv_rank (0-100):
          Where current IV sits relative to recent history.
          Computed as proxy from options chain spread.
          True IV rank needs 52-week IV history (paid data).

        iv_current:
          Raw average IV across all strikes.
          >100% is normal for meme stocks during squeezes.

        iv_momentum:
          Is IV rising or falling vs previous snapshot?
          Rising IV = market pricing in a coming move.
        """
        iv_current = snapshot.get('iv_current', 0.0) or 0.0
        iv_pct     = snapshot.get('iv_pct', 0.0) or 0.0

        # IV rank from snapshot (proxy computation)
        iv_rank = snapshot.get('iv_rank', 0.0) or 0.0

        # If iv_rank not in snapshot, estimate from iv_current
        if iv_rank == 0.0 and iv_current > 0:
            # Rough heuristic: IV > 200% = very high rank
            iv_rank = min(iv_current * 40, 100.0)

        # IV momentum: compare to previous snapshot
        iv_momentum = self._compute_iv_momentum(ticker, iv_current)

        return {
            'iv_current':       round(float(iv_current), 6),
            'iv_pct':           round(float(iv_pct), 2),
            'iv_rank':          round(float(iv_rank), 2),
            'iv_rank_tier':     classify_iv_rank(iv_rank),
            'iv_elevated':      iv_rank >= 60,
            'iv_extreme':       iv_rank >= 90,
            'iv_momentum':      iv_momentum,
            # 'rising' / 'falling' / 'stable' / 'unknown'
        }

    def _max_pain_features(self, snapshot: dict,
                            ticker: str) -> dict:
        """
        Computes max pain related features.

        Max pain = the strike price at which the most option
        contracts expire worthless. Market makers are naturally
        incentivized to pin price near max pain at expiry.

        max_pain_distance:
          Normalized distance of current price from max pain.
          Positive = price ABOVE max pain (MM must sell to pin)
          Negative = price BELOW max pain (MM must buy to pin)
          Large negative = market makers are BUYERS = squeeze help

        max_pain_direction:
          'above_pain'  — price above, MMs selling pressure
          'below_pain'  — price below, MMs buying pressure (squeeze amplifier)
          'at_pain'     — price near max pain, neutral
        """
        max_pain = snapshot.get('max_pain', 0.0) or 0.0

        if max_pain <= 0:
            return {
                'max_pain':          0.0,
                'max_pain_distance': 0.0,
                'max_pain_direction': 'unknown',
                'max_pain_available': False,
            }

        # Get current price
        current_price = self._get_current_price(ticker)

        if current_price <= 0:
            return {
                'max_pain':           round(float(max_pain), 2),
                'max_pain_distance':  0.0,
                'max_pain_direction': 'unknown',
                'max_pain_available': True,
            }

        # Normalized distance (as % of current price)
        distance = (current_price - max_pain) / current_price
        distance_pct = round(distance * 100, 2)

        # Direction classification
        if distance > 0.05:      # Price > 5% above max pain
            direction = 'above_pain'
        elif distance < -0.05:   # Price > 5% below max pain
            direction = 'below_pain'
        else:
            direction = 'at_pain'

        return {
            'max_pain':              round(float(max_pain), 2),
            'max_pain_distance':     round(float(distance), 6),
            'max_pain_distance_pct': distance_pct,
            'max_pain_direction':    direction,
            'max_pain_available':    True,
            'price_vs_pain':         round(float(current_price), 2),
            # Squeeze favorable: price below max pain
            # (MM must buy to hedge)
            'max_pain_bullish':      direction == 'below_pain',
        }

    def _volume_features(self, snapshot: dict) -> dict:
        """
        Computes options volume features.
        Options volume is more current than OI.
        A sudden spike in call volume precedes price moves.
        """
        call_vol  = snapshot.get('total_call_vol', 0) or 0
        put_vol   = snapshot.get('total_put_vol', 0) or 0
        total_vol = call_vol + put_vol

        return {
            'total_call_vol':    int(call_vol),
            'total_put_vol':     int(put_vol),
            'total_options_vol': int(total_vol),
            'call_vol_pct':      round(call_vol / total_vol, 4)
                                  if total_vol > 0 else 0.5,
        }

    def _sweep_features(self, snapshot: dict) -> dict:
        """
        Detects unusual single-strike open interest.
        A single strike with OI >> normal indicates
        large institutional positioning.

        Currently uses a heuristic based on call/put ratio
        extremes as a proxy for sweep detection.
        Real sweep detection needs tick-by-tick options data
        (Unusual Whales API — paid).

        For MVP: flag when CP ratio > 4.0 AND IV rank > 70
        as a proxy for "something unusual is happening."
        """
        cp_ratio = snapshot.get('call_put_ratio', 0.0) or 0.0
        iv_rank  = snapshot.get('iv_rank', 0.0) or 0.0
        call_oi  = snapshot.get('total_call_oi', 0) or 0
        put_oi   = snapshot.get('total_put_oi', 0) or 0

        # Heuristic sweep detection
        # True sweep detection needs tick data (future upgrade)
        proxy_sweep = (
            cp_ratio > 4.0 and
            iv_rank  > 70 and
            call_oi  > put_oi * 3
        )

        return {
            'unusual_sweep_flag':   proxy_sweep,
            'sweep_confidence':     'proxy',
            # 'proxy' until we get tick-level options data
        }

    # ═════════════════════════════════════════════════════════
    # COMPOSITE OPTIONS SQUEEZE SCORE
    # ═════════════════════════════════════════════════════════

    def _compute_options_score(self, features: dict) -> float:
        """
        Computes a composite 0-100 options squeeze score.
        Combines all options signals into one number.
        Used as a single feature in the final 28-vector
        AND as a standalone signal for SHAP explanation.

        Scoring logic:
          call_put_ratio contribution (0-40 points):
            > 1.0  = 10 pts
            > 2.0  = 20 pts
            > 3.0  = 30 pts
            > 4.0  = 40 pts

          iv_rank contribution (0-30 points):
            > 50   = 10 pts
            > 70   = 20 pts
            > 85   = 30 pts

          max_pain_direction contribution (0-20 points):
            below_pain = 20 pts (MMs must buy = squeeze help)
            at_pain    = 10 pts
            above_pain = 0 pts

          unusual_sweep_flag (0-10 points):
            True = 10 pts
        """
        score = 0.0

        # Call/put ratio points
        cp = features.get('call_put_ratio', 0.0)
        if cp >= 4.0:
            score += 40
        elif cp >= 3.0:
            score += 30
        elif cp >= 2.0:
            score += 20
        elif cp >= 1.0:
            score += 10

        # IV rank points
        ivr = features.get('iv_rank', 0.0)
        if ivr >= 85:
            score += 30
        elif ivr >= 70:
            score += 20
        elif ivr >= 50:
            score += 10

        # Max pain direction points
        direction = features.get('max_pain_direction', 'unknown')
        if direction == 'below_pain':
            score += 20
        elif direction == 'at_pain':
            score += 10

        # Unusual sweep bonus
        if features.get('unusual_sweep_flag'):
            score += 10

        return round(min(score, 100), 2)

    # ═════════════════════════════════════════════════════════
    # DATABASE READS
    # ═════════════════════════════════════════════════════════

    def _get_latest_snapshot(self,
                              ticker: str) -> Optional[dict]:
        """Gets the most recent options snapshot for a ticker."""
        cur = self.pg.cursor()
        try:
            # Note: iv_current and iv_pct columns were added
            # after initial table creation. We use COALESCE
            # to handle both old rows (NULL) and new rows.
            cur.execute("""
                SELECT call_put_ratio,
                       iv_rank,
                       COALESCE(iv_current, 0.0),
                       COALESCE(iv_pct, 0.0),
                       max_pain,
                       total_call_oi,
                       total_put_oi,
                       total_call_vol,
                       total_put_vol,
                       nearest_expiry,
                       expiry_count,
                       options_eligible,
                       collected_at
                FROM   options_snapshots
                WHERE  ticker = %s
                ORDER  BY collected_at DESC
                LIMIT  1
            """, (ticker,))
            row = cur.fetchone()

            if not row:
                return None

            return {
                'call_put_ratio':   row[0],
                'iv_rank':          row[1],
                'iv_current':       row[2],   # 0.0 for old rows
                'iv_pct':           row[3],   # 0.0 for old rows
                'max_pain':         row[4],
                'total_call_oi':    row[5],
                'total_put_oi':     row[6],
                'total_call_vol':   row[7],
                'total_put_vol':    row[8],
                'nearest_expiry':   row[9],
                'expiry_count':     row[10],
                'options_eligible': row[11],
                'collected_at':     row[12],
            }
        except Exception as e:
            print(f'  [options_features] snapshot error: {e}')
            return None

    def _get_current_price(self, ticker: str) -> float:
        """Gets most recent closing price for max pain distance calc."""
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT close
                FROM   stock_timeseries
                WHERE  ticker   = %s
                  AND  interval = '1d'
                ORDER  BY time DESC
                LIMIT  1
            """, (ticker,))
            row = cur.fetchone()
            return float(row[0]) if row and row[0] else 0.0
        except Exception:
            return 0.0

    def _compute_iv_momentum(self, ticker: str,
                               current_iv: float) -> str:
        """
        Compares current IV rank to previous snapshot.
        Uses iv_rank (always present) instead of iv_current
        which may not exist in older rows.
        Returns 'rising', 'falling', 'stable', or 'unknown'.
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT iv_rank
                FROM   options_snapshots
                WHERE  ticker   = %s
                  AND  iv_rank IS NOT NULL
                ORDER  BY collected_at DESC
                LIMIT  2
                OFFSET 1
            """, (ticker,))
            row = cur.fetchone()

            if not row or not row[0]:
                return 'unknown'

            prev_iv = float(row[0])
            if prev_iv == 0:
                return 'unknown'

            delta = current_iv - prev_iv

            if delta > prev_iv * 0.05:   # > 5% increase
                return 'rising'
            if delta < -prev_iv * 0.05:  # > 5% decrease
                return 'falling'
            return 'stable'

        except Exception:
            return 'unknown'

    # ═════════════════════════════════════════════════════════
    # HELPER METHODS
    # ═════════════════════════════════════════════════════════

    @staticmethod
    def _no_options_result(ticker: str) -> dict:
        """Returns zeroed result when no options data available."""
        return {
            'ticker':              ticker,
            'options_available':   False,
            'call_put_ratio':      0.0,
            'call_put_tier':       'unknown',
            'iv_rank':             0.0,
            'iv_rank_tier':        'unknown',
            'iv_current':          0.0,
            'max_pain':            0.0,
            'max_pain_distance':   0.0,
            'max_pain_direction':  'unknown',
            'unusual_sweep_flag':  False,
            'total_call_oi':       0,
            'total_put_oi':        0,
            'options_score':       0.0,
            'computed_at':         datetime.utcnow(),
        }

    def get_options_signal_summary(self,
                                    ticker: str) -> str:
        """
        Returns a human-readable summary of options signals.
        Used by SHAP explainer for dashboard explanation text.
        """
        features = self.compute(ticker)

        if not features.get('options_available'):
            return f'{ticker} has no options data.'

        parts = []

        cp = features.get('call_put_ratio', 0)
        if cp >= 3.0:
            parts.append(
                f'extreme call/put ratio of {cp:.1f}x '
                f'(retail piling into calls)')
        elif cp >= 2.0:
            parts.append(
                f'elevated call/put ratio of {cp:.1f}x')

        ivr = features.get('iv_rank', 0)
        if ivr >= 80:
            parts.append(
                f'IV rank at {ivr:.0f}% '
                f'(market pricing in large move)')
        elif ivr >= 60:
            parts.append(f'elevated IV rank at {ivr:.0f}%')

        direction = features.get('max_pain_direction', '')
        if direction == 'below_pain':
            parts.append(
                f'price below max pain ${features["max_pain"]:.2f} '
                f'(market makers are buyers)')
        elif direction == 'above_pain':
            parts.append(
                f'price above max pain ${features["max_pain"]:.2f}')

        if features.get('unusual_sweep_flag'):
            parts.append('unusual options activity detected')

        if not parts:
            return (f'{ticker} options: normal activity, '
                    f'no unusual signals')

        return f'{ticker} options: ' + '; '.join(parts) + '.'

    def close(self):
        self.pg.close()


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    engine = OptionsFeatureEngine()

    print('='*55)
    print('  Options Feature Engine — Manual Test')
    print('='*55)

    test_tickers = ['GME', 'AAPL', 'SPY', 'XYZFAKE']

    for ticker in test_tickers:
        print(f'\n── {ticker} ──')
        features = engine.compute(ticker)

        if not features['options_available']:
            print(f'  No options data available')
            continue

        print(f'  options_available    = '
              f'{features["options_available"]}')
        print(f'  call_put_ratio       = '
              f'{features["call_put_ratio"]} '
              f'({features["call_put_tier"]})')
        print(f'  iv_rank              = '
              f'{features["iv_rank"]}% '
              f'({features["iv_rank_tier"]})')
        print(f'  iv_current           = '
              f'{features["iv_current"]}')
        print(f'  iv_momentum          = '
              f'{features.get("iv_momentum")}')
        print(f'  max_pain             = '
              f'${features["max_pain"]}')
        print(f'  max_pain_distance    = '
              f'{features["max_pain_distance"]:.4f}')
        print(f'  max_pain_direction   = '
              f'{features["max_pain_direction"]}')
        print(f'  unusual_sweep_flag   = '
              f'{features["unusual_sweep_flag"]}')
        print(f'  total_call_oi        = '
              f'{features["total_call_oi"]:,}')
        print(f'  total_put_oi         = '
              f'{features["total_put_oi"]:,}')
        print(f'  options_score        = '
              f'{features["options_score"]}/100')
        print(f'  compute_duration_ms  = '
              f'{features.get("compute_duration_ms")}ms')

        # Human-readable summary
        print(f'\n  Summary: '
              f'{engine.get_options_signal_summary(ticker)}')

    # History test
    print('\n── GME options history (last 48h) ──')
    history = engine.compute_history('GME', hours=48)
    print(f'  {len(history)} snapshots found')
    if history:
        latest = history[-1]
        print(f'  Latest: {latest["time"]} | '
              f'C/P={latest["call_put_ratio"]} | '
              f'IV={latest["iv_rank"]}%')

    engine.close()
    print('\n✓ Options feature engine test complete')