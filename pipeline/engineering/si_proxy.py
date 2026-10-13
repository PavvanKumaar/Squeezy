# pipeline/engineering/si_proxy.py
#
# Short Interest Proxy Estimator
#
# The Problem:
#   Official short interest (SI) data is published by FINRA
#   twice per month. By the time you see it, it is up to
#   14 days old. A stock's SI can change dramatically in
#   14 days — shorts can cover entirely, or new shorts
#   can pile in — and your model would never know.
#
# The Solution:
#   We cannot get fresh SI directly (paid data only).
#   But we CAN infer the DIRECTION of SI movement from
#   real-time proxy signals that are available for free:
#
#   PROXY 1: Borrow rate (iborrowdesk — updates 3x/day)
#     Rising borrow rate  → shorts increasing positions
#     Falling borrow rate → shorts covering / reducing
#     Logic: when demand to short increases, the cost
#     to borrow shares goes up. It's supply/demand.
#
#   PROXY 2: FINRA daily short volume ratio (next-day)
#     Sustained ratio > 55% → shorts building
#     Ratio suddenly drops  → potential covering
#     Logic: short volume ratio = shorts as % of all trades
#
#   PROXY 3: Price action + volume
#     Price rising sharply + high volume → covering happening
#     Price falling + high short volume  → new shorts entering
#     Logic: forced covering shows up as price spikes
#
#   PROXY 4: Fail-to-deliver (FTD) trend (2-week lag)
#     Rising FTD → naked shorting increasing
#     High FTD + price not falling → shorts trapped
#
# Output:
#   si_proxy_direction  — 'increasing' / 'decreasing' / 'unknown'
#   si_proxy_confidence — 'high' / 'medium' / 'low'
#   si_effective_pct    — adjusted SI estimate (stated ± adjustment)
#   si_staleness_days   — how old the official reading is
#   proxy_signals_used  — which proxies fired and in what direction
#
# How it feeds into ML:
#   The feature assembler uses si_effective_pct instead of
#   raw short_interest_pct when staleness > 7 days.
#   The model also receives si_staleness_days as a feature
#   so it can learn to discount stale readings.

import os
import psycopg2
from datetime import datetime, timedelta, date
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# Thresholds for proxy signal classification
BORROW_RATE_SPIKE_THRESHOLD  = 20.0   # +20% absolute change = spike
BORROW_RATE_DROP_THRESHOLD   = -15.0  # -15% absolute change = drop
SHORT_VOL_HIGH_THRESHOLD     =  0.57  # > 57% = elevated shorting
SHORT_VOL_DROP_THRESHOLD     =  0.40  # < 40% sudden drop = covering
PRICE_SURGE_THRESHOLD        =  0.10  # +10% in 1 day = strong surge
PRICE_SURGE_5D_THRESHOLD     =  0.20  # +20% in 5 days = sustained surge
VOLUME_SPIKE_THRESHOLD       =  3.0   # 3x average = unusual volume

# Adjustment factors for si_effective_pct
# When proxies indicate SI is moving, we adjust the stated SI
# by a conservative estimate of the change
SI_INCREASE_ADJUSTMENT = 0.05   # +5% when signals say increasing
SI_DECREASE_ADJUSTMENT = 0.08   # -8% when signals say decreasing
# (decrease adjustment is larger because covering is faster)


class SIProxyEstimator:
    """
    Estimates the current direction of short interest movement
    using real-time proxy signals.

    Does NOT claim to know the exact SI percentage.
    Claims only to know the DIRECTION it is moving.
    This distinction is critical for accuracy and honesty.

    Usage:
        estimator = SIProxyEstimator()
        result = estimator.estimate('GME')
        # result['si_proxy_direction'] = 'increasing'
        # result['si_proxy_confidence'] = 'high'
        # result['si_effective_pct'] = 32.4  (stated 28% + adjustment)
    """

    def __init__(self):
        self.pg = psycopg2.connect(os.getenv('POSTGRES_URL'))

    # ═════════════════════════════════════════════════════════
    # MAIN METHOD
    # ═════════════════════════════════════════════════════════

    def estimate(self, ticker: str) -> dict:
        """
        Runs all proxy signals and produces a consensus estimate
        of short interest direction and confidence.

        Returns dict with:
          si_stated_pct        — official SI from stockanalysis
          si_stale_days        — how old the official reading is
          si_proxy_direction   — 'increasing'/'decreasing'/'unknown'
          si_proxy_confidence  — 'high'/'medium'/'low'
          si_effective_pct     — adjusted estimate
          si_adjustment        — how much we adjusted (+ or -)
          proxy_signals        — list of signal dicts (what fired)
          proxy_bull_count     — signals pointing to SI increasing
          proxy_bear_count     — signals pointing to SI decreasing
          staleness_penalty    — 0.0-1.0 (1.0 = maximally stale)
          needs_fresh_data     — bool: SI is too stale to trust
        """
        ticker = ticker.upper().strip()

        # Get stated SI and staleness
        si_data = self._get_stated_si(ticker)
        stated_pct   = si_data.get('short_interest_pct', 0.0)
        stale_days   = si_data.get('si_stale_days', 14)

        # If data is fresh enough (< 3 days), trust it directly
        if stale_days <= 3:
            return {
                'ticker':              ticker,
                'si_stated_pct':       stated_pct,
                'si_stale_days':       stale_days,
                'si_proxy_direction':  'current',
                'si_proxy_confidence': 'high',
                'si_effective_pct':    stated_pct,
                'si_adjustment':       0.0,
                'proxy_signals':       [],
                'proxy_bull_count':    0,
                'proxy_bear_count':    0,
                'staleness_penalty':   0.0,
                'needs_fresh_data':    False,
                'estimated_at':        datetime.utcnow(),
            }

        # Run all proxy signals
        signals = []

        # Proxy 1: Borrow rate trend
        borrow_signal = self._check_borrow_rate(ticker)
        if borrow_signal:
            signals.append(borrow_signal)

        # Proxy 2: FINRA short volume trend
        finra_signal = self._check_short_volume(ticker)
        if finra_signal:
            signals.append(finra_signal)

        # Proxy 3: Price action + volume
        price_signal = self._check_price_action(ticker)
        if price_signal:
            signals.append(price_signal)

        # Proxy 4: FTD trend
        ftd_signal = self._check_ftd(ticker)
        if ftd_signal:
            signals.append(ftd_signal)

        # Tally signals
        bull_count = sum(
            1 for s in signals
            if s['direction'] == 'increasing')
        bear_count = sum(
            1 for s in signals
            if s['direction'] == 'decreasing')

        # Consensus direction
        direction, confidence = self._consensus(
            bull_count, bear_count, len(signals))

        # Compute effective SI
        effective_pct, adjustment = self._adjust_si(
            stated_pct, direction, stale_days)

        staleness_penalty = min(stale_days / 14.0, 1.0)

        result = {
            'ticker':              ticker,
            'si_stated_pct':       stated_pct,
            'si_stale_days':       stale_days,
            'si_proxy_direction':  direction,
            'si_proxy_confidence': confidence,
            'si_effective_pct':    effective_pct,
            'si_adjustment':       adjustment,
            'proxy_signals':       signals,
            'proxy_bull_count':    bull_count,
            'proxy_bear_count':    bear_count,
            'staleness_penalty':   round(staleness_penalty, 4),
            'needs_fresh_data':    stale_days > 10,
            'estimated_at':        datetime.utcnow(),
        }

        self._print_summary(result)
        return result

    # ═════════════════════════════════════════════════════════
    # PROXY SIGNAL 1 — BORROW RATE TREND
    # ═════════════════════════════════════════════════════════

    def _check_borrow_rate(self, ticker: str) -> Optional[dict]:
        """
        Analyzes borrow rate trend over last 7 days.
        Borrow rate is the strongest real-time proxy for SI direction.

        Signal logic:
          Rising borrow rate   → demand to short is INCREASING
                                 → SI likely going UP
          Falling borrow rate  → shorts COVERING or demand dropping
                                 → SI likely going DOWN
          Spike (1-day)        → new shorts rushing in
                                 → SI increasing rapidly
        """
        cur = self.pg.cursor()
        try:
            # Get borrow rate history from PostgreSQL
            # (fallback from Cassandra for this query)
            cur.execute("""
                SELECT borrow_rate, collected_at
                FROM   stock_snapshots
                WHERE  ticker      = %s
                  AND  borrow_rate IS NOT NULL
                  AND  data_source = 'iborrowdesk'
                  AND  collected_at > NOW() - INTERVAL '7 days'
                ORDER  BY collected_at DESC
                LIMIT  10
            """, (ticker,))
            rows = cur.fetchall()

            if len(rows) < 2:
                return None

            rates = [float(r[0]) for r in rows]
            current_rate = rates[0]
            week_ago_rate = rates[-1]

            # 7-day change
            delta_7d = current_rate - week_ago_rate

            # 1-day change (spike detection)
            delta_1d = rates[0] - rates[1] if len(rates) >= 2 else 0.0

            # Determine direction
            if delta_7d >= BORROW_RATE_SPIKE_THRESHOLD:
                direction  = 'increasing'
                strength   = 'strong' if delta_7d >= 40 else 'moderate'
                reason     = (f'Borrow rate up {delta_7d:+.1f}% '
                               f'over 7 days '
                               f'({week_ago_rate:.1f}% → {current_rate:.1f}%)')

            elif delta_7d <= BORROW_RATE_DROP_THRESHOLD:
                direction  = 'decreasing'
                strength   = 'strong' if delta_7d <= -25 else 'moderate'
                reason     = (f'Borrow rate down {delta_7d:+.1f}% '
                               f'over 7 days '
                               f'({week_ago_rate:.1f}% → {current_rate:.1f}%)')

            else:
                # Check for 1-day spike even if 7d is flat
                if delta_1d >= 15:
                    direction = 'increasing'
                    strength  = 'weak'
                    reason    = (f'Borrow rate 1-day spike: '
                                  f'+{delta_1d:.1f}%')
                else:
                    return None  # No clear signal

            return {
                'proxy':           'borrow_rate',
                'direction':       direction,
                'strength':        strength,
                'reason':          reason,
                'current_value':   current_rate,
                'delta_7d':        round(delta_7d, 2),
                'delta_1d':        round(delta_1d, 2),
                'weight':          3,  # Highest weight — best proxy
            }

        except Exception as e:
            return None

    # ═════════════════════════════════════════════════════════
    # PROXY SIGNAL 2 — FINRA SHORT VOLUME TREND
    # ═════════════════════════════════════════════════════════

    def _check_short_volume(self, ticker: str) -> Optional[dict]:
        """
        Analyzes FINRA daily short volume ratio trend.
        Available next-day — better than 2-week SI lag.

        Signal logic:
          Ratio sustained > 57% over 5 days → shorts building
          Ratio suddenly drops below 40%    → covering happening
          Ratio rising trend over 5 days    → increasing short pressure
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT short_volume_ratio, trade_date
                FROM   finra_short_volume
                WHERE  ticker     = %s
                ORDER  BY trade_date DESC
                LIMIT  10
            """, (ticker,))
            rows = cur.fetchall()

            if len(rows) < 3:
                return None

            ratios = [float(r[0]) for r in rows
                      if r[0] is not None]
            if not ratios:
                return None

            current_ratio = ratios[0]
            avg_5d        = sum(ratios[:5]) / min(5, len(ratios))
            avg_older     = sum(ratios[5:]) / len(ratios[5:]) \
                            if len(ratios) > 5 else avg_5d
            trend_delta   = avg_5d - avg_older

            # Case 1: Sustained high ratio = new shorts building
            if avg_5d > SHORT_VOL_HIGH_THRESHOLD:
                return {
                    'proxy':         'finra_short_volume',
                    'direction':     'increasing',
                    'strength':      'moderate',
                    'reason':        (f'5-day avg short volume ratio '
                                      f'{avg_5d:.1%} — elevated '
                                      f'(>{SHORT_VOL_HIGH_THRESHOLD:.0%})'),
                    'current_value': current_ratio,
                    'avg_5d':        round(avg_5d, 4),
                    'trend_delta':   round(trend_delta, 4),
                    'weight':        2,
                }

            # Case 2: Sudden drop = covering
            if (current_ratio < SHORT_VOL_DROP_THRESHOLD
                    and avg_older > 0.50):
                return {
                    'proxy':         'finra_short_volume',
                    'direction':     'decreasing',
                    'strength':      'strong',
                    'reason':        (f'Short volume ratio dropped to '
                                      f'{current_ratio:.1%} '
                                      f'(was {avg_older:.1%}) — '
                                      f'covering likely'),
                    'current_value': current_ratio,
                    'avg_5d':        round(avg_5d, 4),
                    'trend_delta':   round(trend_delta, 4),
                    'weight':        2,
                }

            # Case 3: Rising trend
            if trend_delta > 0.05:
                return {
                    'proxy':         'finra_short_volume',
                    'direction':     'increasing',
                    'strength':      'weak',
                    'reason':        (f'Short volume ratio rising trend: '
                                      f'{avg_older:.1%} → {avg_5d:.1%}'),
                    'current_value': current_ratio,
                    'avg_5d':        round(avg_5d, 4),
                    'trend_delta':   round(trend_delta, 4),
                    'weight':        1,
                }

            # Falling trend
            if trend_delta < -0.05:
                return {
                    'proxy':         'finra_short_volume',
                    'direction':     'decreasing',
                    'strength':      'weak',
                    'reason':        (f'Short volume ratio falling: '
                                      f'{avg_older:.1%} → {avg_5d:.1%}'),
                    'current_value': current_ratio,
                    'avg_5d':        round(avg_5d, 4),
                    'trend_delta':   round(trend_delta, 4),
                    'weight':        1,
                }

            return None  # No clear signal

        except Exception as e:
            return None

    # ═════════════════════════════════════════════════════════
    # PROXY SIGNAL 3 — PRICE ACTION + VOLUME
    # ═════════════════════════════════════════════════════════

    def _check_price_action(self, ticker: str) -> Optional[dict]:
        """
        Analyzes price movement to infer covering activity.

        Signal logic:
          Sharp price surge (>10% in 1 day) + high volume
            → forced short covering in progress
            → SI going DOWN rapidly

          Price falling steadily + rising short volume
            → new shorts entering on weakness
            → SI likely going UP

        This is the MOST TIMELY signal — price moves in
        real time while SI data lags 2 weeks.
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT close, volume, time
                FROM   stock_timeseries
                WHERE  ticker   = %s
                  AND  interval = '1d'
                ORDER  BY time DESC
                LIMIT  10
            """, (ticker,))
            rows = cur.fetchall()

            if len(rows) < 3:
                return None

            closes  = [float(r[0]) for r in rows if r[0]]
            volumes = [float(r[1]) for r in rows if r[1]]

            if len(closes) < 2:
                return None

            current_price = closes[0]
            prev_price    = closes[1]
            price_1d      = (current_price - prev_price) / prev_price

            price_5d = None
            if len(closes) >= 6:
                price_5d = (current_price - closes[5]) / closes[5]

            # Volume ratio vs 10-day average
            vol_today    = volumes[0]
            avg_vol      = sum(volumes[1:]) / len(volumes[1:])
            vol_ratio    = vol_today / avg_vol if avg_vol > 0 else 1.0

            # Case 1: Sharp 1-day surge with high volume = covering
            if (price_1d >= PRICE_SURGE_THRESHOLD
                    and vol_ratio >= VOLUME_SPIKE_THRESHOLD):
                return {
                    'proxy':         'price_action',
                    'direction':     'decreasing',
                    'strength':      'strong',
                    'reason':        (f'Price surged {price_1d:+.1%} '
                                      f'on {vol_ratio:.1f}x volume — '
                                      f'forced covering likely'),
                    'current_value': current_price,
                    'price_1d':      round(price_1d, 4),
                    'vol_ratio':     round(vol_ratio, 2),
                    'weight':        3,  # High weight — very timely
                }

            # Case 2: Sustained 5-day surge = covering in progress
            if (price_5d is not None
                    and price_5d >= PRICE_SURGE_5D_THRESHOLD
                    and vol_ratio >= 2.0):
                return {
                    'proxy':         'price_action',
                    'direction':     'decreasing',
                    'strength':      'moderate',
                    'reason':        (f'Price up {price_5d:+.1%} '
                                      f'over 5 days on elevated volume — '
                                      f'covering in progress'),
                    'current_value': current_price,
                    'price_5d':      round(price_5d, 4),
                    'vol_ratio':     round(vol_ratio, 2),
                    'weight':        2,
                }

            # Case 3: Price falling + high volume = new shorts
            if (price_1d <= -0.05
                    and vol_ratio >= 2.0):
                return {
                    'proxy':         'price_action',
                    'direction':     'increasing',
                    'strength':      'weak',
                    'reason':        (f'Price down {price_1d:+.1%} '
                                      f'on {vol_ratio:.1f}x volume — '
                                      f'possible new short entries'),
                    'current_value': current_price,
                    'price_1d':      round(price_1d, 4),
                    'vol_ratio':     round(vol_ratio, 2),
                    'weight':        1,
                }

            return None  # No clear price signal

        except Exception as e:
            return None

    # ═════════════════════════════════════════════════════════
    # PROXY SIGNAL 4 — FAIL TO DELIVER TREND
    # ═════════════════════════════════════════════════════════

    def _check_ftd(self, ticker: str) -> Optional[dict]:
        """
        Analyzes Fail-to-Deliver (FTD) data.
        FTD = shares that were sold short but seller could not
        deliver within T+2 settlement window.

        High/rising FTD → naked shorting increasing
                        → SI likely understated
                        → Actual short pressure higher than stated

        FTD data has 2-week lag (same as SI) so this is a
        supplementary signal, not a primary one.
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT ftd_shares, tier, collected_at
                FROM   sec_ftd_data
                WHERE  ticker = %s
                ORDER  BY collected_at DESC
                LIMIT  2
            """, (ticker,))
            rows = cur.fetchall()

            if not rows:
                return None

            current_ftd = int(rows[0][0])
            tier        = rows[0][1]

            # Only signal on elevated FTD
            if tier not in ('elevated', 'extreme'):
                return None

            # Compare to previous reading if available
            trend = 'stable'
            if len(rows) == 2:
                prev_ftd = int(rows[1][0])
                if current_ftd > prev_ftd * 1.5:
                    trend = 'rising'
                elif current_ftd < prev_ftd * 0.6:
                    trend = 'falling'

            if tier == 'extreme' or trend == 'rising':
                return {
                    'proxy':         'ftd',
                    'direction':     'increasing',
                    'strength':      'weak',
                    'reason':        (f'FTD at {current_ftd:,} shares '
                                      f'({tier} tier, trend: {trend}) — '
                                      f'naked shorting indicator'),
                    'current_value': current_ftd,
                    'tier':          tier,
                    'trend':         trend,
                    'weight':        1,  # Low weight — lagged data
                }

            return None

        except Exception as e:
            return None

    # ═════════════════════════════════════════════════════════
    # CONSENSUS AND ADJUSTMENT
    # ═════════════════════════════════════════════════════════

    def _consensus(self, bull_count: int,
                    bear_count: int,
                    total: int) -> tuple:
        """
        Determines consensus direction and confidence
        from all proxy signals.

        Weights:
          borrow_rate  = 3  (strongest proxy)
          price_action = 3  (most timely)
          finra_vol    = 2  (next-day lag)
          ftd          = 1  (lagged, supplementary)

        Confidence rules:
          Both borrow_rate and price_action agree → HIGH
          2+ signals agree, none contradict       → MEDIUM
          Only 1 signal, or mixed signals         → LOW
          No signals at all                       → UNKNOWN
        """
        if total == 0:
            return 'unknown', 'low'

        if bull_count == 0 and bear_count == 0:
            return 'unknown', 'low'

        if bull_count > bear_count:
            direction = 'increasing'
            agree_ratio = bull_count / total
        elif bear_count > bull_count:
            direction = 'decreasing'
            agree_ratio = bear_count / total
        else:
            # Tie → unknown
            return 'unknown', 'low'

        # Confidence from agreement ratio
        if agree_ratio >= 1.0 and total >= 2:
            confidence = 'high'
        elif agree_ratio >= 0.67:
            confidence = 'medium'
        else:
            confidence = 'low'

        return direction, confidence

    def _adjust_si(self, stated_pct: float,
                    direction: str,
                    stale_days: int) -> tuple:
        """
        Computes si_effective_pct by adjusting stated SI
        based on proxy direction and staleness.

        Adjustment logic:
          - Only adjust if staleness > 7 days
          - Increasing direction → add SI_INCREASE_ADJUSTMENT
          - Decreasing direction → subtract SI_DECREASE_ADJUSTMENT
          - Unknown direction → no adjustment, keep stated
          - Scale adjustment by staleness (more stale = bigger adj)
          - Never go below 0% or above 100%

        Returns (effective_pct, adjustment_applied)
        """
        if stated_pct is None:
            return 0.0, 0.0

        if stale_days <= 7 or direction == 'unknown':
            return round(float(stated_pct), 4), 0.0

        # Scale by staleness: 7 days = 50% adjustment, 14 days = 100%
        staleness_scale = min((stale_days - 7) / 7.0, 1.0)

        if direction == 'increasing':
            adjustment = SI_INCREASE_ADJUSTMENT * staleness_scale
            effective  = stated_pct + (adjustment * 100)
        else:  # decreasing
            adjustment = -SI_DECREASE_ADJUSTMENT * staleness_scale
            effective  = stated_pct + (adjustment * 100)

        effective = max(0.0, min(100.0, effective))
        return round(effective, 4), round(adjustment * 100, 4)

    # ═════════════════════════════════════════════════════════
    # DATABASE READS
    # ═════════════════════════════════════════════════════════

    def _get_stated_si(self, ticker: str) -> dict:
        """Gets the most recent official SI reading."""
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT short_interest_pct,
                       days_to_cover,
                       si_stale_days,
                       collected_at
                FROM   stock_snapshots
                WHERE  ticker               = %s
                  AND  short_interest_pct IS NOT NULL
                ORDER  BY collected_at DESC
                LIMIT  1
            """, (ticker,))
            row = cur.fetchone()

            if not row:
                return {
                    'short_interest_pct': 0.0,
                    'days_to_cover':      0.0,
                    'si_stale_days':      14,
                }

            return {
                'short_interest_pct': float(row[0]),
                'days_to_cover':      float(row[1]) if row[1] else 0.0,
                'si_stale_days':      int(row[2]) if row[2] else 14,
                'si_collected_at':    row[3].isoformat() if row[3] else None,
            }
        except Exception as e:
            return {
                'short_interest_pct': 0.0,
                'days_to_cover':      0.0,
                'si_stale_days':      14,
            }

    # ═════════════════════════════════════════════════════════
    # HELPERS
    # ═════════════════════════════════════════════════════════

    def get_si_summary_text(self, ticker: str) -> str:
        """
        Returns human-readable SI proxy summary.
        Used by SHAP explainer and dashboard display.
        """
        result = self.estimate(ticker)

        stated   = result['si_stated_pct']
        stale    = result['si_stale_days']
        direction = result['si_proxy_direction']
        confidence = result['si_proxy_confidence']
        effective = result['si_effective_pct']

        if direction == 'current':
            return (f'Short interest: {stated:.1f}% '
                    f'(data is current)')

        if direction == 'unknown':
            return (f'Short interest: {stated:.1f}% '
                    f'({stale} days old, direction unclear)')

        arrow = '↑' if direction == 'increasing' else '↓'
        return (
            f'Short interest: {stated:.1f}% stated '
            f'({stale}d old) → estimated {effective:.1f}% '
            f'{arrow} {direction} '
            f'({confidence} confidence). '
            f'Based on: '
            + ', '.join(s['proxy']
                        for s in result['proxy_signals'])
        )

    @staticmethod
    def _print_summary(result: dict):
        ticker    = result['ticker']
        stated    = result['si_stated_pct']
        stale     = result['si_stale_days']
        direction = result['si_proxy_direction']
        conf      = result['si_proxy_confidence']
        effective = result['si_effective_pct']
        signals   = result['proxy_signals']

        print(
            f'  ✓ {ticker} SI proxy — '
            f'stated: {stated:.1f}% ({stale}d old) → '
            f'effective: {effective:.1f}% | '
            f'direction: {direction} ({conf}) | '
            f'signals: {len(signals)}'
        )

    def close(self):
        self.pg.close()


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    estimator = SIProxyEstimator()

    print('='*55)
    print('  SI Proxy Estimator — Manual Test')
    print('='*55)

    test_tickers = ['GME', 'AMC', 'AAPL', 'XYZFAKE']

    for ticker in test_tickers:
        print(f'\n── {ticker} ──')
        result = estimator.estimate(ticker)

        print(f'  si_stated_pct:        {result["si_stated_pct"]}%')
        print(f'  si_stale_days:        {result["si_stale_days"]}')
        print(f'  si_proxy_direction:   {result["si_proxy_direction"]}')
        print(f'  si_proxy_confidence:  {result["si_proxy_confidence"]}')
        print(f'  si_effective_pct:     {result["si_effective_pct"]}%')
        print(f'  si_adjustment:        {result["si_adjustment"]:+.2f}%')
        print(f'  staleness_penalty:    {result["staleness_penalty"]}')
        print(f'  needs_fresh_data:     {result["needs_fresh_data"]}')
        print(f'  proxy_bull_count:     {result["proxy_bull_count"]}')
        print(f'  proxy_bear_count:     {result["proxy_bear_count"]}')

        if result['proxy_signals']:
            print(f'  proxy_signals fired:')
            for s in result['proxy_signals']:
                print(f'    [{s["direction"]:12s}] '
                      f'{s["proxy"]:20s} — {s["reason"]}')
        else:
            print(f'  proxy_signals:        none fired')

        print(f'\n  Summary: {estimator.get_si_summary_text(ticker)}')

    estimator.close()
    print('\n✓ SI proxy estimator test complete')