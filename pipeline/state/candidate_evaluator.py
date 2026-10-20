# pipeline/state/candidate_evaluator.py
#
# Candidate Evaluator
#
# Decides when a CANDIDATE stock is ready to become ACTIVE.
# Runs every 30 minutes via Celery beat scheduler.
#
# Two gates must both pass before promotion:
#
# GATE 1 — Data Sufficiency (can we score it at all?)
#   Checks that enough raw data exists to produce a meaningful
#   ML feature vector. Without this gate, the model would score
#   stocks on mostly-zeroed feature vectors and produce garbage.
#
#   Checks:
#     □ At least 2 days of OHLCV in stock_timeseries
#     □ At least 1 stock_snapshot with SI or borrow rate
#     □ At least 1 options snapshot (or options_eligible=False)
#     □ No more than 3 consecutive collection failures
#
# GATE 2 — Signal Threshold (is it worth monitoring?)
#   Checks that at least 2 of 4 signal categories are triggered.
#   A stock that passes Gate 1 but has no signals is just wasting
#   compute resources — we don't need to score it every 5 minutes.
#
#   Signal categories (must meet ANY 2):
#     Market structure: SI > 20% OR DTC > 5 OR borrow rate > 25%
#     Momentum:         Volume > 3x avg OR price up > 10% in 5 days
#     Social:           StockTwits bull > 65% OR trends spike detected
#     Options:          C/P ratio > 2.5 OR IV rank > 60%
#
# Fast-track override:
#   If a CANDIDATE suddenly gets a viral social signal
#   (StockTwits message count spike), skip Gate 2 and
#   promote immediately. Gate 1 still required.
#
# Storage:
#   Reads:  PostgreSQL (snapshots, timeseries, collection_log)
#   Writes: PostgreSQL stocks table (via StockStateManager)
#           PostgreSQL stock_events (transition log)

import os
import psycopg2
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

from pipeline.state.stock_state_manager import StockStateManager

# ─────────────────────────────────────────────────────────────
# GATE 2 SIGNAL THRESHOLDS
# Tune these to control how many stocks become ACTIVE
# Lower thresholds → more ACTIVE stocks → more compute
# Higher thresholds → fewer ACTIVE stocks → higher precision
# ─────────────────────────────────────────────────────────────
THRESHOLDS = {
    # Market structure signals
    'min_short_interest':    20.0,   # SI > 20% (stronger than screen's 15%)
    'min_days_to_cover':      5.0,   # DTC > 5 days
    'min_borrow_rate':        25.0,  # Borrow rate > 25% annualized

    # Momentum signals
    'min_volume_ratio':        3.0,  # Today's vol > 3x 30d average
    'min_price_change_5d':     0.10, # Price up > 10% in 5 days

    # Social signals
    'min_stocktwits_bull':     0.65, # > 65% bullish on StockTwits
    'trend_spike_required':    True, # Google Trends spike detected

    # Options signals
    'min_call_put_ratio':      2.5,  # C/P ratio > 2.5
    'min_iv_rank':             60.0, # IV rank > 60%

    # Fast-track threshold
    'fasttrack_message_spike': 100,  # StockTwits msgs > 100 in latest snapshot
}

# Gate 1 requirements
GATE1_MIN_OHLCV_DAYS   = 2    # Minimum trading days of OHLCV data
GATE1_MAX_FAIL_STREAK  = 3    # Max consecutive collection failures
GATE1_MIN_SIGNALS      = 2    # Min signal categories for Gate 2


class CandidateEvaluator:
    """
    Evaluates CANDIDATE stocks and promotes them to ACTIVE
    when they meet data sufficiency and signal threshold criteria.

    Runs every 30 minutes via Celery beat.
    Each run processes ALL stocks in CANDIDATE status.

    Usage:
        evaluator = CandidateEvaluator()

        # Evaluate one stock
        result = evaluator.evaluate('GME')
        # result['decision'] = 'PROMOTE' / 'STAY' / 'DEMOTE'

        # Evaluate all candidates (production use)
        summary = evaluator.evaluate_all_candidates()
    """

    def __init__(self):
        self.pg      = psycopg2.connect(os.getenv('POSTGRES_URL'))
        self.manager = StockStateManager()

    # ═════════════════════════════════════════════════════════
    # MAIN EVALUATION METHODS
    # ═════════════════════════════════════════════════════════

    def evaluate(self, ticker: str) -> dict:
        """
        Evaluates a single CANDIDATE stock.
        Returns a result dict with the decision and full reasoning.

        Returns dict with:
          ticker        — stock symbol
          decision      — 'PROMOTE' / 'STAY' / 'DEMOTE'
          gate1         — Gate 1 result dict
          gate2         — Gate 2 result dict
          fast_tracked  — bool: bypassed Gate 2?
          reason        — human-readable decision explanation
          evaluated_at  — timestamp
        """
        ticker = ticker.upper().strip()

        result = {
            'ticker':       ticker,
            'decision':     'STAY',
            'gate1':        {},
            'gate2':        {},
            'fast_tracked': False,
            'reason':       '',
            'evaluated_at': datetime.utcnow(),
        }

        # ── Gate 1: Data Sufficiency ──────────────────────
        gate1 = self.check_gate1(ticker)
        result['gate1'] = gate1

        if not gate1['passed']:
            result['decision'] = 'STAY'
            result['reason']   = (
                f'Gate 1 failed: {gate1["failed_checks"]}')
            return result

        # ── Fast track check ──────────────────────────────
        # High StockTwits activity = bypass Gate 2
        if self._check_fast_track(ticker):
            result['fast_tracked'] = True
            result['decision']     = 'PROMOTE'
            result['reason']       = (
                'Fast-tracked: sudden social spike detected. '
                'Gate 2 bypassed.')
            self._promote(ticker, 'fast_track_social_spike')
            return result

        # ── Gate 2: Signal Threshold ──────────────────────
        gate2 = self.check_gate2(ticker)
        result['gate2'] = gate2

        if gate2['signals_met'] >= GATE1_MIN_SIGNALS:
            result['decision'] = 'PROMOTE'
            result['reason']   = (
                f'Gate 1 + Gate 2 passed. '
                f'{gate2["signals_met"]} signal categories triggered: '
                f'{gate2["triggered_categories"]}')
            self._promote(ticker, 'gate1_and_gate2_passed')

        elif not gate2['has_any_signal']:
            # No signals at all after 7+ days → demote
            days_as_candidate = self._days_as_candidate(ticker)
            if days_as_candidate >= 7:
                result['decision'] = 'DEMOTE'
                result['reason']   = (
                    f'No signals after {days_as_candidate} days '
                    f'as CANDIDATE → returning to UNTRACKED')
                self._demote(ticker)
            else:
                result['decision'] = 'STAY'
                result['reason']   = (
                    f'Gate 1 passed, Gate 2 not met yet. '
                    f'{days_as_candidate} days as candidate. '
                    f'Watching...')
        else:
            result['decision'] = 'STAY'
            result['reason']   = (
                f'Gate 1 passed. Gate 2: only '
                f'{gate2["signals_met"]}/2 categories triggered '
                f'({gate2["triggered_categories"]}). '
                f'Need 2 to promote.')

        return result

    def evaluate_all_candidates(self) -> dict:
        """
        Evaluates every stock in CANDIDATE status.
        Called by Celery beat every 30 minutes.

        Returns summary dict with counts and details.
        """
        candidates = self.manager.get_stocks_by_status('CANDIDATE')

        if not candidates:
            return {
                'candidates_evaluated': 0,
                'promoted':             0,
                'demoted':              0,
                'staying':              0,
                'evaluated_at':         datetime.utcnow(),
            }

        print(f'\n[CandidateEvaluator] Evaluating '
              f'{len(candidates)} candidates...')

        promoted  = []
        demoted   = []
        staying   = []
        details   = []

        for ticker in candidates:
            try:
                result = self.evaluate(ticker)
                details.append(result)

                if result['decision'] == 'PROMOTE':
                    promoted.append(ticker)
                    print(f'  ✓ PROMOTE: {ticker} — '
                          f'{result["reason"]}')
                elif result['decision'] == 'DEMOTE':
                    demoted.append(ticker)
                    print(f'  ✗ DEMOTE: {ticker} — '
                          f'{result["reason"]}')
                else:
                    staying.append(ticker)

            except Exception as e:
                print(f'  ! ERROR evaluating {ticker}: {e}')
                staying.append(ticker)

        summary = {
            'candidates_evaluated': len(candidates),
            'promoted':             len(promoted),
            'demoted':              len(demoted),
            'staying':              len(staying),
            'promoted_tickers':     promoted,
            'demoted_tickers':      demoted,
            'details':              details,
            'evaluated_at':         datetime.utcnow(),
        }

        print(f'\n[CandidateEvaluator] Summary: '
              f'{len(promoted)} promoted, '
              f'{len(demoted)} demoted, '
              f'{len(staying)} staying')

        return summary

    # ═════════════════════════════════════════════════════════
    # GATE 1 — DATA SUFFICIENCY
    # ═════════════════════════════════════════════════════════

    def check_gate1(self, ticker: str) -> dict:
        """
        Gate 1: Verifies enough data exists to score this stock.

        Checks:
          1. OHLCV data: at least 2 trading days in stock_timeseries
          2. Snapshot data: at least 1 row in stock_snapshots
          3. Collection health: no failure streak >= 3
          4. Options: snapshot exists OR confirmed ineligible

        Returns dict with:
          passed         — bool
          checks         — dict of individual check results
          failed_checks  — list of check names that failed
        """
        checks  = {}
        failed  = []

        # ── Check 1: OHLCV data ──────────────────────────
        ohlcv_days = self._count_ohlcv_days(ticker)
        checks['ohlcv_days'] = {
            'value':    ohlcv_days,
            'required': GATE1_MIN_OHLCV_DAYS,
            'passed':   ohlcv_days >= GATE1_MIN_OHLCV_DAYS,
        }
        if not checks['ohlcv_days']['passed']:
            failed.append(
                f'ohlcv_days={ohlcv_days} '
                f'(need {GATE1_MIN_OHLCV_DAYS})')

        # ── Check 2: Snapshot data ───────────────────────
        has_snapshot = self._has_snapshot(ticker)
        checks['has_snapshot'] = {
            'value':  has_snapshot,
            'passed': has_snapshot,
        }
        if not has_snapshot:
            failed.append('no_snapshot_data')

        # ── Check 3: Collection health ───────────────────
        fail_streak = self._get_failure_streak(ticker)
        checks['collection_health'] = {
            'value':    fail_streak,
            'required': f'< {GATE1_MAX_FAIL_STREAK}',
            'passed':   fail_streak < GATE1_MAX_FAIL_STREAK,
        }
        if not checks['collection_health']['passed']:
            failed.append(
                f'collection_failures={fail_streak} '
                f'(max {GATE1_MAX_FAIL_STREAK})')

        # ── Check 4: Options data or confirmed ineligible ─
        options_ok = self._check_options_data(ticker)
        checks['options_data'] = {
            'value':  options_ok,
            'passed': options_ok,
        }
        if not options_ok:
            failed.append('no_options_data_yet')

        passed = len(failed) == 0

        return {
            'passed':        passed,
            'checks':        checks,
            'failed_checks': failed,
            'ohlcv_days':    ohlcv_days,
        }

    # ═════════════════════════════════════════════════════════
    # GATE 2 — SIGNAL THRESHOLD
    # ═════════════════════════════════════════════════════════

    def check_gate2(self, ticker: str) -> dict:
        """
        Gate 2: Checks if the stock has enough signal strength
        to justify active monitoring and ML scoring.

        Evaluates 4 signal categories.
        Needs any 2 categories to trigger → PROMOTE.

        Returns dict with:
          signals_met           — int (0-4)
          triggered_categories  — list of category names
          has_any_signal        — bool
          category_results      — detailed results per category
        """
        categories = {}

        # ── Category 1: Market Structure ──────────────────
        categories['market_structure'] = \
            self._check_market_structure(ticker)

        # ── Category 2: Momentum ──────────────────────────
        categories['momentum'] = \
            self._check_momentum(ticker)

        # ── Category 3: Social ────────────────────────────
        categories['social'] = \
            self._check_social(ticker)

        # ── Category 4: Options ───────────────────────────
        categories['options'] = \
            self._check_options_signals(ticker)

        # Tally
        triggered = [
            cat for cat, result in categories.items()
            if result['triggered']
        ]
        signals_met = len(triggered)

        return {
            'signals_met':          signals_met,
            'triggered_categories': triggered,
            'has_any_signal':       signals_met > 0,
            'category_results':     categories,
            'needs':                GATE1_MIN_SIGNALS,
        }

    # ─────────────────────────────────────────────────────────
    # Gate 2 — Category Checks
    # ─────────────────────────────────────────────────────────

    def _check_market_structure(self, ticker: str) -> dict:
        """
        Market structure: SI > 20% OR DTC > 5 OR borrow > 25%
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT short_interest_pct,
                       days_to_cover,
                       borrow_rate
                FROM   stock_snapshots
                WHERE  ticker = %s
                ORDER  BY collected_at DESC
                LIMIT  1
            """, (ticker,))
            row = cur.fetchone()

            if not row:
                return {
                    'triggered': False,
                    'reason':    'no snapshot data',
                    'values':    {},
                }

            si    = float(row[0]) if row[0] else 0.0
            dtc   = float(row[1]) if row[1] else 0.0
            brate = float(row[2]) if row[2] else 0.0

            triggers = []
            if si >= THRESHOLDS['min_short_interest']:
                triggers.append(
                    f'SI={si:.1f}% '
                    f'(>{THRESHOLDS["min_short_interest"]}%)')
            if dtc >= THRESHOLDS['min_days_to_cover']:
                triggers.append(
                    f'DTC={dtc:.1f} '
                    f'(>{THRESHOLDS["min_days_to_cover"]})')
            if brate >= THRESHOLDS['min_borrow_rate']:
                triggers.append(
                    f'borrow={brate:.1f}% '
                    f'(>{THRESHOLDS["min_borrow_rate"]}%)')

            return {
                'triggered': len(triggers) > 0,
                'reason':    ', '.join(triggers) if triggers
                             else 'SI/DTC/borrow below thresholds',
                'values': {
                    'short_interest_pct': si,
                    'days_to_cover':      dtc,
                    'borrow_rate':        brate,
                },
            }

        except Exception as e:
            return {
                'triggered': False,
                'reason':    f'error: {e}',
                'values':    {},
            }

    def _check_momentum(self, ticker: str) -> dict:
        """
        Momentum: Volume > 3x avg OR price up > 10% in 5 days
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT close, volume, time
                FROM   stock_timeseries
                WHERE  ticker   = %s
                  AND  interval = '1d'
                ORDER  BY time DESC
                LIMIT  30
            """, (ticker,))
            rows = cur.fetchall()

            if len(rows) < 3:
                return {
                    'triggered': False,
                    'reason':    'insufficient OHLCV history',
                    'values':    {},
                }

            closes  = [float(r[0]) for r in rows if r[0]]
            volumes = [float(r[1]) for r in rows if r[1]]

            triggers = []

            # Volume ratio check
            if len(volumes) >= 5:
                vol_today = volumes[0]
                avg_vol   = sum(volumes[1:]) / len(volumes[1:])
                vol_ratio = vol_today / avg_vol if avg_vol > 0 else 0
                if vol_ratio >= THRESHOLDS['min_volume_ratio']:
                    triggers.append(
                        f'volume={vol_ratio:.1f}x avg '
                        f'(>{THRESHOLDS["min_volume_ratio"]}x)')

            # 5-day price change
            if len(closes) >= 6:
                price_5d = (closes[0] - closes[5]) / closes[5]
                if price_5d >= THRESHOLDS['min_price_change_5d']:
                    triggers.append(
                        f'price_5d={price_5d:+.1%} '
                        f'(>{THRESHOLDS["min_price_change_5d"]:.0%})')

            return {
                'triggered': len(triggers) > 0,
                'reason':    ', '.join(triggers) if triggers
                             else 'volume and price below thresholds',
                'values': {
                    'volume_ratio': vol_ratio if len(volumes) >= 5 else 0,
                    'price_5d':     price_5d  if len(closes) >= 6 else 0,
                },
            }

        except Exception as e:
            return {
                'triggered': False,
                'reason':    f'error: {e}',
                'values':    {},
            }

    def _check_social(self, ticker: str) -> dict:
        """
        Social: StockTwits bull > 65% OR Google Trends spike
        """
        cur = self.pg.cursor()
        triggers = []
        values   = {}

        # StockTwits check
        try:
            cur.execute("""
                SELECT bull_ratio, message_count
                FROM   stocktwits_snapshots
                WHERE  ticker = %s
                ORDER  BY collected_at DESC
                LIMIT  1
            """, (ticker,))
            row = cur.fetchone()
            if row:
                bull_ratio = float(row[0]) if row[0] else 0.0
                msg_count  = int(row[1])   if row[1] else 0
                values['stocktwits_bull_ratio']  = bull_ratio
                values['stocktwits_msg_count']   = msg_count

                if bull_ratio >= THRESHOLDS['min_stocktwits_bull']:
                    triggers.append(
                        f'StockTwits bull={bull_ratio:.0%} '
                        f'(>{THRESHOLDS["min_stocktwits_bull"]:.0%})')
        except Exception:
            pass

        # Google Trends spike check
        try:
            cur.execute("""
                SELECT trend_score, trend_spike
                FROM   trends_snapshots
                WHERE  ticker = %s
                ORDER  BY collected_at DESC
                LIMIT  1
            """, (ticker,))
            row = cur.fetchone()
            if row:
                trend_score = float(row[0]) if row[0] else 0.0
                trend_spike = bool(row[1])
                values['google_trend_score'] = trend_score
                values['trend_spike']        = trend_spike

                if trend_spike:
                    triggers.append(
                        f'Google Trends spike '
                        f'(score={trend_score:.0f})')
        except Exception:
            pass

        return {
            'triggered': len(triggers) > 0,
            'reason':    ', '.join(triggers) if triggers
                         else 'StockTwits/Trends below thresholds',
            'values':    values,
        }

    def _check_options_signals(self, ticker: str) -> dict:
        """
        Options: C/P ratio > 2.5 OR IV rank > 60%
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT call_put_ratio, iv_rank,
                       options_eligible
                FROM   options_snapshots
                WHERE  ticker = %s
                ORDER  BY collected_at DESC
                LIMIT  1
            """, (ticker,))
            row = cur.fetchone()

            if not row:
                return {
                    'triggered': False,
                    'reason':    'no options data yet',
                    'values':    {},
                }

            cp_ratio       = float(row[0]) if row[0] else 0.0
            iv_rank        = float(row[1]) if row[1] else 0.0
            opts_eligible  = bool(row[2]) if row[2] is not None else True

            if not opts_eligible:
                return {
                    'triggered': False,
                    'reason':    'stock has no options',
                    'values':    {'options_eligible': False},
                }

            triggers = []
            if cp_ratio >= THRESHOLDS['min_call_put_ratio']:
                triggers.append(
                    f'C/P ratio={cp_ratio:.2f} '
                    f'(>{THRESHOLDS["min_call_put_ratio"]})')
            if iv_rank >= THRESHOLDS['min_iv_rank']:
                triggers.append(
                    f'IV rank={iv_rank:.0f}% '
                    f'(>{THRESHOLDS["min_iv_rank"]}%)')

            return {
                'triggered': len(triggers) > 0,
                'reason':    ', '.join(triggers) if triggers
                             else 'C/P ratio and IV rank below thresholds',
                'values': {
                    'call_put_ratio': cp_ratio,
                    'iv_rank':        iv_rank,
                },
            }

        except Exception as e:
            return {
                'triggered': False,
                'reason':    f'error: {e}',
                'values':    {},
            }

    # ═════════════════════════════════════════════════════════
    # FAST TRACK CHECK
    # ═════════════════════════════════════════════════════════

    def _check_fast_track(self, ticker: str) -> bool:
        """
        Returns True if social spike warrants immediate promotion.
        Currently checks StockTwits message count spike.
        When Reddit is available: add mention count check here.
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT message_count
                FROM   stocktwits_snapshots
                WHERE  ticker = %s
                ORDER  BY collected_at DESC
                LIMIT  1
            """, (ticker,))
            row = cur.fetchone()
            if row and row[0]:
                return int(row[0]) >= \
                       THRESHOLDS['fasttrack_message_spike']
            return False
        except Exception:
            return False

    # ═════════════════════════════════════════════════════════
    # STATUS TRANSITIONS
    # ═════════════════════════════════════════════════════════

    def _promote(self, ticker: str, reason: str):
        """Promotes CANDIDATE → ACTIVE via state manager."""
        self.manager.transition_status(
            ticker, 'ACTIVE', reason)

    def _demote(self, ticker: str):
        """Demotes CANDIDATE → UNTRACKED for low-signal stocks."""
        self.manager.transition_status(
            ticker, 'UNTRACKED',
            'no_signals_after_7_days_as_candidate')

    # ═════════════════════════════════════════════════════════
    # GATE 1 HELPER QUERIES
    # ═════════════════════════════════════════════════════════

    def _count_ohlcv_days(self, ticker: str) -> int:
        """Counts distinct trading days in stock_timeseries."""
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT COUNT(DISTINCT DATE(time))
                FROM   stock_timeseries
                WHERE  ticker   = %s
                  AND  interval = '1d'
                  AND  close    IS NOT NULL
            """, (ticker,))
            row = cur.fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0

    def _has_snapshot(self, ticker: str) -> bool:
        """Checks if at least one stock_snapshot exists."""
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT COUNT(*) FROM stock_snapshots
                WHERE ticker = %s
            """, (ticker,))
            return cur.fetchone()[0] > 0
        except Exception:
            return False

    def _get_failure_streak(self, ticker: str) -> int:
        """
        Returns the number of consecutive recent collection
        failures for a ticker across all sources.
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT status
                FROM   collection_log
                WHERE  ticker = %s
                ORDER  BY collected_at DESC
                LIMIT  10
            """, (ticker,))
            rows = cur.fetchall()
            if not rows:
                return 0

            streak = 0
            for row in rows:
                if row[0] == 'failed':
                    streak += 1
                else:
                    break
            return streak
        except Exception:
            return 0

    def _check_options_data(self, ticker: str) -> bool:
        """
        Returns True if:
          - An options snapshot exists, OR
          - Stock is confirmed options_ineligible
        Returns False if neither (data not collected yet).
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT COUNT(*) FROM options_snapshots
                WHERE ticker = %s
            """, (ticker,))
            if cur.fetchone()[0] > 0:
                return True

            # Check if confirmed ineligible in stocks table
            cur.execute("""
                SELECT options_eligible FROM stocks
                WHERE ticker = %s
            """, (ticker,))
            row = cur.fetchone()
            if row and row[0] is False:
                return True  # Confirmed no options — that's fine

            return False
        except Exception:
            return True  # On error, don't block promotion

    def _days_as_candidate(self, ticker: str) -> int:
        """Returns how many days this stock has been CANDIDATE."""
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT promoted_at FROM stocks
                WHERE ticker = %s
            """, (ticker,))
            row = cur.fetchone()
            if row and row[0]:
                delta = datetime.utcnow() - row[0].replace(
                    tzinfo=None)
                return delta.days
            return 0
        except Exception:
            return 0

    def close(self):
        self.pg.close()
        self.manager.close()


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    evaluator = CandidateEvaluator()

    print('='*55)
    print('  Candidate Evaluator — Manual Test')
    print('='*55)

    # Ensure test stocks exist as CANDIDATE
    pg  = psycopg2.connect(os.getenv('POSTGRES_URL'))
    cur = pg.cursor()

    test_stocks = [
        ('GME',     True),   # (ticker, has_data)
        ('AMC',     True),
        ('XYZFAKE', False),  # No data — should stay CANDIDATE
    ]

    for ticker, _ in test_stocks:
        cur.execute("""
            INSERT INTO stocks (ticker, status, promoted_at)
            VALUES (%s, 'CANDIDATE', NOW() - INTERVAL '3 days')
            ON CONFLICT (ticker) DO UPDATE
              SET status = 'CANDIDATE'
        """, (ticker,))
    pg.commit()
    pg.close()

    # Test individual evaluation
    print('\n[1] Individual stock evaluation:')
    for ticker, _ in test_stocks:
        print(f'\n── {ticker} ──')
        result = evaluator.evaluate(ticker)

        print(f'  decision:    {result["decision"]}')
        print(f'  reason:      {result["reason"]}')
        print(f'  fast_tracked:{result["fast_tracked"]}')

        g1 = result['gate1']
        print(f'\n  Gate 1 (data sufficiency): '
              f'{"PASSED ✓" if g1["passed"] else "FAILED ✗"}')
        for check, details in g1.get('checks', {}).items():
            status = '✓' if details['passed'] else '✗'
            print(f'    {status} {check}: {details["value"]}')

        g2 = result['gate2']
        if g2:
            print(f'\n  Gate 2 (signal threshold): '
                  f'{g2["signals_met"]}/2 categories triggered')
            for cat, details in g2.get(
                    'category_results', {}).items():
                status = '✓' if details['triggered'] else '✗'
                print(f'    {status} {cat}: '
                      f'{details["reason"]}')

    # Test evaluate_all_candidates
    print('\n[2] Evaluate all candidates:')
    summary = evaluator.evaluate_all_candidates()
    print(f'  evaluated:  {summary["candidates_evaluated"]}')
    print(f'  promoted:   {summary["promoted"]}')
    print(f'  demoted:    {summary["demoted"]}')
    print(f'  staying:    {summary["staying"]}')

    # Test Gate 1 directly
    print('\n[3] Gate 1 direct check (GME):')
    g1 = evaluator.check_gate1('GME')
    print(f'  passed: {g1["passed"]}')
    print(f'  ohlcv_days: {g1["ohlcv_days"]}')
    if g1['failed_checks']:
        print(f'  failed: {g1["failed_checks"]}')

    # Test Gate 2 directly
    print('\n[4] Gate 2 direct check (GME):')
    g2 = evaluator.check_gate2('GME')
    print(f'  signals_met: {g2["signals_met"]}')
    print(f'  triggered: {g2["triggered_categories"]}')
    for cat, details in g2['category_results'].items():
        status = '✓' if details['triggered'] else '✗'
        print(f'  {status} {cat}: {details["reason"]}')

    evaluator.close()
    print('\n✓ Candidate evaluator test complete')