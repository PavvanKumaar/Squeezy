# pipeline/screener/universe_screener.py
#
# Universe Screener — runs daily at 9:31 AM ET
#
# What it does:
#   1. Pulls all US equities (~8,000 stocks)
#   2. Applies three filters:
#        - Float < 200M shares
#        - Short interest > 15% of float
#        - Average daily volume > 200K shares
#   3. Compares result against current DB state
#   4. Promotes new tickers to CANDIDATE
#   5. Moves failing ACTIVE stocks to COOLING
#
# Filters explained:
#   Float < 200M  → small float = easier to move price with volume
#   SI > 15%      → meaningful short fuel exists
#   Vol > 200K    → enough liquidity to actually trade
#
# Data source: Finviz screener (free, no API key)
#   via finvizfinance library
#   Staleness: 15-30 min during market hours
#
# Fallback source: yfinance info fields
#   Used to cross-check float size (Finviz sometimes wrong)
#
# Storage: PostgreSQL stocks table (status field)

import os
import time
import psycopg2
import yfinance as yf
import requests
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# Screening filters — adjust these to tune candidate count
FILTERS = {
    'max_float_shares':    200_000_000,   # 200M shares
    'min_short_interest':  15.0,          # 15% of float
    'min_avg_volume':      200_000,       # 200K shares/day
}

# How to get the initial universe:
# Primary:  finvizfinance screener (fast, covers most stocks)
# Fallback: manual yfinance pull for known high-SI stocks

try:
    from finvizfinance.screener.overview import Overview
    FINVIZ_AVAILABLE = True
except ImportError:
    FINVIZ_AVAILABLE = False
    print('[screener] finvizfinance not installed. '
          'pip install finvizfinance')


def make_yf_ticker(ticker: str) -> yf.Ticker:
    """Creates yfinance Ticker with headers to avoid empty responses."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        )
    })
    return yf.Ticker(ticker, session=session)


class UniverseScreener:
    """
    Daily universe screener.
    Determines which stocks enter the monitoring pipeline.

    Run schedule: daily at 9:31 AM ET (1 min after market open)
    via Celery beat scheduler.
    """

    def __init__(self):
        self.conn = psycopg2.connect(os.getenv('POSTGRES_URL'))
        self._ensure_tables()

    # ═════════════════════════════════════════════════════════
    # MAIN ENTRY POINT
    # ═════════════════════════════════════════════════════════

    def run(self) -> dict:
        """
        Runs the full screening cycle.
        Returns summary of all actions taken.

        Returns dict with:
          candidates_found   — tickers passing all filters
          promoted           — new tickers moved to CANDIDATE
          no_action          — already in pipeline
          moved_to_cooling   — ACTIVE stocks that failed screen
          archived           — COOLING stocks past grace period
          total_active       — current ACTIVE count after run
          run_at             — timestamp
        """
        print(f'\n{"="*55}')
        print(f'  Universe Screener — {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}')
        print(f'{"="*55}')

        # Step 1: Get candidates from screen
        candidates = self._get_candidates()
        print(f'\n  Screen output: {len(candidates)} tickers pass filters')

        # Step 2: Compare against DB state
        actions = self._compare_with_db(candidates)

        # Step 3: Execute actions
        promoted  = self._promote_candidates(
            actions['promote_to_candidate'])
        cooled    = self._move_to_cooling(
            actions['move_to_cooling'])
        archived  = self._archive_cooling_stocks()

        # Step 4: Log the run
        total_active = self._count_by_status('ACTIVE')
        total_cand   = self._count_by_status('CANDIDATE')

        result = {
            'candidates_found': len(candidates),
            'promoted':         promoted,
            'no_action':        len(actions['no_action']),
            'moved_to_cooling': cooled,
            'archived':         archived,
            'total_active':     total_active,
            'total_candidate':  total_cand,
            'candidate_tickers': actions['promote_to_candidate'],
            'cooling_tickers':   actions['move_to_cooling'],
            'run_at':           datetime.utcnow(),
        }

        self._log_run(result)
        self._print_summary(result)
        return result

    # ═════════════════════════════════════════════════════════
    # STEP 1 — GET CANDIDATES
    # ═════════════════════════════════════════════════════════

    def _get_candidates(self) -> list:
        """
        Pulls candidate tickers from Finviz screener.
        Falls back to a curated high-SI list if Finviz unavailable.
        Returns list of ticker strings.
        """
        if FINVIZ_AVAILABLE:
            tickers = self._screen_finviz()
        else:
            tickers = self._screen_fallback()

        # Cross-check float with yfinance
        # Finviz float data can be stale or wrong
        # Only check if we have a manageable number
        if len(tickers) <= 500:
            tickers = self._validate_floats(tickers)

        return tickers

    def _screen_finviz(self) -> list:
        """
        Uses Finviz screener to get stocks matching our criteria.

        Finviz filters applied:
          Short Float: Over 15%  → short interest > 15%
          Average Volume:        → > 200K shares/day
          Country: USA           → US equities only
          Market Cap: Small      → proxy for small float

        Float filter applied manually after (Finviz has no direct filter).
        """
        try:
            overview = Overview()
            filters  = {
                'Float Short':     'Over 15%',
                'Average Volume':  'Over 200K',
                'Country':         'USA',
            }
            overview.set_filter(filters_dict=filters)

            print('  Pulling Finviz screener...')
            df = overview.screener_view()

            if df is None or df.empty:
                print('  Finviz returned empty result')
                return self._screen_fallback()

            tickers = df['Ticker'].dropna().tolist()
            print(f'  Finviz returned {len(tickers)} tickers '
                  f'before float filter')
            return tickers

        except Exception as e:
            print(f'  Finviz error: {e}')
            return self._screen_fallback()

    def _screen_fallback(self) -> list:
        """
        Fallback screener when Finviz is unavailable.
        Uses a known set of high-short-interest stocks
        plus any currently ACTIVE/CANDIDATE stocks in DB
        (they already passed the screen at some point).
        """
        print('  Using fallback screener (Finviz unavailable)')

        # Get existing pipeline stocks (they already passed once)
        cur = self.conn.cursor()
        cur.execute("""
            SELECT ticker FROM stocks
            WHERE status IN ('ACTIVE', 'CANDIDATE', 'COOLING')
        """)
        existing = [r[0] for r in cur.fetchall()]

        # Known high-SI universe as bootstrap
        # (these are frequently shorted small caps)
        known_high_si = [
            'GME', 'AMC', 'BBBY', 'SPWR', 'CLOV', 'WISH',
            'MVIS', 'SENS', 'ATER', 'KOSS', 'EXPR', 'NAKD',
            'TLRY', 'ACB', 'SNDL', 'NKLA', 'RIDE', 'WKHS',
        ]

        return list(set(existing + known_high_si))

    def _validate_floats(self, tickers: list) -> list:
        """
        Cross-checks float size against yfinance.
        Filters out tickers with float > max_float_shares.
        Processes in batches to avoid rate limits.
        """
        print(f'  Validating floats for {len(tickers)} tickers...')
        valid    = []
        invalid  = []
        errors   = []

        max_float = FILTERS['max_float_shares']

        for i, ticker in enumerate(tickers):
            try:
                t    = make_yf_ticker(ticker)
                info = t.info
                float_shares = info.get('floatShares')

                if float_shares is None:
                    # Can't verify — include with warning
                    valid.append(ticker)
                elif float_shares <= max_float:
                    valid.append(ticker)
                else:
                    invalid.append(ticker)

                # Rate limit: small delay every 10 tickers
                if i > 0 and i % 10 == 0:
                    time.sleep(0.5)
                    print(f'    Validated {i}/{len(tickers)}...')

            except Exception as e:
                errors.append(ticker)
                # Include on error — don't exclude valid stocks
                valid.append(ticker)

        print(f'  Float validation: {len(valid)} pass, '
              f'{len(invalid)} fail, {len(errors)} errors')
        return valid

    # ═════════════════════════════════════════════════════════
    # STEP 2 — COMPARE WITH DB
    # ═════════════════════════════════════════════════════════

    def _compare_with_db(self, candidates: list) -> dict:
        """
        Compares screen output against current DB state.
        Returns action dict telling what to do with each ticker.

        Logic:
          New ticker in candidates     → promote_to_candidate
          Already ACTIVE/CANDIDATE     → no_action (already in pipeline)
          ACTIVE but failed screen     → move_to_cooling
          COOLING (still in pipeline)  → no_action (grace period active)
          ARCHIVED + now passes screen → promote_to_candidate (re-enter)
        """
        cur = self.conn.cursor()
        cur.execute("""
            SELECT ticker, status FROM stocks
        """)
        existing = {r[0]: r[1] for r in cur.fetchall()}

        candidate_set = set(candidates)

        actions = {
            'promote_to_candidate': [],
            'move_to_cooling':      [],
            'no_action':            [],
        }

        # Check each ticker in screen output
        for ticker in candidate_set:
            status = existing.get(ticker)

            if status is None:
                # Brand new ticker — never seen before
                actions['promote_to_candidate'].append(ticker)

            elif status == 'UNTRACKED':
                # Known but never promoted
                actions['promote_to_candidate'].append(ticker)

            elif status == 'ARCHIVED':
                # Was retired, now passes screen again
                actions['promote_to_candidate'].append(ticker)

            elif status in ('CANDIDATE', 'ACTIVE', 'COOLING'):
                # Already in pipeline — leave it alone
                actions['no_action'].append(ticker)

        # Check ACTIVE stocks that are no longer in screen output
        for ticker, status in existing.items():
            if status == 'ACTIVE' and ticker not in candidate_set:
                actions['move_to_cooling'].append(ticker)

        return actions

    # ═════════════════════════════════════════════════════════
    # STEP 3 — EXECUTE ACTIONS
    # ═════════════════════════════════════════════════════════

    def _promote_candidates(self, tickers: list) -> int:
        """
        Promotes tickers to CANDIDATE status.
        Creates new stock records for tickers not yet in DB.
        """
        if not tickers:
            return 0

        cur = self.conn.cursor()
        promoted = 0

        for ticker in tickers:
            try:
                # Upsert — create if not exists, update if archived
                cur.execute("""
                    INSERT INTO stocks
                        (ticker, status, promoted_at, created_at)
                    VALUES (%s, 'CANDIDATE', NOW(), NOW())
                    ON CONFLICT (ticker) DO UPDATE
                        SET status      = 'CANDIDATE',
                            promoted_at = NOW(),
                            updated_at  = NOW()
                    WHERE stocks.status IN (
                        'UNTRACKED', 'ARCHIVED')
                """, (ticker,))

                # Log the promotion event
                cur.execute("""
                    INSERT INTO stock_events
                        (ticker, event_type, event_data, occurred_at)
                    VALUES (%s, %s, %s, NOW())
                """, (
                    ticker,
                    'status_change',
                    f'{{"from": "UNTRACKED", "to": "CANDIDATE", '
                    f'"reason": "passed_daily_screen"}}',
                ))

                promoted += 1

            except Exception as e:
                print(f'  Error promoting {ticker}: {e}')
                continue

        self.conn.commit()
        return promoted

    def _move_to_cooling(self, tickers: list) -> int:
        """
        Moves ACTIVE stocks that failed today's screen to COOLING.
        Does NOT immediately archive — gives 3-day grace period.

        Why grace period:
          A stock mid-squeeze may temporarily fail the SI filter
          because shorts are covering. Dropping it immediately
          means losing tracking at the most critical moment.
        """
        if not tickers:
            return 0

        cur   = self.conn.cursor()
        moved = 0

        for ticker in tickers:
            try:
                cur.execute("""
                    UPDATE stocks
                    SET    status    = 'COOLING',
                           cooled_at = NOW(),
                           updated_at = NOW()
                    WHERE  ticker = %s
                      AND  status = 'ACTIVE'
                """, (ticker,))

                cur.execute("""
                    INSERT INTO stock_events
                        (ticker, event_type, event_data, occurred_at)
                    VALUES (%s, %s, %s, NOW())
                """, (
                    ticker,
                    'status_change',
                    f'{{"from": "ACTIVE", "to": "COOLING", '
                    f'"reason": "failed_daily_screen"}}',
                ))

                moved += 1

            except Exception as e:
                print(f'  Error cooling {ticker}: {e}')

        self.conn.commit()
        return moved

    def _archive_cooling_stocks(self) -> int:
        """
        Archives COOLING stocks that have been cooling for 3+ days.
        Three days gives time for post-squeeze tracking to complete.
        """
        cur = self.conn.cursor()

        # Find stocks cooling for > 3 days
        cur.execute("""
            SELECT ticker FROM stocks
            WHERE  status    = 'COOLING'
              AND  cooled_at < NOW() - INTERVAL '3 days'
        """)
        to_archive = [r[0] for r in cur.fetchall()]

        if not to_archive:
            return 0

        for ticker in to_archive:
            cur.execute("""
                UPDATE stocks
                SET    status     = 'ARCHIVED',
                       updated_at = NOW()
                WHERE  ticker = %s
            """, (ticker,))

            cur.execute("""
                INSERT INTO stock_events
                    (ticker, event_type, event_data, occurred_at)
                VALUES (%s, %s, %s, NOW())
            """, (
                ticker,
                'status_change',
                f'{{"from": "COOLING", "to": "ARCHIVED", '
                f'"reason": "cooling_grace_period_expired"}}',
            ))

        self.conn.commit()
        return len(to_archive)

    # ═════════════════════════════════════════════════════════
    # FAST TRACK — Viral ticker override
    # ═════════════════════════════════════════════════════════

    def fast_track(self, ticker: str,
                   reason: str = 'viral_spike') -> bool:
        """
        Immediately promotes a ticker to ACTIVE status.
        Bypasses CANDIDATE state entirely.

        Used when:
          - Reddit mentions spike > 500 in 1 hour
          - StockTwits trending ticker detected
          - Manual override requested

        Data backfill runs in background after promotion.
        """
        ticker = ticker.upper().strip()
        cur    = self.conn.cursor()

        cur.execute("""
            INSERT INTO stocks
                (ticker, status, fast_tracked,
                 promoted_at, activated_at, created_at)
            VALUES (%s, 'ACTIVE', TRUE, NOW(), NOW(), NOW())
            ON CONFLICT (ticker) DO UPDATE
                SET status       = 'ACTIVE',
                    fast_tracked = TRUE,
                    activated_at = NOW(),
                    updated_at   = NOW()
        """, (ticker,))

        cur.execute("""
            INSERT INTO stock_events
                (ticker, event_type, event_data, occurred_at)
            VALUES (%s, %s, %s, NOW())
        """, (
            ticker,
            'fast_track',
            f'{{"reason": "{reason}", '
            f'"skipped_candidate_phase": true}}',
        ))

        self.conn.commit()
        print(f'  ⚡ FAST TRACK: {ticker} → ACTIVE '
              f'(reason: {reason})')
        return True

    # ═════════════════════════════════════════════════════════
    # HELPERS
    # ═════════════════════════════════════════════════════════

    def get_tickers_by_status(self, status: str) -> list:
        """Returns all tickers with a given status."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT ticker FROM stocks
            WHERE  status = %s
            ORDER  BY updated_at DESC
        """, (status,))
        return [r[0] for r in cur.fetchall()]

    def get_status(self, ticker: str) -> Optional[str]:
        """Returns current status of a ticker."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT status FROM stocks WHERE ticker = %s
        """, (ticker,))
        row = cur.fetchone()
        return row[0] if row else None

    def _count_by_status(self, status: str) -> int:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM stocks WHERE status = %s
        """, (status,))
        return cur.fetchone()[0]

    def _log_run(self, result: dict):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO screener_runs (
                candidates_found, promoted,
                moved_to_cooling, archived,
                total_active, run_at
            ) VALUES (%s,%s,%s,%s,%s,%s)
        """, (
            result['candidates_found'],
            result['promoted'],
            result['moved_to_cooling'],
            result['archived'],
            result['total_active'],
            result['run_at'],
        ))
        self.conn.commit()

    @staticmethod
    def _print_summary(result: dict):
        print(f'\n  ── Screener Summary ──')
        print(f'  Candidates found:   {result["candidates_found"]}')
        print(f'  Promoted to CAND:   {result["promoted"]}')
        print(f'  No action:          {result["no_action"]}')
        print(f'  Moved to COOLING:   {result["moved_to_cooling"]}')
        print(f'  Archived:           {result["archived"]}')
        print(f'  Total ACTIVE:       {result["total_active"]}')
        print(f'  Total CANDIDATE:    {result["total_candidate"]}')
        if result['candidate_tickers']:
            print(f'  New candidates:     '
                  f'{result["candidate_tickers"][:10]}')

    def _ensure_tables(self):
        cur = self.conn.cursor()

        # screener_runs — audit log of each screen cycle
        cur.execute("""
            CREATE TABLE IF NOT EXISTS screener_runs (
                id               SERIAL PRIMARY KEY,
                candidates_found INTEGER,
                promoted         INTEGER,
                moved_to_cooling INTEGER,
                archived         INTEGER,
                total_active     INTEGER,
                run_at           TIMESTAMP DEFAULT NOW()
            )
        """)

        # Ensure cooled_at column exists on stocks table
        cur.execute("""
            ALTER TABLE stocks
            ADD COLUMN IF NOT EXISTS cooled_at TIMESTAMP
        """)

        self.conn.commit()

    def close(self):
        self.conn.close()


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    screener = UniverseScreener()

    print('\nRunning universe screen...')
    result = screener.run()

    print(f'\nActive tickers: '
          f'{screener.get_tickers_by_status("ACTIVE")[:10]}')
    print(f'Candidate tickers: '
          f'{screener.get_tickers_by_status("CANDIDATE")[:10]}')

    # Test fast track
    print('\nTesting fast track (GME)...')
    screener.fast_track('GME', reason='test_manual_override')
    print(f'GME status: {screener.get_status("GME")}')

    screener.close()