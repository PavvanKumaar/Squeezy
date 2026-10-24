# pipeline/collectors/finra_collector.py  (v2 — Cassandra primary)
#
# Storage architecture:
#   PRIMARY:   Cassandra short_volume_daily
#              partition key: (ticker, date)  — one row per partition
#              clustering:    date DESC
#              → 8,000 tickers × 252 days = 2M rows/year
#              → Cassandra reads/writes this in microseconds
#              → Perfect append-only time-series pattern
#
#   SECONDARY: PostgreSQL finra_short_volume
#              → Kept for SQL window queries (30d avg, trend)
#              → TimescaleDB makes rolling queries trivial
#              → Cannot be done in Cassandra (no window functions)
#
# Rule: write everything to Cassandra FIRST.
#       Write to PostgreSQL for analytics queries.
#       Read from PostgreSQL for _compute_30d_avg / _compute_trend.

import requests
import psycopg2
import os
import io
import time
from datetime import datetime, date, timedelta
from typing import Optional
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

try:
    from cassandra.cluster import Cluster
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False
    print('[finra] WARNING: cassandra-driver not installed.')

FINRA_BASE_URL = (
    'https://cdn.finra.org/equity/regsho/daily/'
    'CNMSshvol{date}.txt'
)

RATIO_TIERS = [
    (0.00, 0.35, 'very_low'),
    (0.35, 0.45, 'low'),
    (0.45, 0.55, 'neutral'),
    (0.55, 0.65, 'elevated'),
    (0.65, 1.00, 'high'),
]


def classify_short_ratio(ratio: float) -> str:
    for lo, hi, label in RATIO_TIERS:
        if lo <= ratio < hi:
            return label
    return 'high'


class FinraCollector:

    def __init__(self):
        self.pg = psycopg2.connect(os.getenv('POSTGRES_URL'))
        self._ensure_pg_tables()

        # Cassandra — primary write target
        self.cass_session = None
        self._init_cassandra()

        # In-memory file cache:
        # Don't re-download the same file for every ticker.
        # One file covers all tickers for that date.
        self._file_cache = {}  # date_str → DataFrame

    def _init_cassandra(self):
        if not CASSANDRA_AVAILABLE:
            return
        try:
            cluster = Cluster(
                [os.getenv('CASSANDRA_HOST', 'localhost')],
                port=int(os.getenv('CASSANDRA_PORT', 9042)),
            )
            self.cass_session = cluster.connect('squeezradar')
            self._ensure_cassandra_table()
            print('[finra] Cassandra connected ✓')
        except Exception as e:
            print(f'[finra] Cassandra unavailable: {e}')
            print('[finra] Will use PostgreSQL only.')
            self.cass_session = None

    # ═════════════════════════════════════════════════════════
    # PUBLIC METHODS
    # ═════════════════════════════════════════════════════════

    def collect(self, ticker: str,
                date_str: Optional[str] = None) -> dict:
        """
        Collects FINRA short volume for one ticker.
        Downloads the daily file once and caches it in memory.
        Subsequent tickers in the same run use the cached file.
        """
        start    = time.time()
        date_str = date_str or self._yesterday_str()
        ticker   = ticker.upper().strip()

        # Get the file (from cache or download)
        daily_df = self._get_file(date_str)

        if daily_df is None or daily_df.empty:
            self._log(ticker, 'failed', 0,
                      f'file_unavailable_{date_str}',
                      int((time.time()-start)*1000))
            return {
                'ticker':        ticker,
                'date':          date_str,
                'error':         True,
                'error_message': f'FINRA file unavailable for {date_str}',
            }

        row = daily_df[daily_df['Symbol'] == ticker]
        if row.empty:
            self._log(ticker, 'skipped', 0,
                      'not_in_file',
                      int((time.time()-start)*1000))
            return {
                'ticker':             ticker,
                'date':               date_str,
                'in_file':            False,
                'error':              False,
                'short_volume':       0,
                'total_volume':       0,
                'short_volume_ratio': 0.0,
            }

        row         = row.iloc[0]
        short_vol  = int(round(float(row.get('ShortVolume', 0))))
        exempt_vol = int(round(float(row.get('ShortExemptVolume', 0))))
        total_vol  = int(round(float(row.get('TotalVolume', 0)))) 
        ratio       = round(short_vol / total_vol, 6) \
                      if total_vol > 0 else 0.0

        trade_date  = datetime.strptime(date_str, '%Y%m%d').date()

        result = {
            'ticker':               ticker,
            'date':                 date_str,
            'trade_date':           trade_date,
            'short_volume':         short_vol,
            'short_exempt_volume':  exempt_vol,
            'total_volume':         total_vol,
            'short_volume_ratio':   ratio,
            'short_ratio_tier':     classify_short_ratio(ratio),
            'in_file':              True,
            'error':                False,
            'collected_at':         datetime.utcnow(),
        }

        # Derived fields (read from previously stored data)
        result['short_volume_30d_avg'] = self._compute_30d_avg(ticker)
        result['short_volume_trend']   = self._compute_trend(ticker)

        # WRITE TO CASSANDRA (primary)
        self._save_cassandra(ticker, trade_date, result)

        # WRITE TO POSTGRESQL (secondary — for SQL window queries)
        self._save_postgres(ticker, trade_date, result)

        duration = int((time.time()-start)*1000)
        self._log(ticker, 'success', 1, None, duration)

        print(
            f'  ✓ {ticker} FINRA {date_str} — '
            f'{ratio:.1%} [{result["short_ratio_tier"]}] | '
            f'trend: {result["short_volume_trend"]} | '
            f'storage: Cassandra{"+" if not self.cass_session else ""}'
        )
        return result

    def collect_batch_for_date(self, tickers: list,
                               date_str: Optional[str] = None) -> list:
        """
        Collects short volume for multiple tickers for the same date.
        Downloads the FINRA file ONCE and processes all tickers from it.
        This is the correct way to use this collector in production.
        """
        date_str = date_str or self._yesterday_str()
        print(f'\n[FINRA] Batch collection for {date_str} '
              f'({len(tickers)} tickers)...')

        # Pre-download file once
        df = self._get_file(date_str)
        if df is None:
            print(f'  File unavailable for {date_str}')
            return [{'ticker': t, 'error': True} for t in tickers]

        results = []
        for ticker in tickers:
            r = self.collect(ticker, date_str)
            results.append(r)

        success = sum(1 for r in results if not r.get('error'))
        in_file = sum(1 for r in results if r.get('in_file'))
        print(f'  ✓ {success}/{len(tickers)} collected, '
              f'{in_file} found in FINRA file')

        return results

    def collect_history(self, ticker: str,
                        days: int = 30) -> list:
        """Backfills N days of FINRA data for a ticker."""
        results = []
        d       = datetime.utcnow()

        for _ in range(days * 2):
            if len(results) >= days:
                break
            if d.weekday() >= 5:
                d -= timedelta(days=1)
                continue
            date_str = d.strftime('%Y%m%d')
            r = self.collect(ticker, date_str)
            if not r.get('error'):
                results.append(r)
            d -= timedelta(days=1)
            time.sleep(0.3)

        return results

    def get_history(self, ticker: str, days: int = 30) -> list:
        """
        Returns stored history from PostgreSQL.
        Used for _compute_30d_avg and _compute_trend.
        (Cassandra not used here — SQL window queries easier in PG)
        """
        cur = self.pg.cursor()
        cur.execute("""
            SELECT short_volume, total_volume,
                   short_volume_ratio, trade_date
            FROM   finra_short_volume
            WHERE  ticker     = %s
              AND  trade_date > CURRENT_DATE - INTERVAL '%s days'
            ORDER  BY trade_date DESC
        """, (ticker, days))
        return [
            {
                'short_volume':       r[0],
                'total_volume':       r[1],
                'short_volume_ratio': r[2],
                'trade_date':         r[3],
            }
            for r in cur.fetchall()
        ]

    # ═════════════════════════════════════════════════════════
    # FILE DOWNLOAD WITH IN-MEMORY CACHE
    # ═════════════════════════════════════════════════════════

    def _get_file(self, date_str: str) -> Optional[pd.DataFrame]:
        """
        Returns the FINRA file for a date.
        Uses in-memory cache — downloads once per date per process run.
        """
        if date_str in self._file_cache:
            return self._file_cache[date_str]

        df = self._download_file(date_str)
        if df is not None:
            self._file_cache[date_str] = df
        return df

    def _download_file(self, date_str: str) -> Optional[pd.DataFrame]:
        url = FINRA_BASE_URL.format(date=date_str)

        try:
            resp = requests.get(
                url,
                timeout=30,
                headers={'User-Agent': 'SqueezeRadar/1.0'}
            )

            if resp.status_code == 404:
                print(f'  [FINRA] File not found: {date_str}')
                return None

            if resp.status_code != 200:
                print(
                    f'  [FINRA] HTTP {resp.status_code} '
                    f'for {date_str}'
                )
                return None

            content = resp.text

            if not content or 'Symbol' not in content:
                print(f'  [FINRA] Empty/invalid file: {date_str}')
                return None

            df = pd.read_csv(
                io.StringIO(content),
                sep='|',
                dtype={
                    'Symbol': str,
                    'ShortVolume': 'float64',
                    'ShortExemptVolume': 'float64',
                    'TotalVolume': 'float64',
                }
            )

            # Remove FINRA trailer / invalid rows
            df = df[df['Symbol'].notna()]

            # Normalize symbols
            df['Symbol'] = (
                df['Symbol']
                .astype(str)
                .str.strip()
                .str.upper()
            )

            # FINRA symbols can be longer than 5 characters
            df = df[
                df['Symbol'].str.match(
                    r'^[A-Z0-9.\-]{1,14}$',
                    na=False
                )
            ]

            return df

        except Exception as e:
            print(f'  [FINRA] Download error {date_str}: {e}')
            return None
    # ═════════════════════════════════════════════════════════
    # DERIVED COMPUTATIONS
    # ═════════════════════════════════════════════════════════

    def _compute_30d_avg(self, ticker: str) -> float:
        history = self.get_history(ticker, days=30)
        ratios  = [h['short_volume_ratio'] for h in history
                   if h['short_volume_ratio'] is not None]
        if not ratios:
            return 0.0
        return round(sum(ratios) / len(ratios), 6)

    def _compute_trend(self, ticker: str) -> str:
        history = self.get_history(ticker, days=5)
        if len(history) < 3:
            return 'insufficient_data'
        ratios = [h['short_volume_ratio'] for h in history
                  if h['short_volume_ratio'] is not None]
        if len(ratios) < 3:
            return 'insufficient_data'
        mid    = len(ratios) // 2
        older  = sum(ratios[mid:])  / (len(ratios) - mid)
        newer  = sum(ratios[:mid])  / mid
        diff   = newer - older
        if diff > 0.03:
            return 'rising'
        if diff < -0.03:
            return 'falling'
        return 'flat'

    # ═════════════════════════════════════════════════════════
    # CASSANDRA STORAGE (PRIMARY)
    # ═════════════════════════════════════════════════════════

    def _save_cassandra(self, ticker: str,
                         trade_date: date, data: dict):
        if not self.cass_session:
            return  # Handled by PostgreSQL only

        try:
            self.cass_session.execute("""
                INSERT INTO short_volume_daily (
                    ticker, date,
                    short_volume, total_volume,
                    short_ratio, source
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                ticker,
                trade_date,
                data['short_volume'],
                data['total_volume'],
                data['short_volume_ratio'],
                'finra',
            ))
        except Exception as e:
            print(f'  [FINRA] Cassandra write error: {e}')

    # ═════════════════════════════════════════════════════════
    # POSTGRESQL STORAGE (SECONDARY — for SQL analytics)
    # ═════════════════════════════════════════════════════════

    def _save_postgres(self, ticker: str,
                        trade_date: date, data: dict):
        """
        Writes to PostgreSQL for SQL window function queries.
        ON CONFLICT DO UPDATE ensures idempotent writes.
        """
        cur = self.pg.cursor()
        cur.execute("""
            INSERT INTO finra_short_volume (
                ticker, trade_date,
                short_volume, short_exempt_volume,
                total_volume, short_volume_ratio,
                short_ratio_tier, collected_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ticker, trade_date) DO UPDATE
              SET short_volume       = EXCLUDED.short_volume,
                  total_volume       = EXCLUDED.total_volume,
                  short_volume_ratio = EXCLUDED.short_volume_ratio,
                  short_ratio_tier   = EXCLUDED.short_ratio_tier
        """, (
            ticker,
            trade_date,
            data['short_volume'],
            data['short_exempt_volume'],
            data['total_volume'],
            data['short_volume_ratio'],
            data['short_ratio_tier'],
            data['collected_at'],
        ))
        self.pg.commit()

    # ═════════════════════════════════════════════════════════
    # TABLE SETUP
    # ═════════════════════════════════════════════════════════

    def _ensure_cassandra_table(self):
        try:
            self.cass_session.execute(
                'SELECT COUNT(*) FROM short_volume_daily LIMIT 1')
        except Exception as e:
            print(f'  [FINRA] Cassandra table check: {e}')
            print('  Run: python db/cassandra_setup.py first')

    def _ensure_pg_tables(self):
        cur = self.pg.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS finra_short_volume (
                id                  SERIAL PRIMARY KEY,
                ticker              VARCHAR(10) NOT NULL,
                trade_date          DATE NOT NULL,
                short_volume        BIGINT,
                short_exempt_volume BIGINT,
                total_volume        BIGINT,
                short_volume_ratio  FLOAT,
                short_ratio_tier    VARCHAR(20),
                collected_at        TIMESTAMP DEFAULT NOW(),
                UNIQUE (ticker, trade_date)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_finra_ticker_date
            ON finra_short_volume (ticker, trade_date DESC)
        """)
        self.pg.commit()

    def _log(self, ticker: str, status: str,
             rows: int, error: str, duration_ms: int):
        cur = self.pg.cursor()
        cur.execute("""
            INSERT INTO collection_log
                (ticker, source, status, rows_collected,
                 error_message, duration_ms)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (ticker, 'finra', status, rows, error, duration_ms))
        self.pg.commit()

    @staticmethod
    def _yesterday_str() -> str:
        d = datetime.utcnow() - timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d.strftime('%Y%m%d')

    def close(self):
        self.pg.close()


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    c = FinraCollector()

    print(f'Storage: Cassandra '
          f'{"(connected)" if c.cass_session else "(unavailable — using PostgreSQL)"}')

    # Single ticker test
    print('\n[1] Single ticker:')
    r = c.collect('GME')
    print(f'  ratio = {r.get("short_volume_ratio")}')
    print(f'  tier  = {r.get("short_ratio_tier")}')
    print(f'  trend = {r.get("short_volume_trend")}')
    print(f'  30avg = {r.get("short_volume_30d_avg")}')

    # Batch test (downloads file only once)
    print('\n[2] Batch collection (5 tickers, 1 file download):')
    results = c.collect_batch_for_date(
        ['GME', 'AMC', 'AAPL', 'TSLA', 'SPY'])
    for r in results:
        if not r.get('error') and r.get('in_file'):
            print(f'  {r["ticker"]}: {r["short_volume_ratio"]:.1%} '
                  f'[{r["short_ratio_tier"]}]')

    c.close()