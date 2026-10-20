<<<<<<< HEAD
"""
pipeline/collectors/finra_collector.py
FINRA Short Sale Volume Collector — TASK-202

Downloads and parses the FINRA daily REGSHO consolidated short sale volume file.
Data is available the next business day by ~6 AM ET.

URL pattern:
  https://cdn.finra.org/equity/regsho/daily/CNMSshvol{YYYYMMDD}.txt
Format: pipe-delimited text with columns Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market

Storage targets:
  - short_volume_daily → Cassandra (per ARCHITECTURE.md §4.4)
  - collection_log     → PostgreSQL (every run, success or failure)

Refresh rate: Once daily (next business day). Scheduled by Celery beat.
"""

import os
import io
import time
import logging
from datetime import datetime, timezone, date, timedelta
from typing import Optional

import requests
import pandas as pd
import psycopg2
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
=======
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
>>>>>>> b1b833355d4dc64e3a8533a88f4c9a2fa3acf843
from dotenv import load_dotenv

load_dotenv()

<<<<<<< HEAD
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

FINRA_URL_TEMPLATE = (
    "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date_str}.txt"
)
REQUEST_TIMEOUT = 30           # seconds — files can be a few MB
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2           # seconds, doubles each attempt
FINRA_COLUMNS = [
    "Symbol",
    "ShortVolume",
    "ShortExemptVolume",
    "TotalVolume",
    "Market",
    "Date",          # some file versions include a trailing Date column
]
REQUEST_HEADERS = {
    "User-Agent": (
        "SqueezeRadar/1.0 contact@squeezeradar.com"
    ),
}


# ─────────────────────────────────────────────────────────────────────────────
# FINRACollector
# ─────────────────────────────────────────────────────────────────────────────

class FINRACollector:
    """
    Downloads FINRA daily consolidated short sale volume files and extracts
    per-ticker short volume data.

    Interface (per DESIGN.md §7.1):
        __init__()   — establish DB connections
        collect()    — main public method; collects yesterday's data for a ticker
        _save()      — database write (private)
        _log()       — writes to collection_log (private)

    Additional public helpers:
        download_daily_file(date)           → full DataFrame for that date
        get_short_volume(ticker, date)      → single ticker row dict
        get_short_volume_history(ticker, n) → DataFrame of last n business days
    """

    SOURCE = "finra"

    def __init__(self):
        try:
            self._pg_conn = self._connect_postgres()
        except Exception as e:
            logger.warning("PostgreSQL connection not established at init: %s", e)
            self._pg_conn = None
        self._cass_session = self._connect_cassandra()
        # In-memory cache of downloaded files to avoid re-downloading
        # within the same collector lifetime.
        self._file_cache: dict[str, pd.DataFrame] = {}

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def collect(self, ticker: str, target_date: Optional[date] = None) -> dict:
        """
        Main collection entry point.

        Collects FINRA short sale data for `ticker` on `target_date`
        (defaults to the most recent available business day).

        Steps:
          1. Resolve the target date (default: yesterday / last business day).
          2. Download and parse the FINRA daily file.
          3. Extract the row for the requested ticker.
          4. Write to Cassandra short_volume_daily.
          5. Log to collection_log.
          6. Return result dict.
        """
        ticker = ticker.upper().strip()
        start_ts = time.time()
        result = self._base_result(ticker)

        try:
            # ── 1. Resolve date ────────────────────────────────────────────
            if target_date is None:
                target_date = self._last_business_day()

            result["target_date"] = str(target_date)

            # ── 2. Download file ───────────────────────────────────────────
            df = self.download_daily_file(target_date)
            if df is None or df.empty:
                raise FileNotFoundError(
                    f"FINRA file for {target_date} not available or empty."
                )

            # ── 3. Extract ticker ──────────────────────────────────────────
            row = self.get_short_volume(ticker, target_date, df=df)
            if row is None:
                status = "skipped"
                duration_ms = int((time.time() - start_ts) * 1000)
                result.update({
                    "error": False,
                    "error_message": f"Ticker {ticker} not in FINRA file for {target_date}",
                    "short_volume": None,
                    "total_volume": None,
                    "short_volume_ratio": None,
                    "duration_ms": duration_ms,
                })
                self._log(ticker, status, 0,
                          result["error_message"], duration_ms)
                logger.info("[%s] Not found in FINRA file for %s", ticker, target_date)
                return result

            # ── 4. Persist ─────────────────────────────────────────────────
            self._save(ticker, target_date, row)

            # ── 5. Build result ────────────────────────────────────────────
            duration_ms = int((time.time() - start_ts) * 1000)
            result.update({
                "error": False,
                "error_message": None,
                "short_volume": row["short_volume"],
                "total_volume": row["total_volume"],
                "short_volume_ratio": row["short_volume_ratio"],
                "duration_ms": duration_ms,
            })
            self._log(ticker, "success", 1, None, duration_ms)
            logger.info(
                "✓ [%s] FINRA — short_vol=%s, ratio=%.2f%% (%dms)",
                ticker,
                f"{row['short_volume']:,}" if row["short_volume"] else "N/A",
                (row["short_volume_ratio"] or 0) * 100,
                duration_ms,
            )

        except Exception as exc:
            duration_ms = int((time.time() - start_ts) * 1000)
            error_msg = str(exc)
            result["error"] = True
            result["error_message"] = error_msg
            result["duration_ms"] = duration_ms
            self._log(ticker, "failed", 0, error_msg, duration_ms)
            logger.error("✗ [%s] FINRA collection failed: %s", ticker, error_msg)

        return result

    def download_daily_file(
        self, target_date: date, use_cache: bool = True
    ) -> Optional[pd.DataFrame]:
        """
        Downloads and parses the FINRA REGSHO pipe-delimited file for `target_date`.

        Returns a pandas DataFrame with columns:
            Symbol, ShortVolume, ShortExemptVolume, TotalVolume, Market
        Returns None if the file is unavailable (weekend, holiday, not yet published).

        Retries up to MAX_RETRIES on network errors.
        """
        date_str = target_date.strftime("%Y%m%d")
        cache_key = date_str

        if use_cache and cache_key in self._file_cache:
            logger.debug("Using cached FINRA file for %s", date_str)
            return self._file_cache[cache_key]

        url = FINRA_URL_TEMPLATE.format(date_str=date_str)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.get(
                    url,
                    headers=REQUEST_HEADERS,
                    timeout=REQUEST_TIMEOUT,
                )

                if response.status_code == 404:
                    logger.info(
                        "FINRA file not available for %s (404 — weekend/holiday or not yet published)",
                        date_str,
                    )
                    return None

                if response.status_code == 429:
                    delay = RETRY_BASE_DELAY ** attempt * 5
                    logger.warning("FINRA rate limited (429). Backing off %ds.", delay)
                    time.sleep(delay)
                    continue

                response.raise_for_status()

                # Parse the pipe-delimited file
                df = self._parse_finra_file(response.text, date_str)
                if df is not None and use_cache:
                    self._file_cache[cache_key] = df
                return df

            except requests.RequestException as exc:
                delay = RETRY_BASE_DELAY ** attempt
                logger.warning(
                    "download_daily_file attempt %d failed (%s). Retry in %ds.",
                    attempt, exc, delay,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(delay)

        logger.error("All %d download attempts failed for FINRA file %s.", MAX_RETRIES, date_str)
        return None

    def get_short_volume(
        self,
        ticker: str,
        target_date: Optional[date] = None,
        df: Optional[pd.DataFrame] = None,
    ) -> Optional[dict]:
        """
        Returns a dict with short volume metrics for a single ticker on a given date.

        Returns None if the ticker is not present in the file.

        Dict format:
          {
            'ticker': str,
            'date': date,
            'short_volume': int,
            'short_exempt_volume': int,
            'total_volume': int,
            'short_volume_ratio': float,   # short_volume / total_volume
          }
        """
        ticker = ticker.upper()
        if target_date is None:
            target_date = self._last_business_day()

        if df is None:
            df = self.download_daily_file(target_date)

        if df is None or df.empty:
            return None

        match = df[df["Symbol"] == ticker]
        if match.empty:
            return None

        row = match.iloc[0]
        short_vol = int(row["ShortVolume"])
        total_vol = int(row["TotalVolume"])
        short_exempt = int(row.get("ShortExemptVolume", 0) or 0)

        ratio = round(short_vol / total_vol, 6) if total_vol > 0 else 0.0

        return {
            "ticker": ticker,
            "date": target_date,
            "short_volume": short_vol,
            "short_exempt_volume": short_exempt,
            "total_volume": total_vol,
            "short_volume_ratio": ratio,
        }

    def get_short_volume_history(
        self, ticker: str, days: int = 30
    ) -> pd.DataFrame:
        """
        Returns a DataFrame of short volume history for `ticker` over the
        last `days` business days.

        Columns: date, short_volume, total_volume, short_volume_ratio

        Missing dates (weekends, holidays, unavailable files) are silently skipped.
        """
        ticker = ticker.upper()
        records = []
        target = date.today()

        business_days_checked = 0
        # Overshoot by 50% to account for weekends/holidays
        for _ in range(int(days * 1.5)):
            target -= timedelta(days=1)
            if target.weekday() >= 5:      # skip weekends
                continue

            row = self.get_short_volume(ticker, target)
            if row is not None:
                records.append({
                    "date": row["date"],
                    "short_volume": row["short_volume"],
                    "total_volume": row["total_volume"],
                    "short_volume_ratio": row["short_volume_ratio"],
                })
                business_days_checked += 1

            if business_days_checked >= days:
                break

        if not records:
            return pd.DataFrame(
                columns=["date", "short_volume", "total_volume", "short_volume_ratio"]
            )

        df = pd.DataFrame(records).sort_values("date").reset_index(drop=True)
        return df

    # ─────────────────────────────────────────────────────────────────────────
    # Persistence (private)
    # ─────────────────────────────────────────────────────────────────────────

    def _save(self, ticker: str, target_date: date, row: dict):
        """
        Writes short volume data to Cassandra short_volume_daily table.
        This is the primary storage target per ARCHITECTURE.md §4.4.
        """
        self._save_cassandra(ticker, target_date, row)

    def _save_cassandra(self, ticker: str, target_date: date, row: dict):
        """Upserts a row into the Cassandra short_volume_daily table."""
        if self._cass_session is None:
            logger.warning(
                "Cassandra unavailable — skipping short_volume_daily write for %s", ticker
            )
            return
        try:
            cql = """
                INSERT INTO short_volume_daily
                    (ticker, date, short_volume, total_volume, short_ratio, source)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            self._cass_session.execute(cql, (
                ticker,
                target_date,
                row["short_volume"],
                row["total_volume"],
                row["short_volume_ratio"],
                self.SOURCE,
            ))
        except Exception as exc:
            logger.error("[%s] Cassandra write failed: %s", ticker, exc)
            raise

    # ─────────────────────────────────────────────────────────────────────────
    # Logging
    # ─────────────────────────────────────────────────────────────────────────

    def _log(self, ticker: str, status: str, rows: int,
             error: Optional[str], duration_ms: int):
        """
        Writes a row to collection_log.
        Called on both success and failure (per SPEC.md FR-011).
        """
        if not self._pg_conn:
            logger.warning("Postgres unavailable - skipping collection_log write for %s", ticker)
            return

        sql = """
            INSERT INTO collection_log
                (ticker, source, status, rows_collected, error_message,
                 duration_ms, collected_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """
        try:
            with self._pg_conn.cursor() as cur:
                cur.execute(sql, (ticker, self.SOURCE, status, rows, error, duration_ms))
            self._pg_conn.commit()
        except Exception as log_exc:
            logger.error("Failed to write collection_log: %s", log_exc)

    # ─────────────────────────────────────────────────────────────────────────
    # Parsing helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_finra_file(content: str, date_str: str) -> Optional[pd.DataFrame]:
        """
        Parses FINRA pipe-delimited short sale file content into a DataFrame.

        Expected header line:
            Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market

        Some versions include a trailing Date column. The footer line begins with
        a '#' comment and is dropped.

        Returns None if the content cannot be parsed.
        """
        try:
            lines = [
                line for line in content.splitlines()
                if line.strip() and not line.startswith("#")
            ]
            if not lines:
                logger.warning("FINRA file for %s is empty.", date_str)
                return None

            df = pd.read_csv(
                io.StringIO("\n".join(lines)),
                sep="|",
                dtype=str,
            )

            # Normalise column names
            df.columns = [c.strip() for c in df.columns]

            # Keep only expected columns that exist
            keep = [c for c in ["Symbol", "ShortVolume", "ShortExemptVolume",
                                 "TotalVolume", "Market"] if c in df.columns]
            df = df[keep]

            # Coerce numeric columns
            for col in ["ShortVolume", "ShortExemptVolume", "TotalVolume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

            # Normalise symbol
            df["Symbol"] = df["Symbol"].str.upper().str.strip()

            # Drop the Market-wide summary rows (Symbol is empty or 'Total')
            df = df[df["Symbol"].notna() & (df["Symbol"] != "") & (df["Symbol"] != "TOTAL")]

            return df.reset_index(drop=True)

        except Exception as exc:
            logger.error("Failed to parse FINRA file %s: %s", date_str, exc)
            return None

    # ─────────────────────────────────────────────────────────────────────────
    # Date helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _last_business_day(reference: Optional[date] = None) -> date:
        """
        Returns the most recent business day (Mon–Fri) before or on `reference`.
        Defaults to today.
        """
        d = reference or date.today()
        # Go back one day first — FINRA data is always T+1
        d -= timedelta(days=1)
        # Keep going back until we land on a weekday
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d

    # ─────────────────────────────────────────────────────────────────────────
    # Connection helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _connect_postgres(self):
        url = os.getenv("POSTGRES_URL")
        if url:
            return psycopg2.connect(url)
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            dbname=os.getenv("POSTGRES_DB", "squeezradar"),
            user=os.getenv("POSTGRES_USER", "admin"),
            password=os.getenv("POSTGRES_PASSWORD", "password"),
        )

    def _connect_cassandra(self):
        """
        Returns a Cassandra session or None if unavailable at startup.
        """
        try:
            host = os.getenv("CASSANDRA_HOST", "localhost")
            port = int(os.getenv("CASSANDRA_PORT", 9042))
            keyspace = os.getenv("CASSANDRA_KEYSPACE", "squeezradar")

            cluster = Cluster(
                [host],
                port=port,
                load_balancing_policy=RoundRobinPolicy(),
            )
            session = cluster.connect(keyspace)
            return session

        except Exception as exc:
            logger.warning(
                "Cassandra unavailable at startup (%s). "
                "short_volume_daily writes will be skipped.", exc
            )
            return None

    @staticmethod
    def _base_result(ticker: str) -> dict:
        """Returns the minimal required result dict skeleton (DESIGN.md §7.2)."""
        return {
            "ticker": ticker,
            "collected_at": datetime.now(tz=timezone.utc).isoformat(),
            "target_date": None,
            "error": True,
            "error_message": "Collection not yet attempted",
            "short_volume": None,
            "total_volume": None,
            "short_volume_ratio": None,
            "duration_ms": 0,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Standalone runner
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    collector = FINRACollector()

    # Test: collect yesterday's data for GME and a fake ticker
    test_tickers = ["GME", "FAKE123"]
    for t in test_tickers:
        print(f"\n{'='*60}")
        print(f"Collecting FINRA data: {t}")
        result = collector.collect(t)
        print(json.dumps(result, indent=2, default=str))

    # Test: history fetch
    print(f"\n{'='*60}")
    print("FINRA short volume history for GME (last 5 business days):")
    history = collector.get_short_volume_history("GME", days=5)
    print(history.to_string(index=False))
=======
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
>>>>>>> b1b833355d4dc64e3a8533a88f4c9a2fa3acf843
