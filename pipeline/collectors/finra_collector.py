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
from dotenv import load_dotenv

load_dotenv()

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
