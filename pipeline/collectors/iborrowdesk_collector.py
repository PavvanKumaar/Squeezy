<<<<<<< HEAD
"""
pipeline/collectors/iborrowdesk_collector.py
iBorrowDesk Borrow Rate Collector — TASK-102

Scrapes borrow rate (%) and available shares for a ticker from iborrowdesk.com.
Data is updated 3x daily on the source site.

Storage targets:
  - borrow_rate_history → Cassandra  (time-series append)
  - stock_snapshots     → PostgreSQL (TYPE A snapshot upsert)
  - collection_log      → PostgreSQL (every run, success or failure)

Refresh rate: 3× per day (scheduled by Celery).
"""

import os
import re
import time
import logging
from datetime import datetime, timezone, date
from typing import Optional

import requests
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
=======
import os
import re
import time
import requests
import psycopg2
from datetime import datetime, date, timedelta
from bs4 import BeautifulSoup
>>>>>>> b1b833355d4dc64e3a8533a88f4c9a2fa3acf843
from dotenv import load_dotenv

load_dotenv()

<<<<<<< HEAD
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

IBORROWDESK_URL = "https://iborrowdesk.com/report/{ticker}"
REQUEST_TIMEOUT = 15           # seconds
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2           # doubles each attempt
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ─────────────────────────────────────────────────────────────────────────────
# iBorrowDeskCollector
# ─────────────────────────────────────────────────────────────────────────────

class iBorrowDeskCollector:
    """
    Scrapes iborrowdesk.com for short-selling borrow rates and available shares.

    Interface (per DESIGN.md §7.1):
        __init__()   — establish DB connections
        collect()    — main public method, always returns a dict
        _save()      — database write (private)
        _log()       — writes to collection_log (private)
    """

    SOURCE = "iborrowdesk"

    def __init__(self):
        try:
            self._pg_conn = self._connect_postgres()
        except Exception as e:
            logger.warning("PostgreSQL connection not established at init: %s", e)
            self._pg_conn = None
        self._cass_session = self._connect_cassandra()

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def collect(self, ticker: str) -> dict:
        """
        Main collection entry point.

        Steps:
          1. Scrape iborrowdesk.com for borrow rate and available shares.
          2. Write to Cassandra borrow_rate_history.
          3. Write a TYPE A snapshot to PostgreSQL stock_snapshots.
          4. Log to collection_log.
          5. Return result dict.

        On any failure, returns a result with error=True and logs the failure.
        Does NOT raise — callers must check result['error'].
        """
        ticker = ticker.upper().strip()
        start_ts = time.time()
        result = self._base_result(ticker)

        try:
            # ── 1. Scrape ──────────────────────────────────────────────────
            borrow_rate = self.get_borrow_rate(ticker)
            available_shares = self.get_available_shares(ticker)

            if borrow_rate is None and available_shares is None:
                raise ValueError(f"Ticker '{ticker}' not found on iborrowdesk.")

            # ── 2 & 3. Persist ─────────────────────────────────────────────
            self._save(ticker, borrow_rate, available_shares)

            # ── 4. Build result ────────────────────────────────────────────
            duration_ms = int((time.time() - start_ts) * 1000)
            result.update({
                "error": False,
                "error_message": None,
                "borrow_rate": borrow_rate,
                "available_shares": available_shares,
                "duration_ms": duration_ms,
            })
            self._log(ticker, "success", 1, None, duration_ms)
            logger.info(
                "✓ [%s] iborrowdesk — borrow_rate=%.2f%%, avail_shares=%s (%dms)",
                ticker,
                borrow_rate or 0,
                f"{available_shares:,}" if available_shares is not None else "N/A",
                duration_ms,
            )

        except Exception as exc:
            duration_ms = int((time.time() - start_ts) * 1000)
            error_msg = str(exc)
            result["error"] = True
            result["error_message"] = error_msg
            result["duration_ms"] = duration_ms
            self._log(ticker, "failed", 0, error_msg, duration_ms)
            logger.error("✗ [%s] iborrowdesk collection failed: %s", ticker, error_msg)

        return result

    # ─────────────────────────────────────────────────────────────────────────
    # Data fetchers (public, usable independently)
    # ─────────────────────────────────────────────────────────────────────────

    def get_borrow_rate(self, ticker: str) -> Optional[float]:
        """
        Returns the current borrow fee rate as a percentage float (e.g. 47.2),
        or None if the ticker is not found or the page cannot be parsed.

        Retries up to MAX_RETRIES on transient network errors.
        """
        soup = self._fetch_page(ticker)
        if soup is None:
            return None

        return self._parse_borrow_rate(soup)

    def get_available_shares(self, ticker: str) -> Optional[int]:
        """
        Returns the number of shares available to borrow as an int,
        or None if not found / unparseable.

        Retries up to MAX_RETRIES on transient network errors.
        """
        soup = self._fetch_page(ticker)
        if soup is None:
            return None

        return self._parse_available_shares(soup)

    # ─────────────────────────────────────────────────────────────────────────
    # Persistence (private)
    # ─────────────────────────────────────────────────────────────────────────

    def _save(self, ticker: str, borrow_rate: Optional[float],
              available_shares: Optional[int]):
        """
        Writes borrow rate data to two storage targets:
          1. Cassandra borrow_rate_history  (time-series, per ARCHITECTURE.md §4.4)
          2. PostgreSQL stock_snapshots      (TYPE A snapshot, per DESIGN.md §3.2)
        """
        now = datetime.now(tz=timezone.utc)
        today = date.today()

        # ── Cassandra: borrow_rate_history ───────────────────────────────
        self._save_cassandra(ticker, today, now, borrow_rate, available_shares)

        # ── PostgreSQL: stock_snapshots ──────────────────────────────────
        self._save_postgres_snapshot(ticker, borrow_rate, available_shares, now)

    def _save_cassandra(
        self,
        ticker: str,
        today: date,
        now: datetime,
        borrow_rate: Optional[float],
        available_shares: Optional[int],
    ):
        """Appends a borrow_rate_history row to Cassandra."""
        if self._cass_session is None:
            logger.warning("Cassandra unavailable — skipping borrow_rate_history write for %s", ticker)
            return
        try:
            cql = """
                INSERT INTO borrow_rate_history
                    (ticker, date, ts, borrow_rate, available_shares, source)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            self._cass_session.execute(cql, (
                ticker,
                today,
                now,
                float(borrow_rate) if borrow_rate is not None else None,
                int(available_shares) if available_shares is not None else None,
                self.SOURCE,
            ))
        except Exception as exc:
            logger.error("[%s] Cassandra write failed: %s", ticker, exc)
            raise

    def _save_postgres_snapshot(
        self,
        ticker: str,
        borrow_rate: Optional[float],
        available_shares: Optional[int],
        collected_at: datetime,
    ):
        """
        Inserts a TYPE A stock_snapshots row for this borrow rate collection.
        Only sets borrow_rate and available_shares fields; other snapshot fields
        are left NULL and filled by other collectors.
        """
        if not self._pg_conn:
            logger.warning("Postgres unavailable - skipping stock_snapshots write for %s", ticker)
            return

        sql = """
            INSERT INTO stock_snapshots
                (ticker, borrow_rate, available_shares, data_source, collected_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        with self._pg_conn.cursor() as cur:
            cur.execute(sql, (
                ticker,
                borrow_rate,
                available_shares,
                self.SOURCE,
                collected_at,
            ))
        self._pg_conn.commit()

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
    # Scraping helpers (private)
    # ─────────────────────────────────────────────────────────────────────────

    def _fetch_page(self, ticker: str) -> Optional[BeautifulSoup]:
        """
        Fetches and parses the iborrowdesk report page for a ticker.
        Implements retry with exponential backoff (SPEC.md FR-013).
        Returns a BeautifulSoup object or None on all failures.
        """
        url = IBORROWDESK_URL.format(ticker=ticker.upper())

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.get(
                    url,
                    headers=REQUEST_HEADERS,
                    timeout=REQUEST_TIMEOUT,
                )

                if response.status_code == 404:
                    logger.warning("[%s] iborrowdesk: ticker not found (404)", ticker)
                    return None

                if response.status_code == 429:
                    # Rate limited — back off aggressively
                    delay = RETRY_BASE_DELAY ** attempt * 5
                    logger.warning(
                        "[%s] iborrowdesk: rate limited (429). Backing off %ds.", ticker, delay
                    )
                    time.sleep(delay)
                    continue

                response.raise_for_status()
                return BeautifulSoup(response.text, "html.parser")

            except requests.RequestException as exc:
                delay = RETRY_BASE_DELAY ** attempt
                logger.warning(
                    "[%s] _fetch_page attempt %d failed (%s). Retry in %ds.",
                    ticker, attempt, exc, delay,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(delay)

        logger.error("[%s] All %d fetch attempts failed.", ticker, MAX_RETRIES)
        return None

    @staticmethod
    def _parse_borrow_rate(soup: BeautifulSoup) -> Optional[float]:
        """
        Parses the borrow fee rate from the iborrowdesk page HTML.

        iborrowdesk.com page structure (as of 2026):
          The current fee rate is displayed in a table with headers like
          "Fee Rate" or in a highlighted stat card. The value is a string
          like "47.20%" or "47.2 %".

        Returns the float percentage (e.g. 47.2) or None if not found.
        """
        # Strategy 1: look for a <td> or <span> containing "%" near a "Fee" label
        # Try common CSS patterns used on the iborrowdesk report page
        candidates = []

        # Pattern A: table cells with a data-label attribute containing "fee"
        for tag in soup.find_all(["td", "th", "span", "div"]):
            text = tag.get_text(strip=True)
            label = (tag.get("data-label") or "").lower()
            if "fee" in label and "%" in text:
                candidates.append(text)

        # Pattern B: scan all text nodes for a % value that follows a "fee" context
        # Look for a section or row that has "Fee Rate" as a sibling/preceding text
        for tag in soup.find_all(True):
            text = tag.get_text(strip=True)
            if re.search(r"fee\s*rate", text, re.IGNORECASE) and "%" in text:
                # Try extracting the % value from this block
                match = re.search(r"([\d,.]+)\s*%", text)
                if match:
                    candidates.append(match.group(1))

        # Pattern C: look for standalone % values in stat-card style elements
        for tag in soup.find_all(["h1", "h2", "h3", "h4", "strong", "b"]):
            text = tag.get_text(strip=True)
            if "%" in text:
                match = re.search(r"([\d,.]+)\s*%", text)
                if match:
                    # Only keep if the parent context mentions "fee" or "borrow"
                    parent_text = (tag.parent.get_text(strip=True) if tag.parent else "").lower()
                    if "fee" in parent_text or "borrow" in parent_text:
                        candidates.append(match.group(1))

        for raw in candidates:
            try:
                return float(raw.replace(",", "").replace("%", "").strip())
            except ValueError:
                continue

        logger.debug("Could not parse borrow rate from iborrowdesk page.")
        return None

    @staticmethod
    def _parse_available_shares(soup: BeautifulSoup) -> Optional[int]:
        """
        Parses the number of shares available to borrow from the iborrowdesk page.

        Returns an int (e.g. 250000) or None if not found.
        """
        candidates = []

        # Pattern A: data-label containing "available" or "shares"
        for tag in soup.find_all(["td", "th", "span", "div"]):
            label = (tag.get("data-label") or "").lower()
            text = tag.get_text(strip=True).replace(",", "")
            if ("available" in label or "shares" in label) and text.isdigit():
                candidates.append(text)

        # Pattern B: text near "Available" heading
        for tag in soup.find_all(True):
            text = tag.get_text(strip=True)
            if re.search(r"available\s*(shares)?", text, re.IGNORECASE):
                # Try to find a sibling or child with a raw number
                for sibling in (tag.next_siblings if hasattr(tag, "next_siblings") else []):
                    s_text = getattr(sibling, "get_text", lambda **_: "")().replace(",", "").strip()
                    if s_text.isdigit():
                        candidates.append(s_text)
                        break

        # Pattern C: large integers in stat cards
        for tag in soup.find_all(["h1", "h2", "h3", "strong", "b"]):
            parent_text = (tag.parent.get_text(strip=True) if tag.parent else "").lower()
            if "available" in parent_text or "shares" in parent_text:
                text = tag.get_text(strip=True).replace(",", "").strip()
                if text.isdigit():
                    candidates.append(text)

        for raw in candidates:
            try:
                return int(raw)
            except ValueError:
                continue

        logger.debug("Could not parse available shares from iborrowdesk page.")
        return None

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
        Returns a Cassandra session connected to the squeezradar keyspace.
        Returns None (instead of raising) so the rest of the collector can
        still function if Cassandra is temporarily unavailable.
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
                "borrow_rate_history writes will be skipped.", exc
            )
            return None

    @staticmethod
    def _base_result(ticker: str) -> dict:
        """Returns the minimal required result dict skeleton (DESIGN.md §7.2)."""
        return {
            "ticker": ticker,
            "collected_at": datetime.now(tz=timezone.utc).isoformat(),
            "error": True,
            "error_message": "Collection not yet attempted",
            "borrow_rate": None,
            "available_shares": None,
            "duration_ms": 0,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Standalone runner
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    collector = iBorrowDeskCollector()

    test_tickers = ["GME", "FAKE123"]
    for t in test_tickers:
        print(f"\n{'='*60}")
        print(f"Collecting: {t}")
        result = collector.collect(t)
        print(json.dumps(result, indent=2, default=str))
=======
try:
    from cassandra.cluster import Cluster
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False
    print('[iborrowdesk] WARNING: cassandra-driver not installed.')


# Borrow rate tier classification
# Used as a categorical feature in the ML model
BORROW_TIERS = [
    (0,    10,  'cheap'),           # < 10%  — easy to borrow, many shares
    (10,   30,  'normal'),          # 10-30% — moderate short interest
    (30,   75,  'expensive'),       # 30-75% — elevated, shorts feeling pain
    (75,   200, 'hard_to_borrow'), # 75-200% — severe squeeze potential
    (200,  999, 'extreme'),         # > 200% — extreme, seen in GME/AMC peaks
]


def classify_borrow_rate(rate: float) -> str:
    for lo, hi, label in BORROW_TIERS:
        if lo <= rate < hi:
            return label
    return 'extreme'


class IBorrowDeskCollector:
    """
    Scrapes iborrowdesk.com for borrow rate and available shares.
    
    Storage architecture:
       PRIMARY:   Cassandra borrow_rate_history
                  partition key: (ticker, date)
                  clustering:    ts DESC
                  
       SECONDARY: PostgreSQL stock_snapshots
                  → Latest borrow rate snapshot only
                  → Overwritten/upserted each collection cycle
                  → Fallback table if Cassandra is unavailable

    Data update schedule (ET):
      ~8:00 AM  — pre-market reading
      ~12:00 PM — midday reading
      ~4:00 PM  — close reading

    Schedule your Celery task to run at 8:05, 12:05, 16:05 ET.
    """

    BASE_URL = 'https://iborrowdesk.com/report/{ticker}'
    HEADERS  = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        ),
        'Accept': 'text/html,application/xhtml+xml,application/json,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://iborrowdesk.com/',
    }

    def __init__(self):
        # Secondary Storage & Collection Logs
        self.conn = psycopg2.connect(os.getenv('POSTGRES_URL'))
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

        # Primary Storage: Cassandra
        self.cass_session = None
        self._init_cassandra()
        self._ensure_pg_table()

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
            print('[iborrowdesk] Cassandra connected ✓')
        except Exception as e:
            print(f'[iborrowdesk] Cassandra unavailable: {e}')
            print('[iborrowdesk] Will use PostgreSQL fallback only.')
            self.cass_session = None

    # ═════════════════════════════════════════════════════════
    # PUBLIC METHODS
    # ═════════════════════════════════════════════════════════

    def collect(self, ticker: str) -> dict:
        """
        Main entry point. Scrapes current borrow rate for one ticker.

        Returns dict with:
          borrow_rate        — annualized rate in % (e.g. 47.2)
          available_shares   — shares available to short (e.g. 125000)
          borrow_fee_tier    — cheap/normal/expensive/hard_to_borrow/extreme
          borrow_rate_delta  — change vs last stored reading
          borrow_rate_7d_avg — 7-day rolling average from history
          is_hard_to_borrow  — bool flag for ML feature
        """
        start = time.time()
        ticker = ticker.upper().strip()

        raw = self._scrape(ticker)
        if raw.get('error'):
            self._log(ticker, 'failed', 0,
                      raw.get('error_message'),
                      int((time.time() - start) * 1000))
            return raw

        # Enrich with computed fields
        raw['collected_at'] = datetime.now()
        raw['trade_date'] = date.today()
        raw['borrow_fee_tier'] = classify_borrow_rate(raw['borrow_rate'])
        raw['is_hard_to_borrow'] = raw['borrow_rate'] >= 75
        raw['borrow_rate_delta'] = self._compute_delta(ticker, raw['borrow_rate'])
        raw['borrow_rate_7d_avg'] = self._compute_7d_avg(ticker)

        # Save to Cassandra (primary history)
        self._save_cassandra(ticker, raw)

        # Save to PostgreSQL (latest snapshot only)
        self._save_postgres_snapshot(ticker, raw)

        duration = int((time.time() - start) * 1000)
        self._log(ticker, 'success', 1, None, duration)

        print(
            f'  ✓ {ticker} borrow rate: {raw["borrow_rate"]}% '
            f'({raw["borrow_fee_tier"]}) | '
            f'avail: {raw["available_shares"]:,} | '
            f'delta: {raw["borrow_rate_delta"]:+.1f}% | '
            f'storage: {"Cassandra" if self.cass_session else "PostgreSQL"}'
        )
        return raw

    def collect_batch(self, tickers: list, delay_sec: float = 1.5) -> list:
        """
        Collects borrow rates for multiple tickers.
        Adds delay between requests to avoid being blocked.
        """
        results = []
        for i, ticker in enumerate(tickers):
            result = self.collect(ticker)
            results.append(result)
            if i < len(tickers) - 1:
                time.sleep(delay_sec)
        return results

    def get_history(self, ticker: str, days: int = 30) -> list:
        """
        Returns stored borrow rate history from Cassandra (primary)
        or PostgreSQL fallback. Used to compute borrow_rate_delta and 7d_avg.
        """
        if self.cass_session:
            return self._history_from_cassandra(ticker, days)
        return self._history_from_postgres(ticker, days)

    # ═════════════════════════════════════════════════════════
    # SCRAPING
    # ═════════════════════════════════════════════════════════

    def _scrape(self, ticker: str) -> dict:
        """
        Fetch borrow data from iBorrowDesk's JSON API.

        Endpoint:
            https://www.iborrowdesk.com/api/ticker/{ticker}

        Returns the latest available daily borrow fee/rate.
        """
        ticker = ticker.upper().strip()
        url = f"https://www.iborrowdesk.com/api/ticker/{ticker}"

        try:
            response = self.session.get(url, timeout=15)

            if response.status_code == 404:
                return {
                    "ticker": ticker,
                    "error": True,
                    "error_message": "ticker_not_found",
                }

            if response.status_code != 200:
                return {
                    "ticker": ticker,
                    "error": True,
                    "error_message": f"http_{response.status_code}",
                }

            try:
                data = response.json()
            except ValueError:
                return {
                    "ticker": ticker,
                    "error": True,
                    "error_message": "invalid_json",
                }

            daily = data.get("daily", [])

            if not daily:
                return {
                    "ticker": ticker,
                    "error": True,
                    "error_message": "no_daily_data",
                }

            # Find the most recent record that actually has a fee.
            latest = None
            for record in reversed(daily):
                if record.get("fee") is not None:
                    latest = record
                    break

            if latest is None:
                return {
                    "ticker": ticker,
                    "error": True,
                    "error_message": "no_borrow_rate",
                }

            borrow_rate = float(latest["fee"])
            available = latest.get("available")

            if available is not None:
                available = int(available)

            return {
                "ticker": ticker,
                "borrow_rate": round(borrow_rate, 4),
                "available_shares": available or 0,
                "date": latest.get("date"),
                "rebate": latest.get("rebate"),
                "country": data.get("country"),
                "available_stale": data.get("available_stale", False),
                "error": False,
            }

        except requests.RequestException as e:
            return {
                "ticker": ticker,
                "error": True,
                "error_message": f"request_error: {str(e)}",
            }

        except Exception as e:
            return {
                "ticker": ticker,
                "error": True,
                "error_message": f"parse_error: {str(e)}",
            }

    def _parse_table(self, soup: BeautifulSoup):
        """Primary parser fallback — reads HTML table rows."""
        borrow_rate = None
        available   = None

        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue
                label = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)

                if 'fee rate' in label or 'rate' in label:
                    rate = self._parse_pct(value)
                    if rate is not None:
                        borrow_rate = rate

                if 'available' in label:
                    avail = self._parse_int(value)
                    if avail is not None:
                        available = avail

        return borrow_rate, available

    def _parse_regex(self, html: str):
        """Fallback parser — regex on raw HTML."""
        borrow_rate = None
        available   = None

        rate_match = re.search(
            r'(?:fee\s*rate|borrow\s*rate)[^0-9]*(\d+\.?\d*)\s*%',
            html, re.IGNORECASE
        )
        if rate_match:
            borrow_rate = float(rate_match.group(1))

        avail_match = re.search(
            r'available[^0-9]*([0-9,]+)',
            html, re.IGNORECASE
        )
        if avail_match:
            available = int(avail_match.group(1).replace(',', ''))

        return borrow_rate, available

    @staticmethod
    def _parse_pct(text: str):
        """Extracts float from strings like '47.25%' or '47.25'."""
        try:
            return float(text.replace('%', '').replace(',', '').strip())
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _parse_int(text: str):
        """Extracts int from strings like '125,000' or '1.2M'."""
        text = text.replace(',', '').strip().upper()
        try:
            if text.endswith('M'):
                return int(float(text[:-1]) * 1_000_000)
            if text.endswith('K'):
                return int(float(text[:-1]) * 1_000)
            return int(float(text))
        except (ValueError, AttributeError):
            return None

    # ═════════════════════════════════════════════════════════
    # DERIVED COMPUTATIONS
    # ═════════════════════════════════════════════════════════

    def _compute_delta(self, ticker: str, current_rate: float) -> float:
        """
        Returns change in borrow rate vs previous reading.
        Positive = rate increasing (shorts piling in).
        Negative = rate decreasing (shorts covering).
        """
        history = self.get_history(ticker, days=2)
        if len(history) < 1:
            return 0.0
        prev = history[0].get('borrow_rate')
        if prev is None or prev == 0:
            return 0.0
        return round(current_rate - prev, 4)

    def _compute_7d_avg(self, ticker: str) -> float:
        """Returns 7-day rolling average of borrow rate."""
        history = self.get_history(ticker, days=7)
        if not history:
            return 0.0
        rates = [h['borrow_rate'] for h in history if h.get('borrow_rate') is not None]
        if not rates:
            return 0.0
        return round(sum(rates) / len(rates), 4)

    # ═════════════════════════════════════════════════════════
    # CASSANDRA STORAGE (PRIMARY)
    # ═════════════════════════════════════════════════════════

    def _save_cassandra(self, ticker: str, data: dict):
        """Saves time-series snapshot to Cassandra primary table."""
        if not self.cass_session:
            self._save_postgres_history(ticker, data)
            return

        try:
            self.cass_session.execute("""
                INSERT INTO borrow_rate_history (
                    ticker, date, ts,
                    borrow_rate, available_shares,
                    borrow_fee_tier, is_hard_to_borrow,
                    borrow_rate_delta, source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                ticker,
                data['trade_date'],
                data['collected_at'],
                data['borrow_rate'],
                data['available_shares'],
                data['borrow_fee_tier'],
                data['is_hard_to_borrow'],
                data['borrow_rate_delta'],
                'iborrowdesk',
            ))
        except Exception as e:
            print(f'  [iborrowdesk] Cassandra write error: {e}')
            self._save_postgres_history(ticker, data)

    def _history_from_cassandra(self, ticker: str, days: int) -> list:
        """Fetches history records from Cassandra primary storage."""
        results = []
        try:
            for i in range(days):
                d = date.today() - timedelta(days=i)
                rows = self.cass_session.execute("""
                    SELECT borrow_rate, available_shares, ts, borrow_fee_tier
                    FROM borrow_rate_history
                    WHERE ticker = %s AND date = %s
                    LIMIT 3
                """, (ticker, d))
                for row in rows:
                    results.append({
                        'borrow_rate': row.borrow_rate,
                        'available_shares': row.available_shares,
                        'collected_at': row.ts,
                        'borrow_fee_tier': row.borrow_fee_tier,
                    })
        except Exception as e:
            print(f'  [iborrowdesk] Cassandra read error: {e}')
            return self._history_from_postgres(ticker, days)

        return results

    # ═════════════════════════════════════════════════════════
    # POSTGRESQL STORAGE (SECONDARY & FALLBACK)
    # ═════════════════════════════════════════════════════════

    def _save_postgres_snapshot(self, ticker: str, data: dict):
        """Updates stock_snapshots safely after guaranteeing ticker exists in stocks."""
        cur = self.conn.cursor()
        try:
            # 1. Guarantee master 'stocks' table entry to prevent FK constraint failure
            cur.execute("""
                INSERT INTO stocks (ticker)
                VALUES (%s)
                ON CONFLICT (ticker) DO NOTHING
            """, (ticker,))

            # 2. Insert into stock_snapshots
            cur.execute("""
                INSERT INTO stock_snapshots
                    (ticker, borrow_rate, data_source, collected_at)
                VALUES (%s, %s, %s, %s)
            """, (
                ticker,
                data['borrow_rate'],
                'iborrowdesk',
                data['collected_at'],
            ))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f'  [iborrowdesk] Postgres snapshot error: {e}')

    def _save_postgres_history(self, ticker: str, data: dict):
        """Fallback table write if Cassandra is offline."""
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO borrow_rate_snapshots (
                ticker, borrow_rate, available_shares,
                borrow_fee_tier, borrow_rate_delta,
                is_hard_to_borrow, collected_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            ticker,
            data['borrow_rate'],
            data['available_shares'],
            data['borrow_fee_tier'],
            data['borrow_rate_delta'],
            data['is_hard_to_borrow'],
            data['collected_at'],
        ))
        self.conn.commit()

    def _history_from_postgres(self, ticker: str, days: int) -> list:
        """Fetches history from PostgreSQL if Cassandra is down."""
        cur = self.conn.cursor()
        try:
            cur.execute("""
                SELECT borrow_rate, available_shares, collected_at
                FROM   borrow_rate_snapshots
                WHERE  ticker      = %s
                  AND  collected_at > NOW() - INTERVAL '%s days'
                ORDER  BY collected_at DESC
            """, (ticker, days))
            rows = cur.fetchall()
            return [
                {
                    'borrow_rate': r[0],
                    'available_shares': r[1],
                    'collected_at': r[2],
                }
                for r in rows
            ]
        except Exception:
            return []

    # ═════════════════════════════════════════════════════════
    # LOGGING & DDL
    # ═════════════════════════════════════════════════════════

    def _log(self, ticker: str, status: str, rows: int, error: str, duration_ms: int):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO collection_log
                (ticker, source, status, rows_collected, error_message, duration_ms)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (ticker, 'iborrowdesk', status, rows, error, duration_ms))
        self.conn.commit()

    def _ensure_cassandra_table(self):
        try:
            self.cass_session.execute('SELECT COUNT(*) FROM borrow_rate_history LIMIT 1')
        except Exception as e:
            print(f'  [iborrowdesk] Cassandra table check: {e}')

    def _ensure_pg_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS borrow_rate_snapshots (
                id                SERIAL PRIMARY KEY,
                ticker            VARCHAR(10) NOT NULL,
                borrow_rate       FLOAT,
                available_shares  BIGINT,
                borrow_fee_tier   VARCHAR(30),
                borrow_rate_delta FLOAT,
                is_hard_to_borrow BOOLEAN,
                collected_at      TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_borrow_ticker_time
            ON borrow_rate_snapshots (ticker, collected_at DESC)
        """)
        self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    c = IBorrowDeskCollector()

    for ticker in ['GME', 'AMC', 'AAPL', 'FAKE999']:
        print(f'\nTesting {ticker}:')
        r = c.collect(ticker)
        if not r.get('error'):
            print(f'  borrow_rate      = {r["borrow_rate"]}%')
            print(f'  available_shares = {r["available_shares"]:,}')
            print(f'  tier             = {r["borrow_fee_tier"]}')
            print(f'  delta            = {r["borrow_rate_delta"]:+.2f}%')
            print(f'  7d_avg           = {r["borrow_rate_7d_avg"]}%')
            print(f'  hard_to_borrow   = {r["is_hard_to_borrow"]}')
        else:
            print(f'  error: {r["error_message"]}')

    c.close()
>>>>>>> b1b833355d4dc64e3a8533a88f4c9a2fa3acf843
