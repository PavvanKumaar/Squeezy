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
from dotenv import load_dotenv

load_dotenv()

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
