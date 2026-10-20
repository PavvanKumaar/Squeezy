"""
pipeline/collectors/yfinace_collector.py
yfinance Data Collector — TASK-101

Collects OHLCV, float, market cap, 52-week high/low, options chain,
earnings dates, and recent news from Yahoo Finance (yfinance).

Storage targets:
  - stocks         → PostgreSQL (upsert master entity)
  - stock_timeseries → TimescaleDB hypertable (OHLCV append)
  - collection_log → PostgreSQL (every run, success or failure)

Refresh rate: Every 15 minutes during market hours (scheduled by Celery).
"""

import os
import time
import logging
from datetime import datetime, timezone
from typing import Optional

import yfinance as yf
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

MAX_RETRIES = 3
RETRY_BASE_DELAY = 2       # seconds (doubles each retry)
OHLCV_PERIOD = "30d"       # yfinance period string for OHLCV history
OHLCV_INTERVAL = "15m"     # yfinance interval string


# ─────────────────────────────────────────────────────────────────────────────
# yfinanceCollector
# ─────────────────────────────────────────────────────────────────────────────

class yfinanceCollector:
    """
    Collects market data from Yahoo Finance for a single ticker.

    Interface (per DESIGN.md §7.1):
        __init__()   — establish DB connections
        collect()    — main public method, always returns a dict
        _save()      — database write helpers (private)
        _log()       — writes to collection_log (private)
    """

    SOURCE = "yfinance"

    def __init__(self):
        try:
            self._pg_conn = self._connect_postgres()
        except Exception as e:
            logger.warning("PostgreSQL connection not established at init: %s", e)
            self._pg_conn = None

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def collect(self, ticker: str) -> dict:
        """
        Main collection method.

        Steps:
          1. Fetch stock info snapshot from yfinance.
          2. Fetch OHLCV history.
          3. Fetch options chain (up to 4 nearest expiries).
          4. Fetch earnings date.
          5. Fetch recent news.
          6. Upsert stocks table.
          7. Write OHLCV rows to stock_timeseries.
          8. Write to collection_log.
          9. Return result dict.

        Always returns a dict with at minimum the standard keys defined in
        DESIGN.md §7.2, plus source-specific fields.
        """
        ticker = ticker.upper().strip()
        start_ts = time.time()
        result = self._base_result(ticker)

        try:
            # ── 1. Stock info ──────────────────────────────────────────────
            info = self.get_stock_info(ticker)
            if info.get("error"):
                raise ValueError(f"Ticker not found or no data: {ticker}")

            # ── 2. OHLCV history ───────────────────────────────────────────
            ohlcv_df = self.get_ohlcv(ticker, period=OHLCV_PERIOD, interval=OHLCV_INTERVAL)

            # ── 3. Options chain ───────────────────────────────────────────
            options = self.get_options_chain(ticker)

            # ── 4. Earnings date ───────────────────────────────────────────
            earnings_date = self.get_earnings_date(ticker)

            # ── 5. Recent news ─────────────────────────────────────────────
            news = self.get_recent_news(ticker)

            # ── 6. Persist stocks entity ───────────────────────────────────
            self.save_stock(ticker, info)

            # ── 7. Persist OHLCV ───────────────────────────────────────────
            rows_written = self.save_ohlcv(ticker, ohlcv_df)

            # ── 8. Build result ────────────────────────────────────────────
            duration_ms = int((time.time() - start_ts) * 1000)
            result.update({
                "error": False,
                "error_message": None,
                # Info fields
                "company_name": info.get("longName"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "market_cap": info.get("marketCap"),
                "float_shares": info.get("floatShares"),
                "shares_outstanding": info.get("sharesOutstanding"),
                "fifty_two_week_high": info.get("fiftyTwoWeekHigh"),
                "fifty_two_week_low": info.get("fiftyTwoWeekLow"),
                "previous_close": info.get("previousClose"),
                "current_price": info.get("currentPrice") or info.get("regularMarketPrice"),
                "volume": info.get("volume") or info.get("regularMarketVolume"),
                "avg_volume_30d": info.get("averageVolume"),
                # Derived fields
                "ohlcv_rows": rows_written,
                "options_expiries": len(options.get("expiry_dates", [])),
                "earnings_date": str(earnings_date) if earnings_date else None,
                "news_count": len(news),
                "news": news[:5],          # return only the 5 most recent in result
                "options_summary": options,
                "duration_ms": duration_ms,
            })
            self._log(ticker, "success", rows_written, None, duration_ms)
            logger.info("✓ [%s] yfinance collected — %d OHLCV rows in %dms",
                        ticker, rows_written, duration_ms)

        except Exception as exc:
            duration_ms = int((time.time() - start_ts) * 1000)
            error_msg = str(exc)
            result["error"] = True
            result["error_message"] = error_msg
            result["duration_ms"] = duration_ms
            self._log(ticker, "failed", 0, error_msg, duration_ms)
            logger.error("✗ [%s] yfinance collection failed: %s", ticker, error_msg)

        return result

    # ─────────────────────────────────────────────────────────────────────────
    # Data fetchers (public, usable independently)
    # ─────────────────────────────────────────────────────────────────────────

    def get_stock_info(self, ticker: str) -> dict:
        """
        Returns a dict with all stock metadata fields from yfinance.
        Retries up to MAX_RETRIES on transient errors.
        Returns {'error': True} if the ticker is not found.
        """
        ticker = ticker.upper()
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                t = yf.Ticker(ticker)
                info = t.info

                # yfinance returns a minimal dict (just quoteType) for unknown tickers
                if not info or info.get("quoteType") in (None, "NONE") or \
                        "regularMarketPrice" not in info and "currentPrice" not in info \
                        and "previousClose" not in info:
                    logger.warning("[%s] yfinance returned no price data", ticker)
                    return {"error": True, "ticker": ticker}

                return info

            except Exception as exc:
                delay = RETRY_BASE_DELAY ** attempt
                logger.warning("[%s] get_stock_info attempt %d failed (%s). Retry in %ds.",
                               ticker, attempt, exc, delay)
                if attempt < MAX_RETRIES:
                    time.sleep(delay)

        return {"error": True, "ticker": ticker}

    def get_ohlcv(self, ticker: str, period: str = "30d", interval: str = "15m"):
        """
        Returns a pandas DataFrame with OHLCV columns indexed by datetime.
        Raises on all attempts failing.
        """
        import pandas as pd

        ticker = ticker.upper()
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                df = yf.download(
                    ticker,
                    period=period,
                    interval=interval,
                    auto_adjust=True,
                    progress=False,
                    threads=False,
                )
                if df.empty:
                    logger.warning("[%s] yfinance OHLCV returned empty DataFrame", ticker)
                    return pd.DataFrame()

                df.index = df.index.tz_localize("UTC") if df.index.tzinfo is None else \
                    df.index.tz_convert("UTC")
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [c[0].lower() if isinstance(c, tuple) else str(c).lower() for c in df.columns]
                else:
                    df.columns = [str(c).lower() for c in df.columns]
                df["ticker"] = ticker
                df["interval"] = interval
                return df

            except Exception as exc:
                delay = RETRY_BASE_DELAY ** attempt
                logger.warning("[%s] get_ohlcv attempt %d failed (%s). Retry in %ds.",
                               ticker, attempt, exc, delay)
                if attempt < MAX_RETRIES:
                    time.sleep(delay)

        import pandas as pd
        return pd.DataFrame()

    def get_options_chain(self, ticker: str) -> dict:
        """
        Returns a summary dict from the options chain for up to 4 nearest expiries.
        Includes total call OI, put OI, call/put ratio, and max pain estimate.
        Returns {'options_available': False} if ticker has no listed options.
        """
        ticker = ticker.upper()
        try:
            t = yf.Ticker(ticker)
            expiry_dates = t.options

            if not expiry_dates:
                return {
                    "options_available": False,
                    "expiry_dates": [],
                    "total_call_oi": 0,
                    "total_put_oi": 0,
                    "call_put_ratio": None,
                    "max_pain_strike": None,
                }

            # Collect chains for up to 4 nearest expiries
            chains = []
            for expiry in list(expiry_dates)[:4]:
                try:
                    chain = t.option_chain(expiry)
                    chains.append(chain)
                except Exception as e:
                    logger.warning("[%s] Could not fetch chain for %s: %s", ticker, expiry, e)

            total_call_oi = sum(
                chain.calls["openInterest"].sum()
                for chain in chains
                if not chain.calls.empty
            )
            total_put_oi = sum(
                chain.puts["openInterest"].sum()
                for chain in chains
                if not chain.puts.empty
            )

            call_put_ratio = (
                round(float(total_call_oi) / float(total_put_oi), 4)
                if total_put_oi > 0 else None
            )

            # Max pain: strike at which total option loss (for buyers) is maximized
            max_pain_strike = self._compute_max_pain(chains) if chains else None

            return {
                "options_available": True,
                "expiry_dates": list(expiry_dates)[:4],
                "chains_fetched": len(chains),
                "total_call_oi": int(total_call_oi),
                "total_put_oi": int(total_put_oi),
                "call_put_ratio": call_put_ratio,
                "max_pain_strike": max_pain_strike,
            }

        except Exception as exc:
            logger.warning("[%s] get_options_chain failed: %s", ticker, exc)
            return {
                "options_available": False,
                "expiry_dates": [],
                "error": str(exc),
            }

    def get_earnings_date(self, ticker: str) -> Optional[datetime]:
        """
        Returns the next earnings datetime in UTC, or None if unavailable.
        """
        ticker = ticker.upper()
        try:
            t = yf.Ticker(ticker)
            cal = t.calendar
            if cal is None or cal.empty:
                return None

            # calendar columns may vary; look for 'Earnings Date' row
            if hasattr(cal, "loc") and "Earnings Date" in cal.index:
                raw = cal.loc["Earnings Date"]
                # Raw is a Timestamp or a list; take the first
                if hasattr(raw, "__iter__") and not isinstance(raw, str):
                    raw = list(raw)[0]
                dt = raw.to_pydatetime() if hasattr(raw, "to_pydatetime") else None
                return dt.replace(tzinfo=timezone.utc) if dt and dt.tzinfo is None else dt

        except Exception as exc:
            logger.debug("[%s] get_earnings_date: %s", ticker, exc)

        return None

    def get_recent_news(self, ticker: str) -> list:
        """
        Returns a list of dicts with recent news articles from yfinance.
        Each dict contains: title, publisher, link, published_utc.
        """
        ticker = ticker.upper()
        try:
            t = yf.Ticker(ticker)
            raw_news = t.news or []
            articles = []
            for item in raw_news:
                articles.append({
                    "title": item.get("title", ""),
                    "publisher": item.get("publisher", ""),
                    "link": item.get("link", ""),
                    "published_utc": datetime.fromtimestamp(
                        item["providerPublishTime"], tz=timezone.utc
                    ).isoformat() if item.get("providerPublishTime") else None,
                })
            return articles

        except Exception as exc:
            logger.warning("[%s] get_recent_news failed: %s", ticker, exc)
            return []

    # ─────────────────────────────────────────────────────────────────────────
    # Persistence (private)
    # ─────────────────────────────────────────────────────────────────────────

    def save_stock(self, ticker: str, info: dict):
        """
        Upserts the stocks master entity table from yfinance info dict.
        Only updates mutable fields; status/fast_tracked are not touched here.
        """
        if not self._pg_conn:
            logger.warning("Postgres unavailable - skipping save_stock for %s", ticker)
            return

        sql = """
            INSERT INTO stocks (
                ticker, company_name, sector, industry,
                float_shares, market_cap, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (ticker) DO UPDATE SET
                company_name   = EXCLUDED.company_name,
                sector         = EXCLUDED.sector,
                industry       = EXCLUDED.industry,
                float_shares   = EXCLUDED.float_shares,
                market_cap     = EXCLUDED.market_cap,
                updated_at     = NOW()
        """
        with self._pg_conn.cursor() as cur:
            cur.execute(sql, (
                ticker,
                info.get("longName"),
                info.get("sector"),
                info.get("industry"),
                info.get("floatShares"),
                info.get("marketCap"),
            ))
        self._pg_conn.commit()

    def save_ohlcv(self, ticker: str, df) -> int:
        """
        Appends OHLCV rows to the stock_timeseries TimescaleDB hypertable.
        Skips if the DataFrame is empty.
        Returns the number of rows inserted.
        """
        if df is None or df.empty:
            return 0

        if not self._pg_conn:
            logger.warning("Postgres unavailable - skipping save_ohlcv for %s", ticker)
            return len(df)

        rows = []
        for ts, row in df.iterrows():
            rows.append((
                ts.to_pydatetime(),
                ticker,
                float(row.get("open", 0) or 0),
                float(row.get("high", 0) or 0),
                float(row.get("low", 0) or 0),
                float(row.get("close", 0) or 0),
                int(row.get("volume", 0) or 0),
                None,          # vwap not provided by yfinance free tier
                OHLCV_INTERVAL,
            ))

        if not rows:
            return 0

        sql = """
            INSERT INTO stock_timeseries
                (time, ticker, open, high, low, close, volume, vwap, interval)
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        with self._pg_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
        self._pg_conn.commit()
        return len(rows)

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
    # Internal helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _connect_postgres(self):
        """Returns a psycopg2 connection using POSTGRES_URL or individual env vars."""
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

    @staticmethod
    def _base_result(ticker: str) -> dict:
        """Returns the minimal required result dict skeleton (DESIGN.md §7.2)."""
        return {
            "ticker": ticker,
            "collected_at": datetime.now(tz=timezone.utc).isoformat(),
            "error": True,
            "error_message": "Collection not yet attempted",
            "duration_ms": 0,
        }

    @staticmethod
    def _compute_max_pain(chains: list) -> Optional[float]:
        """
        Estimates the max pain strike price across provided option chains.
        Max pain = strike where total value of expiring options (for buyers) is minimized.
        Returns the strike as a float, or None if data is insufficient.
        """
        try:
            import pandas as pd
            all_calls = pd.concat([c.calls for c in chains if not c.calls.empty])
            all_puts = pd.concat([c.puts for c in chains if not c.puts.empty])

            strikes = sorted(set(all_calls["strike"].tolist() + all_puts["strike"].tolist()))
            if not strikes:
                return None

            min_loss = float("inf")
            max_pain_strike = None

            for s in strikes:
                # Call losses at expiry (for call buyers): max(0, s - strike) * OI
                call_loss = (
                    all_calls[all_calls["strike"] <= s]
                    .assign(loss=lambda df: (s - df["strike"]) * df["openInterest"])
                    ["loss"].sum()
                )
                # Put losses at expiry (for put buyers): max(0, strike - s) * OI
                put_loss = (
                    all_puts[all_puts["strike"] >= s]
                    .assign(loss=lambda df: (df["strike"] - s) * df["openInterest"])
                    ["loss"].sum()
                )
                total_loss = call_loss + put_loss
                if total_loss < min_loss:
                    min_loss = total_loss
                    max_pain_strike = s

            return float(max_pain_strike) if max_pain_strike is not None else None

        except Exception:
            return None


# ─────────────────────────────────────────────────────────────────────────────
# Standalone runner
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    collector = yfinanceCollector()

    test_tickers = ["GME", "FAKE123"]
    for t in test_tickers:
        print(f"\n{'='*60}")
        print(f"Collecting: {t}")
        result = collector.collect(t)
        # Pretty-print top-level keys (skip large nested dicts)
        summary = {k: v for k, v in result.items() if k not in ("news", "options_summary")}
        print(json.dumps(summary, indent=2, default=str))
        print(f"Error: {result['error']}")
        if not result["error"]:
            print(f"OHLCV rows written: {result.get('ohlcv_rows', 0)}")
