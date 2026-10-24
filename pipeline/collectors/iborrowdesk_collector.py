import os
import re
import time
import requests
import psycopg2
from datetime import datetime, date, timedelta
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

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
