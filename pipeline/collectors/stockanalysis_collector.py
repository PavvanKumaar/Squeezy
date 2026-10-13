# pipeline/collectors/stockanalysis_collector.py
#
# Features collected:
#   short_interest_pct    — % of FLOAT sold short (NOT % of shares outstanding)
#   days_to_cover         — short ratio (DTC = SI shares / avg daily volume)
#   short_interest_change — week-over-week SI change, or None if unknown
#   si_stale_days         — computed from page report date if available, else None
#
# Source: https://stockanalysis.com/stocks/{ticker}/statistics/
#
# IMPORTANT — two different SI percentages exist on the page:
#   shortFloat  = 8.47%  ← THIS IS WHAT WE WANT (% of float)
#   shortInt    = 7.74%  ← DO NOT USE (% of shares outstanding)
#   We always extract shortFloat. If ambiguous, we return a parse error.
#
# Staleness: up to 14 days (FINRA bimonthly).
#   si_stale_days is computed from the report date shown on the page
#   when available. Returns None when the date cannot be determined.
#   Downstream code (si_proxy.py) must handle None gracefully.
#
# Storage: PostgreSQL stock_snapshots

import os
import re
import time
import psycopg2
import requests
from datetime import datetime, date
from typing import Optional, Tuple
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# FIX 1: statistics page contains the short selling data
BASE_URL = 'https://stockanalysis.com/stocks/{ticker}/statistics/'

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept':          'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer':         'https://stockanalysis.com/',
}

# Row labels we look for — normalized to lowercase
LABEL_MAP = {
    'short % of float':              'short_interest_pct',
    'short interest % float':        'short_interest_pct',
    'short float':                   'short_interest_pct',
    'short ratio':                   'days_to_cover',
    'short ratio (days to cover)':   'days_to_cover',
    'days to cover':                 'days_to_cover',
}

# These look like SI% but are % of shares outstanding — NEVER use these
SHARES_OUTSTANDING_LABELS = {
    'short % of shares outstanding',
    'short interest % outstanding',
    'short int % of shares out',
    'short interest % of shares out',
}


class StockAnalysisCollector:
    """
    Scrapes short interest data from stockanalysis.com/statistics page.

    Key guarantee: short_interest_pct always represents % of FLOAT,
    never % of shares outstanding. If float-specific data cannot be
    confirmed, the collector returns error=True rather than storing
    the wrong figure.
    """

    def __init__(self):
        self.conn    = psycopg2.connect(os.getenv('POSTGRES_URL'))
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._ensure_schema_compat()

    # =========================================================
    # PUBLIC METHODS
    # =========================================================

    def collect(self, ticker: str) -> dict:
        """
        Scrapes short interest data for one ticker.

        Returns dict with:
          short_interest_pct    — % of float (float, or None on failure)
          days_to_cover         — DTC (float, or None)
          short_interest_change — always None (not on this page)
          si_stale_days         — int from report date, or None
          error                 — bool
          error_message         — str or None
        """
        start  = time.time()
        ticker = ticker.upper().strip()

        raw = self._scrape(ticker)

        if raw.get('error'):
            self._log(ticker, 'failed', 0,
                      raw.get('error_message'),
                      int((time.time()-start)*1000))
            return raw

        if raw.get('short_interest_pct') is None:
            raw['error']         = True
            raw['error_message'] = 'parse_succeeded_but_si_pct_missing'
            self._log(ticker, 'failed', 0,
                      raw['error_message'],
                      int((time.time()-start)*1000))
            return raw

        self._save(ticker, raw)
        self._log(ticker, 'success', 1, None,
                  int((time.time()-start)*1000))

        print(
            f'  ✓ {ticker} SI — '
            f'{raw["short_interest_pct"]}% (float) | '
            f'DTC: {raw["days_to_cover"]} | '
            f'stale_days: {raw["si_stale_days"]}'
        )
        return raw

    def collect_batch(self, tickers: list,
                      delay_sec: float = 2.0) -> list:
        results = []
        for i, ticker in enumerate(tickers):
            results.append(self.collect(ticker))
            if i < len(tickers) - 1:
                time.sleep(delay_sec)
        return results

    def get_latest(self, ticker: str) -> dict:
        """Returns most recent stored SI row from PostgreSQL."""
        cur = self.conn.cursor()
        try:
            cur.execute("""
                SELECT short_interest_pct,
                       days_to_cover,
                       si_stale_days,
                       data_source,
                       collected_at
                FROM   stock_snapshots
                WHERE  ticker              = %s
                  AND  short_interest_pct IS NOT NULL
                ORDER  BY collected_at DESC
                LIMIT  1
            """, (ticker,))
            row = cur.fetchone()
            if not row:
                return {}
            return {
                'short_interest_pct': float(row[0]),
                'days_to_cover':      float(row[1]) if row[1] is not None else None,
                'si_stale_days':      int(row[2])   if row[2] is not None else None,
                'data_source':        row[3],
                'collected_at':       row[4].isoformat(),
            }
        except Exception as e:
            return {'error': str(e)}

    # =========================================================
    # SCRAPING
    # =========================================================

    def _scrape(self, ticker: str) -> dict:
        url    = BASE_URL.format(ticker=ticker.lower())
        result = {
            'ticker':               ticker,
            'short_interest_pct':   None,
            'days_to_cover':        None,
            'short_interest_change': None,   # FIX 3: not None=0, not measured
            'si_stale_days':        None,    # FIX 4: not hardcoded 14
            'data_source':          'stockanalysis',
            'source_url':           url,
            'collected_at':         datetime.utcnow(),
            'error':                False,
            'error_message':        None,
        }

        try:
            resp = self.session.get(url, timeout=15)

            if resp.status_code == 404:
                result.update({'error': True,
                                'error_message': 'ticker_not_found'})
                return result

            if resp.status_code != 200:
                result.update({'error': True,
                                'error_message': f'http_{resp.status_code}'})
                return result

            soup = BeautifulSoup(resp.text, 'html.parser')

            # FIX 2: statistics page parser
            si, dtc, report_date = self._parse_statistics_page(soup)

            # Regex fallback if table parse found nothing
            if si is None:
                si, dtc = self._parse_regex_fallback(resp.text)

            if si is None:
                result.update({'error': True,
                                'error_message': 'parse_failed_no_si_found'})
                return result

            result['short_interest_pct'] = round(float(si), 4)
            if dtc is not None:
                result['days_to_cover'] = round(float(dtc), 4)

            # Compute stale days only when we have a real report date
            if report_date is not None:
                result['si_stale_days'] = (date.today() - report_date).days

            return result

        except requests.exceptions.Timeout:
            result.update({'error': True, 'error_message': 'timeout'})
            return result
        except Exception as e:
            result.update({'error': True, 'error_message': str(e)})
            return result

    # =========================================================
    # FIX 2 — Statistics page parser
    # =========================================================

    def _parse_statistics_page(
        self, soup: BeautifulSoup
    ) -> Tuple[Optional[float], Optional[float], Optional[date]]:
        """
        Parses the /statistics/ page for short interest data.

        Scans all <tr> rows. Normalises label text to lowercase
        and matches against LABEL_MAP. Explicitly rejects rows in
        SHARES_OUTSTANDING_LABELS so we never store the wrong metric.

        Returns (short_interest_pct, days_to_cover, report_date).
        Any value may be None when not found.
        """
        si   = None
        dtc  = None
        report_date = None

        for row in soup.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue

            label = cells[0].get_text(separator=' ', strip=True).lower()
            value = cells[1].get_text(strip=True)

            # Reject shares-outstanding metric
            if label in SHARES_OUTSTANDING_LABELS:
                continue

            mapped = LABEL_MAP.get(label)

            if mapped == 'short_interest_pct' and si is None:
                parsed = self._parse_pct(value)
                if parsed is not None:
                    si = parsed

            elif mapped == 'days_to_cover' and dtc is None:
                parsed = self._parse_float(value)
                if parsed is not None:
                    dtc = parsed

        report_date = self._extract_report_date(soup)
        return si, dtc, report_date

    def _extract_report_date(
        self, soup: BeautifulSoup
    ) -> Optional[date]:
        """
        Looks for a report date in page text.
        Returns date object or None — never fabricates a date.
        """
        text = soup.get_text(separator=' ', strip=True)

        patterns = [
            (r'(?:as of|updated|reported)[:\s]+([A-Z][a-z]{2}\s+\d{1,2},?\s+\d{4})',
             '%b %d %Y'),
            (r'(?:as of|updated)[:\s]+(\d{1,2}/\d{1,2}/\d{4})',
             '%m/%d/%Y'),
            (r'(?:as of|updated)[:\s]+(\d{4}-\d{2}-\d{2})',
             '%Y-%m-%d'),
        ]

        for pattern, fmt in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                raw_date = match.group(1).replace(',', '').strip()
                try:
                    return datetime.strptime(raw_date, fmt).date()
                except ValueError:
                    continue

        return None

    # =========================================================
    # REGEX FALLBACK
    # =========================================================

    def _parse_regex_fallback(
        self, html: str
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Targeted field-level regexes for embedded JS-like data.
        Extracts shortFloat (% of float) only.
        Does NOT attempt json.loads() on the JS object.
        Does NOT extract shortInt (% of shares outstanding).
        """
        si  = None
        dtc = None

        # shortFloat:"8.47%" — % of float, which we want
        sf = re.search(
            r'shortFloat\s*[:"\']+\s*([\d.]+)\s*%',
            html, re.IGNORECASE)
        if sf:
            try:
                val = float(sf.group(1))
                if 0 < val <= 100:
                    si = val
            except ValueError:
                pass

        # shortRatio:"5.27"
        sr = re.search(
            r'shortRatio\s*[:"\']+\s*([\d.]+)',
            html, re.IGNORECASE)
        if sr:
            try:
                val = float(sr.group(1))
                if 0 < val <= 100:
                    dtc = val
            except ValueError:
                pass

        return si, dtc

    # =========================================================
    # PARSING HELPERS
    # =========================================================

    @staticmethod
    def _parse_pct(text: str) -> Optional[float]:
        """
        Extracts % of float from strings like '8.47%' or '0.0847'.
        Returns None if value is implausible for SI% of float.
        """
        try:
            cleaned = re.sub(r'[^\d.]', '', str(text).strip())
            if not cleaned:
                return None
            val = float(cleaned)
            if val < 1.0:       # decimal form → convert to %
                val = val * 100
            if 0 < val <= 100:  # plausible SI% range
                return val
            return None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_float(text: str) -> Optional[float]:
        try:
            cleaned = re.sub(r'[^\d.]', '', str(text).strip())
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None

    # =========================================================
    # DATABASE
    # =========================================================

    def _save(self, ticker: str, data: dict):
        """
        Saves SI snapshot to PostgreSQL.
        si_stale_days stored as NULL when unknown (not as 14).
        short_interest_change not saved (it is None — not measured).
        """
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO stock_snapshots (
                ticker,
                short_interest_pct,
                days_to_cover,
                si_stale_days,
                data_source,
                collected_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            ticker,
            data.get('short_interest_pct'),
            data.get('days_to_cover'),
            data.get('si_stale_days'),    # NULL when unknown
            data.get('data_source', 'stockanalysis'),
            data.get('collected_at', datetime.utcnow()),
        ))
        self.conn.commit()

    def _log(self, ticker: str, status: str,
             rows: int, error: str, duration_ms: int):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO collection_log
                (ticker, source, status, rows_collected,
                 error_message, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (ticker, 'stockanalysis', status,
              rows, error, duration_ms))
        self.conn.commit()

    def _ensure_schema_compat(self):
        """Makes si_stale_days nullable if it was NOT NULL before."""
        cur = self.conn.cursor()
        try:
            cur.execute("""
                ALTER TABLE stock_snapshots
                ALTER COLUMN si_stale_days DROP NOT NULL
            """)
            self.conn.commit()
        except Exception:
            self.conn.rollback()

    def close(self):
        self.conn.close()


# ---------------------------------------------------------
if __name__ == '__main__':
    collector = StockAnalysisCollector()

    print('='*55)
    print('  StockAnalysis Collector — Fixed Version Test')
    print('='*55)

    for ticker in ['GME', 'AMC', 'AAPL', 'XYZFAKE']:
        print(f'\n── {ticker} ──')
        result = collector.collect(ticker)

        if not result.get('error'):
            print(f'  short_interest_pct:    {result["short_interest_pct"]}%')
            print(f'  days_to_cover:         {result["days_to_cover"]}')
            print(f'  short_interest_change: {result["short_interest_change"]}  <- None = not measured')
            print(f'  si_stale_days:         {result["si_stale_days"]}  <- None = date unknown')
        else:
            print(f'  error: {result["error_message"]}')

    print('\n── Latest stored (GME) ──')
    print(collector.get_latest('GME'))

    collector.close()
    print('\n✓ Test complete')