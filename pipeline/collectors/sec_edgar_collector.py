# pipeline/collectors/sec_edgar_collector.py
#
# Features collected:
#   sec_13d_13g_filing       — activist investor entered (same day)
#   insider_transactions     — Form 4 insider buys/sells (same day)
#   fail_to_deliver (FTD)    — SEC FTD data (2-week lag)
#   institutional_ownership  — 13F quarterly holdings
#
# Sources:
#   EDGAR full-text search: https://efts.sec.gov/LATEST/search-index
#   EDGAR company search:   https://data.sec.gov/submissions/{CIK}.json
#   EDGAR FTD files:        https://www.sec.gov/data/foiadocuments/docs/
#                           fails-to-deliver/
#
# Why each matters:
#   13D/13G — Activist investor (>5% stake) entering a heavily shorted
#              stock is a classic squeeze catalyst. Must be detected same day.
#
#   Form 4  — Insider buying their own stock is a strong bullish signal,
#              especially when combined with high short interest.
#
#   FTD     — Failures to deliver indicate naked shorting. Sustained high
#              FTD levels are a squeeze signal (shorts can't find shares).
#              SEC EDGAR updates FTD data twice a month.
#
#   13F     — Institutional ownership quarterly snapshot. Useful for
#              context: institutions entering or exiting a shorted stock.
#
# All data is FREE and official from the SEC.
# Required header: 'User-Agent: YourName/1.0 your@email.com'
# Rate limit: 10 requests per second (EDGAR enforces this)

import requests
import psycopg2
import os
import time
import json
import csv
import io
import re
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

EDGAR_SEARCH_URL   = 'https://efts.sec.gov/LATEST/search-index'
EDGAR_COMPANY_URL  = 'https://data.sec.gov/submissions/{cik}.json'
EDGAR_FILINGS_URL  = 'https://www.sec.gov/cgi-bin/browse-edgar'
EDGAR_FTD_BASE     = ('https://www.sec.gov/data/foiadocuments/'
                      'docs/fails-to-deliver/')
EDGAR_HEADERS = {
    # REQUIRED by SEC — must identify yourself
    # Replace with your own name and email
    'User-Agent': (
        os.getenv('SEC_USER_AGENT',
                  'SqueezeRadar/1.0 squeezradar@example.com')
    ),
    'Accept': 'application/json',
}


class SECEdgarCollector:
    """
    Collects SEC EDGAR data for squeeze-relevant regulatory events.

    Covers:
      - 13D/13G activist filings       (same day detection)
      - Form 4 insider transactions    (same day detection)
      - Fail-to-deliver data           (2-week lag, twice monthly)
      - 13F institutional ownership    (quarterly, 45-day lag)
    """

    def __init__(self):
        self.conn    = psycopg2.connect(os.getenv('POSTGRES_URL'))
        self.session = requests.Session()
        self.session.headers.update(EDGAR_HEADERS)
        self._ensure_tables()
        self._last_request_time = 0.0

    # ═════════════════════════════════════════════════════════
    # PUBLIC METHODS
    # ═════════════════════════════════════════════════════════

    def collect_all(self, ticker: str) -> dict:
        """
        Runs all SEC collection tasks for one ticker.
        Use this as the daily collection entry point.
        """
        print(f'\n[SEC EDGAR] Collecting for {ticker}...')
        result = {'ticker': ticker, 'errors': []}

        # 1. 13D/13G activist filings
        activist = self.get_activist_filings(ticker, days_back=7)
        result['activist_filings']     = activist
        result['has_activist_filing']  = len(activist) > 0
        result['activist_filing_count'] = len(activist)

        # 2. Form 4 insider transactions
        insiders = self.get_insider_transactions(ticker, days_back=30)
        result['insider_transactions'] = insiders
        result['insider_net_sentiment'] = self._insider_sentiment(
            insiders)

        # 3. FTD data
        ftd = self.get_fail_to_deliver(ticker)
        result['ftd_shares']     = ftd.get('ftd_shares', 0)
        result['ftd_as_pct_vol'] = ftd.get('ftd_as_pct_vol', 0.0)
        result['ftd_tier']       = ftd.get('tier', 'unknown')

        # 4. Save events to DB
        self._save_events(ticker, result)

        print(
            f'  ✓ {ticker} SEC — '
            f'activist: {result["activist_filing_count"]} | '
            f'insider sentiment: {result["insider_net_sentiment"]} | '
            f'FTD: {result["ftd_shares"]:,}'
        )
        return result

    # ─────────────────────────────────────────────────────────
    # 1. 13D / 13G ACTIVIST FILINGS
    # ─────────────────────────────────────────────────────────
    def get_activist_filings(self, ticker: str,
                              days_back: int = 7) -> list:
        """
        Searches EDGAR for recent 13D and 13G filings for a ticker.

        13D — "Schedule 13D" — activist investor with >5% stake
               AND intent to influence management. Most impactful.
        13G — "Schedule 13G" — passive investor with >5% stake.
               Less impactful but still notable.

        Staleness: same day (EDGAR updates within hours of filing)
        """
        start     = time.time()
        since     = (datetime.utcnow()
                     - timedelta(days=days_back)).strftime('%Y-%m-%d')
        filings   = []

        for form_type in ['SC 13D', 'SC 13G', 'SC 13D/A', 'SC 13G/A']:
            self._rate_limit()
            try:
                params = {
                    'q':         f'"{ticker}"',
                    'dateRange': 'custom',
                    'startdt':   since,
                    'forms':     form_type,
                    '_source':   'filing_date,period_of_report,'
                                 'entity_name,file_num',
                }
                resp = self.session.get(
                    EDGAR_SEARCH_URL,
                    params=params,
                    timeout=15
                )
                if resp.status_code != 200:
                    continue

                data = resp.json()
                hits = data.get('hits', {}).get('hits', [])

                for hit in hits:
                    src = hit.get('_source', {})
                    filings.append({
                        'ticker':       ticker,
                        'form_type':    form_type,
                        'filer_name':   src.get('entity_name', ''),
                        'filed_date':   src.get('filing_date', ''),
                        'period':       src.get(
                            'period_of_report', ''),
                        'is_activist':  '13D' in form_type,
                        'collected_at': datetime.utcnow(),
                    })

            except Exception as e:
                print(f'  [SEC] 13D/13G fetch error: {e}')
                continue

        self._log(ticker, 'sec_13d_13g', 'success',
                  len(filings), None,
                  int((time.time()-start)*1000))
        return filings

    # ─────────────────────────────────────────────────────────
    # 2. FORM 4 INSIDER TRANSACTIONS
    # ─────────────────────────────────────────────────────────
    def get_insider_transactions(self, ticker: str,
                                  days_back: int = 30) -> list:
        """
        Fetches Form 4 insider buy/sell transactions.

        Form 4 is filed within 2 business days of a transaction.
        Insider buying while stock has high short interest = strong
        bullish signal. Insider selling during a squeeze = caution.

        Staleness: same day (within 2 business days of transaction)
        """
        start  = time.time()
        since  = (datetime.utcnow()
                  - timedelta(days=days_back)).strftime('%Y-%m-%d')
        txns   = []

        self._rate_limit()
        try:
            params = {
                'q':         f'"{ticker}"',
                'dateRange': 'custom',
                'startdt':   since,
                'forms':     '4',
            }
            resp = self.session.get(
                EDGAR_SEARCH_URL,
                params=params,
                timeout=15
            )

            if resp.status_code == 200:
                data = resp.json()
                hits = data.get('hits', {}).get('hits', [])

                for hit in hits[:20]:  # limit to 20 most recent
                    src = hit.get('_source', {})
                    txns.append({
                        'ticker':      ticker,
                        'filer_name':  src.get('entity_name', ''),
                        'filed_date':  src.get('filing_date', ''),
                        'form_type':   '4',
                        # transaction type determined from filing body
                        # P = Purchase, S = Sale, A = Award
                        'transaction_type': 'unknown',
                        'collected_at': datetime.utcnow(),
                    })

        except Exception as e:
            print(f'  [SEC] Form 4 fetch error: {e}')

        # Supplement with OpenInsider for richer transaction detail
        openinsider = self._scrape_openinsider(ticker)
        txns.extend(openinsider)

        # Deduplicate by filed_date + filer_name
        seen  = set()
        dedup = []
        for t in txns:
            key = (t['filed_date'], t['filer_name'])
            if key not in seen:
                seen.add(key)
                dedup.append(t)

        self._log(ticker, 'sec_form4', 'success',
                  len(dedup), None,
                  int((time.time()-start)*1000))
        return dedup

    # ─────────────────────────────────────────────────────────
    # 3. FAIL TO DELIVER (FTD)
    # ─────────────────────────────────────────────────────────
    def get_fail_to_deliver(self, ticker: str) -> dict:
        """
        Downloads SEC FTD data for a ticker.

        FTD = shares that were sold short but seller failed to deliver
        the actual shares within the T+2 settlement window.
        High FTD = potential naked shorting = squeeze fuel.

        SEC publishes FTD files twice per month:
          1st-15th:  published ~20th of same month
          16th-end:  published ~5th of next month
        Staleness: up to 2 weeks

        File URL pattern:
          https://www.sec.gov/data/foiadocuments/docs/
          fails-to-deliver/cnsfails{YYYYMM}{a|b}.zip
        """
        start = time.time()

        # Try last 3 FTD file periods
        for period in self._recent_ftd_periods(n=3):
            result = self._fetch_ftd_file(ticker, period)
            if result and result.get('ftd_shares', 0) > 0:
                self._log(ticker, 'sec_ftd', 'success', 1,
                          None, int((time.time()-start)*1000))
                return result

        self._log(ticker, 'sec_ftd', 'skipped', 0,
                  'no_recent_ftd', int((time.time()-start)*1000))
        return {
            'ticker':        ticker,
            'ftd_shares':    0,
            'ftd_as_pct_vol': 0.0,
            'tier':          'unknown',
            'error':         False,
        }

    # ─────────────────────────────────────────────────────────
    # 4. INSTITUTIONAL OWNERSHIP (13F)
    # ─────────────────────────────────────────────────────────
    def get_institutional_ownership(self, ticker: str) -> dict:
        """
        Fetches institutional ownership from SEC 13F filings.
        Staleness: quarterly (45-day lag after quarter end)

        Returns approximate % of float held by institutions.
        Very low institutional ownership (<20%) combined with
        high short interest can amplify squeeze dynamics.
        """
        start = time.time()
        self._rate_limit()

        try:
            # Search for 13F-HR filings mentioning the ticker
            params = {
                'q':     f'"{ticker}"',
                'forms': '13F-HR',
                '_source': 'entity_name,filing_date,period_of_report',
            }
            resp = self.session.get(
                EDGAR_SEARCH_URL,
                params=params,
                timeout=15
            )

            if resp.status_code != 200:
                return {'ticker': ticker, 'error': True}

            data = resp.json()
            hits = data.get('hits', {}).get('hits', [])

            institutions = []
            for hit in hits[:10]:
                src = hit.get('_source', {})
                institutions.append({
                    'institution': src.get('entity_name', ''),
                    'filed_date':  src.get('filing_date', ''),
                    'period':      src.get('period_of_report', ''),
                })

            result = {
                'ticker':               ticker,
                'institution_count':    len(institutions),
                'institutions':         institutions,
                'most_recent_13f_date': (
                    institutions[0]['filed_date']
                    if institutions else None
                ),
                'collected_at':         datetime.utcnow(),
                'error':                False,
            }

            self._log(ticker, 'sec_13f', 'success',
                      len(institutions), None,
                      int((time.time()-start)*1000))
            return result

        except Exception as e:
            self._log(ticker, 'sec_13f', 'failed', 0,
                      str(e), int((time.time()-start)*1000))
            return {'ticker': ticker, 'error': True,
                    'error_message': str(e)}

    # ═════════════════════════════════════════════════════════
    # SUPPLEMENTARY: OPENINSIDER SCRAPER
    # ═════════════════════════════════════════════════════════

    def _scrape_openinsider(self, ticker: str) -> list:
        """
        Scrapes openinsider.com for richer insider transaction detail.
        Provides transaction type (P=Purchase, S=Sale), share count,
        price paid, and officer title.

        Staleness: same day (mirrors SEC Form 4 within hours)
        """
        try:
            url = (f'http://openinsider.com/screener?'
                   f's={ticker}&fd=30&td=0&cnt=20&action=1')
            headers = {
                'User-Agent': (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                )}
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                return []

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, 'html.parser')

            table = soup.find('table', {'class': 'tinytable'})
            if not table:
                return []

            transactions = []
            rows = table.find_all('tr')[1:]  # skip header

            for row in rows[:15]:
                cols = row.find_all('td')
                if len(cols) < 11:
                    continue
                try:
                    transactions.append({
                        'ticker':           ticker,
                        'filed_date':       cols[1].get_text(strip=True),
                        'trade_date':       cols[2].get_text(strip=True),
                        'filer_name':       cols[5].get_text(strip=True),
                        'title':            cols[6].get_text(strip=True),
                        'transaction_type': cols[7].get_text(strip=True),
                        # P = Purchase, S = Sale, A = Award
                        'shares':           self._parse_num(
                            cols[8].get_text(strip=True)),
                        'price':            self._parse_num(
                            cols[9].get_text(strip=True)
                            .replace('$', '')),
                        'value':            self._parse_num(
                            cols[10].get_text(strip=True)
                            .replace('$', '').replace('+', '')),
                        'source':           'openinsider',
                        'form_type':        '4',
                        'collected_at':     datetime.utcnow(),
                    })
                except (IndexError, ValueError):
                    continue

            return transactions

        except Exception as e:
            print(f'  [SEC] OpenInsider scrape failed: {e}')
            return []

    # ═════════════════════════════════════════════════════════
    # FTD HELPERS
    # ═════════════════════════════════════════════════════════

    def _fetch_ftd_file(self, ticker: str,
                         period: str) -> Optional[dict]:
        """
        Downloads and parses one SEC FTD zip file.
        period format: 'YYYYMM_a' or 'YYYYMM_b'
        """
        import zipfile

        year_month, half = period.split('_')
        filename  = f'cnsfails{year_month}{half}.zip'
        url       = EDGAR_FTD_BASE + filename

        try:
            self._rate_limit()
            resp = requests.get(
                url,
                timeout=30,
                headers=EDGAR_HEADERS
            )
            if resp.status_code != 200:
                return None

            # Unzip in memory
            zf      = zipfile.ZipFile(io.BytesIO(resp.content))
            txtfile = [n for n in zf.namelist()
                       if n.endswith('.txt')][0]
            content = zf.read(txtfile).decode('utf-8',
                                               errors='replace')

            # Parse pipe-delimited FTD file
            # Format: SETTLEMENT DATE|CUSIP|SYMBOL|QUANTITY|DESCRIPTION|PRICE
            total_ftd = 0
            for line in content.splitlines():
                parts = line.split('|')
                if len(parts) < 4:
                    continue
                if parts[2].strip().upper() == ticker.upper():
                    try:
                        total_ftd += int(parts[3].strip())
                    except ValueError:
                        pass

            if total_ftd == 0:
                return None

            return {
                'ticker':        ticker,
                'ftd_shares':    total_ftd,
                'ftd_period':    period,
                'ftd_as_pct_vol': 0.0,  # computed later with volume
                'tier':          self._ftd_tier(total_ftd),
                'error':         False,
            }

        except Exception as e:
            print(f'  [SEC] FTD file error ({period}): {e}')
            return None

    def _ftd_tier(self, ftd_shares: int) -> str:
        if ftd_shares < 10_000:
            return 'low'
        if ftd_shares < 100_000:
            return 'moderate'
        if ftd_shares < 1_000_000:
            return 'elevated'
        return 'extreme'

    @staticmethod
    def _recent_ftd_periods(n: int = 3) -> list:
        """
        Returns the last N FTD file period identifiers.
        Each month has two files: YYYYMM_a (1st-15th),
                                   YYYYMM_b (16th-end)
        """
        periods = []
        d = datetime.utcnow()
        for _ in range(n * 2):
            ym = d.strftime('%Y%m')
            periods.append(f'{ym}_b')
            periods.append(f'{ym}_a')
            d = d.replace(day=1) - timedelta(days=1)
        return periods[:n]

    # ═════════════════════════════════════════════════════════
    # DERIVED COMPUTATIONS
    # ═════════════════════════════════════════════════════════

    def _insider_sentiment(self, transactions: list) -> str:
        """
        Summarises net insider sentiment from recent transactions.
        Returns: 'net_buying' / 'net_selling' / 'mixed' / 'none'
        """
        if not transactions:
            return 'none'

        buys  = sum(1 for t in transactions
                    if t.get('transaction_type', '') == 'P')
        sells = sum(1 for t in transactions
                    if t.get('transaction_type', '') == 'S')

        if buys > sells * 1.5:
            return 'net_buying'
        if sells > buys * 1.5:
            return 'net_selling'
        if buys > 0 and sells > 0:
            return 'mixed'
        if buys > 0:
            return 'net_buying'
        if sells > 0:
            return 'net_selling'
        return 'none'

    # ═════════════════════════════════════════════════════════
    # RATE LIMITING
    # ═════════════════════════════════════════════════════════

    def _rate_limit(self):
        """
        Enforces EDGAR's 10 req/sec limit.
        Sleeps if last request was < 0.11 seconds ago.
        """
        elapsed = time.time() - self._last_request_time
        if elapsed < 0.11:
            time.sleep(0.11 - elapsed)
        self._last_request_time = time.time()

    # ═════════════════════════════════════════════════════════
    # DATABASE WRITES
    # ═════════════════════════════════════════════════════════

    def _save_events(self, ticker: str, data: dict):
        """Saves activist filings and insider txns as TYPE D events."""
        cur = self.conn.cursor()

        # Save activist filings
        for filing in data.get('activist_filings', []):
            cur.execute("""
                INSERT INTO stock_events
                    (ticker, event_type, event_data, occurred_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                ticker,
                'activist_filing',
                json.dumps(filing, default=str),
                datetime.utcnow(),
            ))

        # Save insider transactions
        for txn in data.get('insider_transactions', []):
            cur.execute("""
                INSERT INTO stock_events
                    (ticker, event_type, event_data, occurred_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                ticker,
                'insider_transaction',
                json.dumps(txn, default=str),
                datetime.utcnow(),
            ))

        # Save FTD if elevated
        if data.get('ftd_shares', 0) > 10_000:
            cur.execute("""
                INSERT INTO stock_events
                    (ticker, event_type, event_data, occurred_at)
                VALUES (%s, %s, %s, %s)
            """, (
                ticker,
                'elevated_ftd',
                json.dumps({
                    'ftd_shares':    data['ftd_shares'],
                    'tier':          data['ftd_tier'],
                }, default=str),
                datetime.utcnow(),
            ))

        self.conn.commit()

    def _log(self, ticker: str, source: str,
             status: str, rows: int,
             error: str, duration_ms: int):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO collection_log
                (ticker, source, status, rows_collected,
                 error_message, duration_ms)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (ticker, source, status, rows, error, duration_ms))
        self.conn.commit()

    def _ensure_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sec_ftd_data (
                id              SERIAL PRIMARY KEY,
                ticker          VARCHAR(10),
                ftd_shares      BIGINT,
                ftd_period      VARCHAR(10),
                ftd_as_pct_vol  FLOAT,
                tier            VARCHAR(20),
                collected_at    TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS insider_transactions (
                id              SERIAL PRIMARY KEY,
                ticker          VARCHAR(10),
                filer_name      VARCHAR(200),
                title           VARCHAR(100),
                transaction_type VARCHAR(5),
                shares          BIGINT,
                price           FLOAT,
                value           FLOAT,
                filed_date      VARCHAR(20),
                trade_date      VARCHAR(20),
                source          VARCHAR(50),
                collected_at    TIMESTAMP DEFAULT NOW()
            )
        """)
        self.conn.commit()

    @staticmethod
    def _parse_num(text: str) -> Optional[float]:
        try:
            return float(
                re.sub(r'[^\d.-]', '', text.replace(',', '')))
        except (ValueError, AttributeError):
            return None

    def close(self):
        self.conn.close()


# Need json for _save_events
import json

# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    c = SECEdgarCollector()

    print('Test 1: collect_all for GME')
    r = c.collect_all('GME')
    print(f'  activist_filings  = {r["activist_filing_count"]}')
    print(f'  insider sentiment = {r["insider_net_sentiment"]}')
    print(f'  ftd_shares        = {r["ftd_shares"]:,}')
    print(f'  ftd_tier          = {r["ftd_tier"]}')

    print('\nTest 2: 13D/13G filings (last 90 days)')
    filings = c.get_activist_filings('GME', days_back=90)
    print(f'  Found {len(filings)} activist filings')
    for f in filings[:3]:
        print(f'    {f["form_type"]} by {f["filer_name"]} '
              f'on {f["filed_date"]}')

    print('\nTest 3: Institutional ownership')
    inst = c.get_institutional_ownership('GME')
    print(f'  institution_count     = {inst.get("institution_count")}')
    print(f'  most_recent_13f_date  = {inst.get("most_recent_13f_date")}')

    c.close()