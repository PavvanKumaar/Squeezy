# pipeline/collectors/pytrends_collector.py

from pytrends.request import TrendReq
import psycopg2
import os
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class PyTrendsCollector:

    def __init__(self):
        self.pg = psycopg2.connect(os.getenv('POSTGRES_URL'))
        # hl = host language, tz = timezone offset
        self.pytrends = TrendReq(hl='en-US', tz=360,
                                  timeout=(10, 25),
                                  retries=2,
                                  backoff_factor=0.5)

    def collect(self, ticker: str) -> dict:
        """
        Gets Google search interest for a ticker
        Returns score 0-100 where 100 = peak interest
        Score is RELATIVE not absolute
        """
        start = time.time()

        try:
            # Build payload — search for "$GME" and "GME stock"
            kw_list = [f"${ticker}", f"{ticker} stock"]
            self.pytrends.build_payload(
                kw_list,
                timeframe='now 7-d',  # last 7 days
                geo='US'
            )

            df = self.pytrends.interest_over_time()

            if df.empty:
                return {
                    'ticker': ticker,
                    'trend_score': 0,
                    'trend_spike': False,
                    'error': False
                }

            # Average the two keyword columns
            avg_score = float(df[kw_list].mean(axis=1).mean())
            peak_score = float(df[kw_list].max(axis=1).max())
            recent_score = float(
                df[kw_list].mean(axis=1).iloc[-1])

            # Spike = recent score is 3x the weekly average
            trend_spike = recent_score > (avg_score * 3)

            result = {
                'ticker': ticker,
                'trend_score': round(recent_score, 2),
                'trend_avg_7d': round(avg_score, 2),
                'trend_peak_7d': round(peak_score, 2),
                'trend_spike': trend_spike,
                'collected_at': datetime.utcnow(),
                'error': False
            }

            self._save(ticker, result)

            duration = int((time.time() - start) * 1000)
            self._log(ticker, 'success', 1, None, duration)

            print(f"  ✓ {ticker} Google Trends — "
                  f"score: {recent_score:.0f}/100 | "
                  f"spike: {trend_spike}")

            # IMPORTANT: sleep to avoid rate limiting
            # pytrends gets blocked if you call too fast
            time.sleep(2)

            return result

        except Exception as e:
            self._log(ticker, 'failed', 0, str(e), 0)
            print(f"  ✗ {ticker} Trends failed: {e}")
            # Sleep on error too (might be rate limited)
            time.sleep(10)
            return {'ticker': ticker,
                    'trend_score': 0,
                    'error': True,
                    'error_message': str(e)}

    def _save(self, ticker: str, result: dict):
        cur = self.pg.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trends_snapshots (
                id              SERIAL PRIMARY KEY,
                ticker          VARCHAR(10),
                trend_score     FLOAT,
                trend_avg_7d    FLOAT,
                trend_peak_7d   FLOAT,
                trend_spike     BOOLEAN,
                collected_at    TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            INSERT INTO trends_snapshots
            (ticker, trend_score, trend_avg_7d,
             trend_peak_7d, trend_spike)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            ticker,
            result['trend_score'],
            result['trend_avg_7d'],
            result['trend_peak_7d'],
            result['trend_spike']
        ))
        self.pg.commit()

    def _log(self, ticker, status, rows, error, duration_ms):
        cur = self.pg.cursor()
        cur.execute("""
            INSERT INTO collection_log
            (ticker, source, status, rows_collected,
             error_message, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (ticker, 'pytrends', status,
              rows, error, duration_ms))
        self.pg.commit()


