# pipeline/collectors/options_collector.py

import yfinance as yf
import pandas as pd
import numpy as np
import psycopg2
import os
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class OptionsCollector:

    def __init__(self):
        self.conn = psycopg2.connect(
            os.getenv('POSTGRES_URL'))

    # ─────────────────────────────────────────
    # MAIN METHOD — call this for any ticker
    # ─────────────────────────────────────────
    def collect(self, ticker: str) -> dict:
        start = time.time()
        result = {
            'ticker': ticker,
            'collected_at': datetime.utcnow(),
            'error': False
        }

        try:
            stock = yf.Ticker(ticker)

            # Get all expiry dates available
            expiries = stock.options
            if not expiries:
                result['error'] = True
                result['error_message'] = 'No options available'
                result['options_eligible'] = False
                self._log(ticker, 'failed',
                          0, 'No options available',
                          int((time.time()-start)*1000))
                return result

            # Collect chains for next 4 expiries only
            # (beyond that, data is thin and unreliable)
            all_calls = []
            all_puts  = []

            for expiry in expiries[:4]:
                try:
                    chain = stock.option_chain(expiry)
                    calls = chain.calls.copy()
                    puts  = chain.puts.copy()
                    calls['expiry'] = expiry
                    puts['expiry']  = expiry
                    all_calls.append(calls)
                    all_puts.append(puts)
                except Exception:
                    continue

            if not all_calls:
                result['error'] = True
                result['error_message'] = 'Could not fetch chains'
                return result

            calls_df = pd.concat(all_calls, ignore_index=True)
            puts_df  = pd.concat(all_puts,  ignore_index=True)

            # ── Compute metrics ──────────────────────
            metrics = {}
            metrics['call_put_ratio'] = self._call_put_ratio(
                calls_df, puts_df)
            metrics['max_pain']       = self._max_pain(
                calls_df, puts_df)
            metrics['iv_rank']        = self._iv_rank(
                calls_df, puts_df)
            metrics['total_call_oi']  = int(
                calls_df['openInterest'].sum())
            metrics['total_put_oi']   = int(
                puts_df['openInterest'].sum())
            metrics['total_call_vol'] = int(
                calls_df['volume'].fillna(0).sum())
            metrics['total_put_vol']  = int(
                puts_df['volume'].fillna(0).sum())
            metrics['nearest_expiry'] = expiries[0]
            metrics['expiry_count']   = len(expiries)
            metrics['options_eligible'] = True

            result.update(metrics)

            # ── Save to PostgreSQL ───────────────────
            self._save(ticker, metrics)

            duration = int((time.time() - start) * 1000)
            self._log(ticker, 'success', 1, None, duration)

            print(f"  ✓ {ticker} options — "
                  f"C/P ratio: {metrics['call_put_ratio']:.2f} | "
                  f"IV rank: {metrics['iv_rank']:.0f}% | "
                  f"Max pain: ${metrics['max_pain']:.2f}")

        except Exception as e:
            result['error'] = True
            result['error_message'] = str(e)
            self._log(ticker, 'failed', 0, str(e),
                      int((time.time()-start)*1000))
            print(f"  ✗ {ticker} options failed: {e}")

        return result

    # ─────────────────────────────────────────
    # METRIC COMPUTATIONS
    # ─────────────────────────────────────────
    def _call_put_ratio(self, calls: pd.DataFrame,
                         puts: pd.DataFrame) -> float:
        """
        Call/Put ratio by open interest
        > 1.0 = more calls than puts (bullish sentiment)
        > 2.5 = strongly bullish / speculative
        """
        call_oi = calls['openInterest'].fillna(0).sum()
        put_oi  = puts['openInterest'].fillna(0).sum()
        if put_oi == 0:
            return 0.0
        return round(float(call_oi / put_oi), 4)

    def _max_pain(self, calls: pd.DataFrame,
                   puts: pd.DataFrame) -> float:
        """
        Max pain = strike price where option buyers
        lose the most money at expiry.
        Market makers are incentivized to pin price here.
        """
        # Get all unique strike prices
        all_strikes = pd.concat([
            calls[['strike', 'openInterest']],
            puts[['strike', 'openInterest']]
        ])['strike'].unique()

        if len(all_strikes) == 0:
            return 0.0

        min_pain  = float('inf')
        max_pain_price = 0.0

        for strike in sorted(all_strikes):
            # Pain from calls: all calls with strike < price
            call_pain = calls[
                calls['strike'] < strike
            ]['openInterest'].fillna(0).sum() * (
                strike - calls[calls['strike'] < strike]['strike']
            ).fillna(0).sum()

            # Pain from puts: all puts with strike > price
            put_pain = puts[
                puts['strike'] > strike
            ]['openInterest'].fillna(0).sum() * (
                puts[puts['strike'] > strike]['strike'] - strike
            ).fillna(0).sum()

            total_pain = call_pain + put_pain
            if total_pain < min_pain:
                min_pain = total_pain
                max_pain_price = strike

        return float(max_pain_price)

    def _iv_rank(self, calls: pd.DataFrame,
                  puts: pd.DataFrame) -> float:
        """
        IV Rank approximation from current chain
        Uses average IV across all strikes
        Returns 0-100 (higher = more expensive options)
        True IV rank needs 52-week history which
        yfinance doesn't provide — this is a proxy
        """
        all_iv = pd.concat([
            calls['impliedVolatility'],
            puts['impliedVolatility']
        ]).dropna()

        if all_iv.empty:
            return 0.0

        iv_mean = all_iv.mean()
        iv_min  = all_iv.min()
        iv_max  = all_iv.max()

        if iv_max == iv_min:
            return 50.0

        # Normalize to 0-100
        rank = (iv_mean - iv_min) / (iv_max - iv_min) * 100
        return round(float(rank), 2)

    # ─────────────────────────────────────────
    # DATABASE WRITE
    # ─────────────────────────────────────────
    def _save(self, ticker: str, metrics: dict):
        """
        Saves options summary to PostgreSQL
        Creates options_snapshots table if needed
        """
        cur = self.conn.cursor()

        # Create table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS options_snapshots (
                id              SERIAL PRIMARY KEY,
                ticker          VARCHAR(10),
                call_put_ratio  FLOAT,
                max_pain        FLOAT,
                iv_rank         FLOAT,
                total_call_oi   BIGINT,
                total_put_oi    BIGINT,
                total_call_vol  BIGINT,
                total_put_vol   BIGINT,
                nearest_expiry  VARCHAR(20),
                expiry_count    INTEGER,
                collected_at    TIMESTAMP DEFAULT NOW(),
                iv_current      FLOAT,
                iv_pct          FLOAT,
                options_eligible BOOL
            )
        """)

        cur.execute("""
            INSERT INTO options_snapshots (
                ticker, call_put_ratio, max_pain,
                iv_rank, total_call_oi, total_put_oi,
                total_call_vol, total_put_vol,
                nearest_expiry, expiry_count,iv_current,iv_pct,options_eligible
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s
            )
        """, (
            ticker,
            metrics.get('call_put_ratio'),
            metrics.get('max_pain'),
            metrics.get('iv_rank'),
            metrics.get('total_call_oi'),
            metrics.get('total_put_oi'),
            metrics.get('total_call_vol'),
            metrics.get('total_put_vol'),
            metrics.get('nearest_expiry'),
            metrics.get('expiry_count'),
            metrics.get('iv_current'),
            metrics.get('iv_pct'),
            metrics.get('options_eligible')
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
        """, (ticker, 'options_yfinance',
              status, rows, error, duration_ms))
        self.conn.commit()

    def close(self):
        self.conn.close()

    


