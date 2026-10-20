# pipeline/engineering/market_features.py
#
# Market Feature Engine
#
# Reads raw data from databases and computes all market-side
# features needed for the ML feature vector.
#
# Data sources (reads from):
#   TimescaleDB  stock_timeseries     — OHLCV history
#   PostgreSQL   stock_snapshots      — SI, borrow rate latest
#   PostgreSQL   yfinance_snapshots   — pre-computed RSI/MACD/ATR
#   Cassandra    borrow_rate_history  — for borrow rate delta
#   Cassandra    short_volume_daily   — FINRA short volume
#   PostgreSQL   finra_short_volume   — fallback for SQL queries
#
# Features produced (12 market features for ML vector):
#   short_interest_pct, days_to_cover, borrow_rate,
#   si_staleness_penalty, borrow_rate_delta,
#   volume_ratio_30d, float_rotation, volume_acceleration,
#   price_change_1d, price_change_5d,
#   rsi_14, distance_from_52w_high,
#   short_volume_ratio, atr_14

import os
import time
import numpy as np
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

try:
    from cassandra.cluster import Cluster
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False


class MarketFeatureEngine:
    """
    Computes all market-side features for a given ticker.
    Reads from multiple databases, computes derived features,
    and returns a clean dict ready for the feature assembler.

    Usage:
        engine = MarketFeatureEngine()
        features = engine.compute('GME')
        # features is a dict with all 14 market features
    """

    def __init__(self):
        self.pg = psycopg2.connect(os.getenv('POSTGRES_URL'))

        # Cassandra for borrow rate + short volume history
        self.cass = None
        if CASSANDRA_AVAILABLE:
            try:
                cluster = Cluster(
                    [os.getenv('CASSANDRA_HOST', 'localhost')])
                self.cass = cluster.connect('squeezradar')
            except Exception as e:
                print(f'[market_features] Cassandra unavailable: {e}'
                      f' — using PostgreSQL fallback')

    # ═════════════════════════════════════════════════════════
    # MAIN METHOD
    # ═════════════════════════════════════════════════════════

    def compute(self, ticker: str) -> dict:
        """
        Computes all market features for one ticker.
        Returns dict with all features. Missing values → 0.0.
        Never raises — returns partial dict on errors.

        Returns dict with:
          All 14 market features + quality metadata
        """
        ticker  = ticker.upper().strip()
        start   = time.time()
        result  = {
            'ticker':           ticker,
            'computed_at':      datetime.utcnow(),
            'feature_errors':   [],
        }

        # ── 1. Short interest features ───────────────────
        si = self._get_short_interest(ticker)
        result.update(si)

        # ── 2. Borrow rate features ──────────────────────
        br = self._get_borrow_rate_features(ticker)
        result.update(br)

        # ── 3. FINRA short volume features ───────────────
        sv = self._get_short_volume_features(ticker)
        result.update(sv)

        # ── 4. Price and volume features ─────────────────
        pv = self._get_price_volume_features(ticker)
        result.update(pv)

        # ── 5. Technical indicators ───────────────────────
        ti = self._get_technical_indicators(ticker)
        result.update(ti)

        # ── 6. Float features ─────────────────────────────
        fl = self._get_float_features(ticker)
        result.update(fl)

        # ── 7. SI staleness penalty ───────────────────────
        result['si_staleness_penalty'] = self._compute_staleness_penalty(
            result.get('si_stale_days', 14))

        result['compute_duration_ms'] = int(
            (time.time()-start)*1000)

        return result

    # ═════════════════════════════════════════════════════════
    # 1. SHORT INTEREST FEATURES
    # ═════════════════════════════════════════════════════════

    def _get_short_interest(self, ticker: str) -> dict:
        """
        Reads short interest and days-to-cover from stock_snapshots.
        Source: stockanalysis_collector (2-week staleness).
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT short_interest_pct,
                       days_to_cover,
                       si_stale_days,
                       collected_at
                FROM   stock_snapshots
                WHERE  ticker      = %s
                  AND  short_interest_pct IS NOT NULL
                ORDER  BY collected_at DESC
                LIMIT  1
            """, (ticker,))
            row = cur.fetchone()

            if not row:
                return {
                    'short_interest_pct': 0.0,
                    'days_to_cover':      0.0,
                    'si_stale_days':      14,
                    'si_data_available':  False,
                }

            return {
                'short_interest_pct': float(row[0]) if row[0] else 0.0,
                'days_to_cover':      float(row[1]) if row[1] else 0.0,
                'si_stale_days':      int(row[2]) if row[2] else 14,
                'si_collected_at':    row[3].isoformat() if row[3] else None,
                'si_data_available':  True,
            }

        except Exception as e:
            return {
                'short_interest_pct': 0.0,
                'days_to_cover':      0.0,
                'si_stale_days':      14,
                'si_data_available':  False,
                'si_error':           str(e),
            }

    # ═════════════════════════════════════════════════════════
    # 2. BORROW RATE FEATURES
    # ═════════════════════════════════════════════════════════

    def _get_borrow_rate_features(self, ticker: str) -> dict:
        """
        Gets borrow rate from Cassandra (primary) or PostgreSQL.
        Computes delta (change vs yesterday).
        """
        # Try Cassandra first
        if self.cass:
            return self._borrow_from_cassandra(ticker)
        return self._borrow_from_postgres(ticker)

    def _borrow_from_cassandra(self, ticker: str) -> dict:
        """Reads borrow rate history from Cassandra."""
        try:
            from datetime import date
            today     = date.today()
            yesterday = today - timedelta(days=1)

            # Get today's readings
            rows_today = list(self.cass.execute("""
                SELECT borrow_rate, available_shares, ts
                FROM borrow_rate_history
                WHERE ticker = %s AND date = %s
                LIMIT 3
            """, (ticker, today)))

            # Get yesterday's readings
            rows_yesterday = list(self.cass.execute("""
                SELECT borrow_rate, ts
                FROM borrow_rate_history
                WHERE ticker = %s AND date = %s
                LIMIT 1
            """, (ticker, yesterday)))

            if not rows_today:
                return self._borrow_from_postgres(ticker)

            current_rate = float(rows_today[0].borrow_rate)

            # Delta vs yesterday
            if rows_yesterday:
                prev_rate = float(rows_yesterday[0].borrow_rate)
                delta     = round(current_rate - prev_rate, 4)
            else:
                delta = 0.0

            return {
                'borrow_rate':            current_rate,
                'borrow_rate_delta':      delta,
                'available_shares':       int(rows_today[0].available_shares)
                                          if rows_today[0].available_shares
                                          else 0,
                'is_hard_to_borrow':      current_rate >= 75,
                'borrow_rate_tier':       self._borrow_tier(current_rate),
                'borrow_data_available':  True,
                'borrow_data_source':     'cassandra',
            }

        except Exception as e:
            return self._borrow_from_postgres(ticker)

    def _borrow_from_postgres(self, ticker: str) -> dict:
        """Fallback: reads from PostgreSQL stock_snapshots."""
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT borrow_rate, collected_at
                FROM   stock_snapshots
                WHERE  ticker     = %s
                  AND  borrow_rate IS NOT NULL
                ORDER  BY collected_at DESC
                LIMIT  2
            """, (ticker,))
            rows = cur.fetchall()

            if not rows:
                return {
                    'borrow_rate':           0.0,
                    'borrow_rate_delta':     0.0,
                    'is_hard_to_borrow':     False,
                    'borrow_data_available': False,
                }

            current_rate = float(rows[0][0])
            delta = 0.0
            if len(rows) > 1:
                delta = round(current_rate - float(rows[1][0]), 4)

            return {
                'borrow_rate':           current_rate,
                'borrow_rate_delta':     delta,
                'is_hard_to_borrow':     current_rate >= 75,
                'borrow_rate_tier':      self._borrow_tier(current_rate),
                'borrow_data_available': True,
                'borrow_data_source':    'postgres',
            }

        except Exception as e:
            return {
                'borrow_rate':           0.0,
                'borrow_rate_delta':     0.0,
                'is_hard_to_borrow':     False,
                'borrow_data_available': False,
                'borrow_error':          str(e),
            }

    # ═════════════════════════════════════════════════════════
    # 3. FINRA SHORT VOLUME FEATURES
    # ═════════════════════════════════════════════════════════

    def _get_short_volume_features(self, ticker: str) -> dict:
        """
        Gets FINRA short volume ratio from PostgreSQL.
        (Cassandra stores it but SQL queries easier here for 5d avg)
        """
        cur = self.pg.cursor()
        try:
            # Latest day
            cur.execute("""
                SELECT short_volume_ratio, short_ratio_tier,
                       short_volume, total_volume, trade_date
                FROM   finra_short_volume
                WHERE  ticker     = %s
                ORDER  BY trade_date DESC
                LIMIT  1
            """, (ticker,))
            latest = cur.fetchone()

            if not latest:
                return {
                    'short_volume_ratio':    0.0,
                    'short_volume_ratio_5d': 0.0,
                    'short_volume_trend':    'unknown',
                    'finra_data_available':  False,
                }

            # 5-day average
            cur.execute("""
                SELECT AVG(short_volume_ratio)
                FROM   finra_short_volume
                WHERE  ticker     = %s
                  AND  trade_date > CURRENT_DATE - INTERVAL '5 days'
            """, (ticker,))
            avg_row    = cur.fetchone()
            avg_5d     = float(avg_row[0]) if avg_row[0] else 0.0

            # Trend: compare recent 2 days vs previous 3 days
            cur.execute("""
                SELECT short_volume_ratio
                FROM   finra_short_volume
                WHERE  ticker     = %s
                ORDER  BY trade_date DESC
                LIMIT  5
            """, (ticker,))
            hist       = [r[0] for r in cur.fetchall()
                          if r[0] is not None]
            trend      = self._compute_ratio_trend(hist)

            return {
                'short_volume_ratio':    float(latest[0]),
                'short_ratio_tier':      latest[1],
                'short_volume':          int(latest[2]) if latest[2] else 0,
                'total_volume_finra':    int(latest[3]) if latest[3] else 0,
                'short_volume_ratio_5d': round(avg_5d, 6),
                'short_volume_trend':    trend,
                'finra_trade_date':      latest[4].isoformat()
                                         if latest[4] else None,
                'finra_data_available':  True,
            }

        except Exception as e:
            return {
                'short_volume_ratio':    0.0,
                'short_volume_ratio_5d': 0.0,
                'short_volume_trend':    'unknown',
                'finra_data_available':  False,
                'finra_error':           str(e),
            }

    # ═════════════════════════════════════════════════════════
    # 4. PRICE AND VOLUME FEATURES
    # ═════════════════════════════════════════════════════════

    def _get_price_volume_features(self, ticker: str) -> dict:
        """
        Reads OHLCV from TimescaleDB and computes price/volume features.
        """
        cur = self.pg.cursor()
        try:
            # Get last 60 days of daily OHLCV
            cur.execute("""
                SELECT time, open, high, low, close, volume
                FROM   stock_timeseries
                WHERE  ticker   = %s
                  AND  interval = '1d'
                  AND  time     > NOW() - INTERVAL '60 days'
                ORDER  BY time ASC
            """, (ticker,))
            rows = cur.fetchall()

            if len(rows) < 2:
                return {
                    'price_current':        0.0,
                    'price_change_1d':      0.0,
                    'price_change_5d':      0.0,
                    'price_change_20d':     0.0,
                    'volume_today':         0,
                    'volume_ratio_30d':     0.0,
                    'avg_daily_volume_30d': 0,
                    'ohlcv_data_available': False,
                }

            # Build DataFrame
            df = pd.DataFrame(rows, columns=[
                'time','open','high','low','close','volume'])
            df = df.dropna(subset=['close','volume'])

            close  = df['close'].astype(float)
            volume = df['volume'].astype(float)
            high   = df['high'].astype(float)

            result = {'ohlcv_data_available': True}

            # Price metrics
            result['price_current']    = round(float(close.iloc[-1]), 4)

            if len(close) >= 2:
                result['price_change_1d'] = round(
                    (close.iloc[-1] - close.iloc[-2])
                    / close.iloc[-2], 6)

            if len(close) >= 6:
                result['price_change_5d'] = round(
                    (close.iloc[-1] - close.iloc[-6])
                    / close.iloc[-6], 6)

            if len(close) >= 21:
                result['price_change_20d'] = round(
                    (close.iloc[-1] - close.iloc[-21])
                    / close.iloc[-21], 6)

            # 52w high approximation from available data
            if len(high) > 0:
                period_high = float(high.max())
                result['distance_from_52w_high'] = round(
                    (period_high - float(close.iloc[-1]))
                    / period_high, 6) if period_high > 0 else 0.0

            # Volume metrics
            result['volume_today'] = int(volume.iloc[-1])

            if len(volume) >= 30:
                avg_30d = float(volume.rolling(30).mean().iloc[-1])
                result['avg_daily_volume_30d']  = int(avg_30d)
                result['avg_hourly_volume_30d']  = int(avg_30d / 6.5)
                result['volume_ratio_30d']       = round(
                    float(volume.iloc[-1]) / avg_30d, 4) \
                    if avg_30d > 0 else 0.0

            if len(volume) >= 5:
                avg_5d = float(volume.rolling(5).mean().iloc[-1])
                result['volume_ratio_5d'] = round(
                    float(volume.iloc[-1]) / avg_5d, 4) \
                    if avg_5d > 0 else 0.0

            # Volatility
            if len(close) >= 11:
                result['volatility_10d'] = round(
                    float(close.pct_change()
                              .rolling(10).std().iloc[-1]), 6)

            return result

        except Exception as e:
            return {
                'price_current':        0.0,
                'price_change_1d':      0.0,
                'price_change_5d':      0.0,
                'volume_today':         0,
                'volume_ratio_30d':     0.0,
                'ohlcv_data_available': False,
                'ohlcv_error':          str(e),
            }

    # ═════════════════════════════════════════════════════════
    # 5. TECHNICAL INDICATORS
    # ═════════════════════════════════════════════════════════

    def _get_technical_indicators(self, ticker: str) -> dict:
        """
        Reads pre-computed RSI/MACD/ATR from yfinance_snapshots.
        If not available, computes from TimescaleDB OHLCV.
        """
        cur = self.pg.cursor()
        try:
            # Try pre-computed first (faster)
            cur.execute("""
                SELECT rsi_14, macd, macd_signal,
                       macd_hist, atr_14, volatility_10d,
                       collected_at
                FROM   yfinance_snapshots
                WHERE  ticker = %s
                ORDER  BY collected_at DESC
                LIMIT  1
            """, (ticker,))
            row = cur.fetchone()

            if row and row[0] is not None:
                return {
                    'rsi_14':       round(float(row[0]), 2),
                    'macd':         round(float(row[1]), 6) if row[1] else 0.0,
                    'macd_signal':  round(float(row[2]), 6) if row[2] else 0.0,
                    'macd_hist':    round(float(row[3]), 6) if row[3] else 0.0,
                    'atr_14':       round(float(row[4]), 4) if row[4] else 0.0,
                    'volatility_10d': round(float(row[5]), 6)
                                      if row[5] else 0.0,
                    'indicators_source': 'yfinance_snapshots',
                }

            # Fallback: compute from TimescaleDB OHLCV
            return self._compute_indicators_from_ohlcv(ticker)

        except Exception as e:
            return {
                'rsi_14':          50.0,  # Neutral default
                'macd':            0.0,
                'atr_14':          0.0,
                'indicators_error': str(e),
            }

    def _compute_indicators_from_ohlcv(self,
                                        ticker: str) -> dict:
        """Computes RSI/MACD/ATR directly from TimescaleDB."""
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT close, high, low
                FROM   stock_timeseries
                WHERE  ticker   = %s
                  AND  interval = '1d'
                ORDER  BY time DESC
                LIMIT  60
            """, (ticker,))
            rows = cur.fetchall()

            if len(rows) < 15:
                return {'rsi_14': 50.0, 'macd': 0.0, 'atr_14': 0.0}

            close = pd.Series([float(r[0]) for r in rows]).iloc[::-1]
            high  = pd.Series([float(r[1]) for r in rows]).iloc[::-1]
            low   = pd.Series([float(r[2]) for r in rows]).iloc[::-1]

            result = {'indicators_source': 'computed_from_ohlcv'}

            # RSI-14
            if len(close) >= 15:
                result['rsi_14'] = round(
                    self._rsi(close, 14), 2)

            # MACD
            if len(close) >= 35:
                m, s, h = self._macd(close)
                result['macd']       = round(m, 6)
                result['macd_signal'] = round(s, 6)
                result['macd_hist']  = round(h, 6)

            # ATR-14
            if len(close) >= 15:
                result['atr_14'] = round(
                    self._atr(high, low, close, 14), 4)

            return result

        except Exception as e:
            return {
                'rsi_14':    50.0,
                'macd':      0.0,
                'atr_14':    0.0,
                'error':     str(e),
            }

    # ═════════════════════════════════════════════════════════
    # 6. FLOAT FEATURES
    # ═════════════════════════════════════════════════════════

    def _get_float_features(self, ticker: str) -> dict:
        """
        Gets float and computes float rotation.
        Float rotation = today's volume / float shares.
        High float rotation = heavy turnover = unusual activity.
        """
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT float_shares, shares_outstanding
                FROM   stock_snapshots
                WHERE  ticker      = %s
                  AND  float_shares IS NOT NULL
                ORDER  BY collected_at DESC
                LIMIT  1
            """, (ticker,))
            row = cur.fetchone()

            if not row:
                # Try stocks table
                cur.execute("""
                    SELECT float_shares FROM stocks
                    WHERE ticker = %s
                """, (ticker,))
                row2 = cur.fetchone()
                if row2 and row2[0]:
                    float_shares = int(row2[0])
                else:
                    return {
                        'float_shares':    0,
                        'float_rotation':  0.0,
                        'float_available': False,
                    }
            else:
                float_shares = int(row[0])

            # Compute float rotation using today's volume
            cur.execute("""
                SELECT volume FROM stock_timeseries
                WHERE  ticker   = %s
                  AND  interval = '1d'
                ORDER  BY time DESC
                LIMIT  1
            """, (ticker,))
            vol_row = cur.fetchone()
            volume_today = int(vol_row[0]) if vol_row else 0

            float_rotation = round(
                volume_today / float_shares, 6) \
                if float_shares > 0 else 0.0

            return {
                'float_shares':      float_shares,
                'float_rotation':    float_rotation,
                'float_available':   True,
                'float_category':    self._float_category(float_shares),
            }

        except Exception as e:
            return {
                'float_shares':    0,
                'float_rotation':  0.0,
                'float_available': False,
                'float_error':     str(e),
            }

    # ═════════════════════════════════════════════════════════
    # TECHNICAL INDICATOR COMPUTATIONS
    # ═════════════════════════════════════════════════════════

    @staticmethod
    def _rsi(close: pd.Series, period: int = 14) -> float:
        delta   = close.diff()
        gain    = delta.clip(lower=0)
        loss    = -delta.clip(upper=0)
        avg_g   = gain.ewm(com=period-1, min_periods=period).mean()
        avg_l   = loss.ewm(com=period-1, min_periods=period).mean()
        rs      = avg_g / avg_l.replace(0, np.nan)
        rsi     = 100 - (100 / (1 + rs))
        val     = float(rsi.iloc[-1])
        return 50.0 if np.isnan(val) else val

    @staticmethod
    def _macd(close: pd.Series,
               fast=12, slow=26, signal=9):
        ema_fast   = close.ewm(span=fast, adjust=False).mean()
        ema_slow   = close.ewm(span=slow, adjust=False).mean()
        macd_line  = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        hist        = macd_line - signal_line
        return (float(macd_line.iloc[-1]),
                float(signal_line.iloc[-1]),
                float(hist.iloc[-1]))

    @staticmethod
    def _atr(high: pd.Series, low: pd.Series,
              close: pd.Series, period: int = 14) -> float:
        prev_c = close.shift(1)
        tr     = pd.concat([
            high - low,
            (high - prev_c).abs(),
            (low  - prev_c).abs(),
        ], axis=1).max(axis=1)
        atr    = tr.ewm(span=period, adjust=False).mean()
        return float(atr.iloc[-1])

    # ═════════════════════════════════════════════════════════
    # HELPER METHODS
    # ═════════════════════════════════════════════════════════

    @staticmethod
    def _compute_staleness_penalty(stale_days: int) -> float:
        """
        Returns staleness penalty for SI data.
        0.0 = fresh data, 1.0 = maximally stale (14+ days).
        Used to discount SI-dependent features in ML model.
        """
        return min(stale_days / 14.0, 1.0)

    @staticmethod
    def _compute_ratio_trend(ratios: list) -> str:
        """
        Classifies short volume ratio trend from recent history.
        Returns: 'rising', 'falling', 'flat', 'insufficient_data'
        """
        if len(ratios) < 3:
            return 'insufficient_data'
        recent = sum(ratios[:2]) / 2
        older  = sum(ratios[2:]) / len(ratios[2:])
        delta  = recent - older
        if delta > 0.03:
            return 'rising'
        if delta < -0.03:
            return 'falling'
        return 'flat'

    @staticmethod
    def _borrow_tier(rate: float) -> str:
        if rate < 10:
            return 'cheap'
        if rate < 30:
            return 'normal'
        if rate < 75:
            return 'expensive'
        if rate < 200:
            return 'hard_to_borrow'
        return 'extreme'

    @staticmethod
    def _float_category(float_shares: int) -> str:
        if float_shares < 10_000_000:
            return 'nano'       # < 10M
        if float_shares < 50_000_000:
            return 'micro'      # 10-50M
        if float_shares < 100_000_000:
            return 'small'      # 50-100M
        if float_shares < 200_000_000:
            return 'medium'     # 100-200M
        return 'large'          # > 200M

    def close(self):
        self.pg.close()


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    engine = MarketFeatureEngine()

    print('='*55)
    print('  Market Feature Engine — Manual Test')
    print('='*55)

    for ticker in ['GME', 'AAPL', 'XYZFAKE']:
        print(f'\n── {ticker} ──')
        features = engine.compute(ticker)

        key_features = [
            'short_interest_pct', 'days_to_cover',
            'borrow_rate', 'borrow_rate_delta',
            'si_staleness_penalty',
            'short_volume_ratio', 'short_volume_trend',
            'price_current', 'price_change_1d',
            'price_change_5d', 'volume_ratio_30d',
            'float_rotation', 'rsi_14', 'atr_14',
            'distance_from_52w_high',
        ]
        for k in key_features:
            v = features.get(k)
            if v is not None:
                print(f'  {k:30s} = {v}')

        print(f'  compute_duration_ms           = '
              f'{features.get("compute_duration_ms")}')

    engine.close()