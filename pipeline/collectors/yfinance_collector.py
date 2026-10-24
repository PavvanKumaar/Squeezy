# pipeline/collectors/yfinance_collector.py
# Features collected:
#   price (OHLCV), avg_daily_volume_30d, avg_hourly_volume_30d (derived),
#   float_shares, float_rotation (derived), volume_acceleration (derived),
#   RSI / MACD / ATR (derived), call_put_ratio, IV, max_pain,
#   earnings_date, news_catalyst_flag

import yfinance as yf
import pandas as pd
import numpy as np
import psycopg2
import requests
import os
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
# HELPER — always use this to create a yfinance Ticker object
# Adds proper headers to avoid empty-response errors
# ─────────────────────────────────────────────────────────────
def make_ticker(ticker: str) -> yf.Ticker:
    # session = requests.Session()
    # session.headers.update({
    #     'User-Agent': (
    #         'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    #         'AppleWebKit/537.36 (KHTML, like Gecko) '
    #         'Chrome/120.0.0.0 Safari/537.36'
    #     ),
    #     'Accept': 'application/json,text/html',
    # })
    return yf.Ticker(ticker)
    #session=session


class YFinanceCollector:
    """
    Collects ALL yfinance-sourced features:
      - OHLCV time-series (daily + 1min intraday)
      - Stock info (float, market cap, 52w high/low, avg volume)
      - Derived: avg_hourly_volume_30d, float_rotation,
                 volume_acceleration, RSI, MACD, ATR
      - Options chain: call_put_ratio, IV, max_pain
      - Earnings date
      - Recent news headlines
    """

    def __init__(self):
        self.conn = psycopg2.connect(os.getenv('POSTGRES_URL'))
        self._ensure_tables()

    # ═════════════════════════════════════════════════════════
    # PUBLIC METHODS
    # ═════════════════════════════════════════════════════════

    def collect_all(self, ticker: str) -> dict:
        """
        Master method — runs every collection task for one ticker.
        Returns a unified dict of all collected and derived features.
        """
        print(f"\n[yfinance] Collecting all data for {ticker}...")
        result = {'ticker': ticker, 'errors': []}

        # 1. Stock info (float, volume baseline, sector etc)
        info = self.get_stock_info(ticker)
        result.update(info)

        # 2. Daily OHLCV — save to TimescaleDB, return DataFrame
        daily_df = self.get_and_save_ohlcv(ticker,
                                            period='60d',
                                            interval='1d')

        # 3. Intraday 1min — last 7 days only (yfinance limit)
        intraday_df = self.get_and_save_ohlcv(ticker,
                                               period='5d',
                                               interval='1m')

        # 4. Derived features from OHLCV
        if daily_df is not None and len(daily_df) > 0:
            derived = self.compute_derived_features(
                daily_df, intraday_df,
                float_shares=info.get('float_shares'))
            result.update(derived)

        # 5. Options chain
        options = self.get_options_summary(ticker)
        result.update(options)

        # 6. Earnings date
        result['earnings_date'] = self.get_earnings_date(ticker)

        # 7. Recent news
        result['recent_news'] = self.get_recent_news(ticker)
        result['news_count']   = len(result['recent_news'])

        # 8. Save snapshot to DB
        self._save_snapshot(ticker, result)

        print(f"  ✓ {ticker} — float: {info.get('float_shares_fmt')} | "
              f"vol_ratio: {result.get('volume_ratio_30d', 'N/A')} | "
              f"RSI: {result.get('rsi_14', 'N/A')}")

        return result

    # ─────────────────────────────────────────────────────────
    # 1. STOCK INFO
    # ─────────────────────────────────────────────────────────
    def get_stock_info(self, ticker: str) -> dict:
        """
        Fetches static/slow-changing stock metadata.
        Staleness: daily (yfinance caches this)

        Returns:
          float_shares, shares_outstanding, avg_daily_volume_30d,
          market_cap, sector, industry, company_name,
          week52_high, week52_low, beta
        """
        start = time.time()
        try:
            t = make_ticker(ticker)
            info = t.info

            float_shares = info.get('floatShares')

            result = {
                'ticker':               ticker,
                'company_name':         info.get('longName', ''),
                'sector':               info.get('sector', ''),
                'industry':             info.get('industry', ''),
                'float_shares':         float_shares,
                'float_shares_fmt':     self._fmt_shares(float_shares),
                'shares_outstanding':   info.get('sharesOutstanding'),
                'avg_daily_volume_30d': info.get('averageVolume'),
                'avg_daily_volume_10d': info.get('averageVolume10days'),
                'market_cap':           info.get('marketCap'),
                'week52_high':          info.get('fiftyTwoWeekHigh'),
                'week52_low':           info.get('fiftyTwoWeekLow'),
                'beta':                 info.get('beta'),
                'pe_ratio':             info.get('trailingPE'),
                'short_ratio':          info.get('shortRatio'),
                # shortRatio from yfinance = days to cover (approx)
                'short_pct_of_float':   info.get('shortPercentOfFloat'),
                'info_collected_at':    datetime.utcnow(),
            }

            self._log(ticker, 'yfinance_info', 'success', 1,
                      None, int((time.time()-start)*1000))
            return result

        except Exception as e:
            self._log(ticker, 'yfinance_info', 'failed', 0,
                      str(e), int((time.time()-start)*1000))
            print(f"  ✗ {ticker} info failed: {e}")
            return {
                'ticker': ticker,
                'error_info': str(e)
            }

    # ─────────────────────────────────────────────────────────
    # 2. OHLCV TIME-SERIES
    # ─────────────────────────────────────────────────────────
    def get_and_save_ohlcv(self, ticker: str,
                            period: str = '60d',
                            interval: str = '1d') -> pd.DataFrame:
        """
        Fetches OHLCV and saves to TimescaleDB stock_timeseries.
        Staleness: 15 min for intraday, EOD for daily

        Supported combinations:
          period='60d',  interval='1d'   → 60 trading days daily bars
          period='5d',   interval='1m'   → 5 days 1-min bars (max free)
          period='1d',   interval='5m'   → today 5-min bars

        Returns: DataFrame (empty DataFrame on failure, never None)
        """
        start = time.time()
        try:
            t = make_ticker(ticker)
            df = t.history(period=period, interval=interval)

            if df.empty:
                return pd.DataFrame()

            # Normalize timezone to UTC
            if df.index.tz is not None:
                df.index = df.index.tz_convert('UTC')
            else:
                df.index = df.index.tz_localize('UTC')

            df['ticker']   = ticker
            df['interval'] = interval

            # Write to TimescaleDB
            rows_written = self._save_ohlcv(ticker, df, interval)

            self._log(ticker, f'yfinance_ohlcv_{interval}',
                      'success', rows_written, None,
                      int((time.time()-start)*1000))

            return df

        except Exception as e:
            self._log(ticker, f'yfinance_ohlcv_{interval}',
                      'failed', 0, str(e),
                      int((time.time()-start)*1000))
            print(f"  ✗ {ticker} OHLCV({interval}) failed: {e}")
            return pd.DataFrame()

    # ─────────────────────────────────────────────────────────
    # 3. DERIVED FEATURES FROM OHLCV
    # ─────────────────────────────────────────────────────────
    def compute_derived_features(self,
                                  daily_df: pd.DataFrame,
                                  intraday_df: pd.DataFrame,
                                  float_shares: int = None) -> dict:
        """
        Computes all features that require calculation from raw OHLCV.

        Returns:
          volume_ratio_30d, float_rotation, avg_hourly_volume_30d,
          volume_acceleration, price_change_1d, price_change_5d,
          price_change_20d, distance_from_52w_high,
          rsi_14, macd, macd_signal, macd_hist, atr_14,
          current_price, volatility_10d
        """
        result = {}

        close  = daily_df['Close']
        volume = daily_df['Volume']
        high   = daily_df['High']
        low    = daily_df['Low']

        # ── Price metrics ───────────────────────────────────
        if len(close) >= 2:
            result['current_price'] = round(float(close.iloc[-1]), 4)
            result['price_change_1d'] = round(
                float((close.iloc[-1] - close.iloc[-2])
                      / close.iloc[-2]), 6)

        if len(close) >= 6:
            result['price_change_5d'] = round(
                float((close.iloc[-1] - close.iloc[-6])
                      / close.iloc[-6]), 6)

        if len(close) >= 21:
            result['price_change_20d'] = round(
                float((close.iloc[-1] - close.iloc[-21])
                      / close.iloc[-21]), 6)

        if len(close) >= 252:
            w52_high = float(high.rolling(252).max().iloc[-1])
            result['distance_from_52w_high'] = round(
                (w52_high - float(close.iloc[-1])) / w52_high, 6)
        elif len(high) > 0:
            w52_high = float(high.max())
            result['distance_from_52w_high'] = round(
                (w52_high - float(close.iloc[-1])) / w52_high, 6)

        # ── Volume metrics ──────────────────────────────────
        if len(volume) >= 2:
            result['volume_today'] = int(volume.iloc[-1])

        if len(volume) >= 30:
            avg_30d = float(volume.rolling(30).mean().iloc[-1])
            result['avg_daily_volume_30d_computed'] = int(avg_30d)
            result['volume_ratio_30d'] = round(
                float(volume.iloc[-1]) / avg_30d, 4) \
                if avg_30d > 0 else 0.0

            # avg hourly = avg daily / 6.5 trading hours
            result['avg_hourly_volume_30d'] = int(avg_30d / 6.5)

        # Float rotation = today's volume / float shares
        if float_shares and float_shares > 0 and len(volume) >= 1:
            result['float_rotation'] = round(
                float(volume.iloc[-1]) / float_shares, 6)

        # Volatility (10d standard deviation of daily returns)
        if len(close) >= 11:
            result['volatility_10d'] = round(
                float(close.pct_change()
                          .rolling(10).std().iloc[-1]), 6)

        # ── Volume acceleration from intraday ───────────────
        if intraday_df is not None and not intraday_df.empty:
            intra_vol = intraday_df['Volume']
            # Last hour vs average hour over available intraday
            if len(intra_vol) >= 120:  # 2 hours of 1min bars
                last_60min = float(intra_vol.iloc[-60:].sum())
                avg_60min  = float(intra_vol.mean() * 60)
                if avg_60min > 0:
                    result['volume_acceleration'] = round(
                        last_60min / avg_60min, 4)

        # ── Technical indicators ────────────────────────────

        # RSI-14
        if len(close) >= 15:
            result['rsi_14'] = round(
                self._compute_rsi(close, 14), 2)

        # MACD (12, 26, 9)
        if len(close) >= 35:
            macd, signal, hist = self._compute_macd(close)
            result['macd']        = round(float(macd), 6)
            result['macd_signal'] = round(float(signal), 6)
            result['macd_hist']   = round(float(hist), 6)

        # ATR-14 (Average True Range)
        if len(close) >= 15:
            result['atr_14'] = round(
                self._compute_atr(high, low, close, 14), 4)

        return result

    # ─────────────────────────────────────────────────────────
    # 4. OPTIONS CHAIN
    # ─────────────────────────────────────────────────────────
    def get_options_summary(self, ticker: str) -> dict:
        """
        Fetches options chain and computes:
          call_put_ratio (by OI), iv_current (avg IV),
          max_pain (strike price), total_call_oi, total_put_oi
        Staleness: 15 min during market hours
        """
        start = time.time()
        try:
            t = make_ticker(ticker)
            expiries = t.options

            if not expiries:
                return {
                    'options_eligible': False,
                    'options_error': 'no_expiries'
                }

            all_calls, all_puts = [], []
            for expiry in expiries[:4]:
                try:
                    chain = t.option_chain(expiry)
                    c = chain.calls.copy()
                    p = chain.puts.copy()
                    c['expiry'] = expiry
                    p['expiry'] = expiry
                    all_calls.append(c)
                    all_puts.append(p)
                except Exception:
                    continue

            if not all_calls:
                return {'options_eligible': False,
                        'options_error': 'chains_empty'}

            calls = pd.concat(all_calls, ignore_index=True)
            puts  = pd.concat(all_puts,  ignore_index=True)

            # Call/Put ratio by open interest
            call_oi = calls['openInterest'].fillna(0).sum()
            put_oi  = puts['openInterest'].fillna(0).sum()
            cp_ratio = round(float(call_oi / put_oi), 4) \
                       if put_oi > 0 else 0.0

            # Current IV — average across all strikes
            all_iv = pd.concat([
                calls['impliedVolatility'],
                puts['impliedVolatility']
            ]).dropna()
            iv_current = round(float(all_iv.mean()), 6) \
                         if not all_iv.empty else 0.0

            # Max pain
            max_pain = self._compute_max_pain(calls, puts)

            result = {
                'options_eligible':  True,
                'call_put_ratio':    cp_ratio,
                'iv_current':        iv_current,
                'iv_pct':            round(iv_current * 100, 2),
                'max_pain':          max_pain,
                'total_call_oi':     int(call_oi),
                'total_put_oi':      int(put_oi),
                'total_call_vol':    int(
                    calls['volume'].fillna(0).sum()),
                'total_put_vol':     int(
                    puts['volume'].fillna(0).sum()),
                'nearest_expiry':    expiries[0],
                'expiry_count':      len(expiries),
                'options_collected_at': datetime.utcnow(),
            }

            self._log(ticker, 'yfinance_options',
                      'success', 1, None,
                      int((time.time()-start)*1000))
            return result

        except Exception as e:
            self._log(ticker, 'yfinance_options',
                      'failed', 0, str(e),
                      int((time.time()-start)*1000))
            return {'options_eligible': False,
                    'options_error': str(e)}

    # ─────────────────────────────────────────────────────────
    # 5. EARNINGS DATE
    # ─────────────────────────────────────────────────────────
    def get_earnings_date(self, ticker: str):
        """
        Returns next earnings date or None.
        Staleness: weekly
        """
        try:
            t = make_ticker(ticker)
            cal = t.calendar
            if cal is not None and not cal.empty:
                # calendar returns a DataFrame
                if 'Earnings Date' in cal.index:
                    val = cal.loc['Earnings Date'].iloc[0]
                    if pd.notna(val):
                        return pd.to_datetime(val).to_pydatetime()
            return None
        except Exception:
            return None

    # ─────────────────────────────────────────────────────────
    # 6. NEWS HEADLINES
    # ─────────────────────────────────────────────────────────
    def get_recent_news(self, ticker: str) -> list:
        """
        Returns list of recent news headlines.
        Staleness: 30 min
        Used for: news_catalyst_flag feature
        """
        try:
            t = make_ticker(ticker)
            raw = t.news or []
            news = []
            for item in raw[:10]:
                news.append({
                    'title':     item.get('title', ''),
                    'publisher': item.get('publisher', ''),
                    'url':       item.get('link', ''),
                    'published': datetime.fromtimestamp(
                        item.get('providerPublishTime', 0),
                        tz=timezone.utc
                    ).isoformat(),
                })
            return news
        except Exception:
            return []

    # ═════════════════════════════════════════════════════════
    # TECHNICAL INDICATOR COMPUTATIONS
    # ═════════════════════════════════════════════════════════

    def _compute_rsi(self, close: pd.Series,
                     period: int = 14) -> float:
        """Relative Strength Index"""
        delta = close.diff()
        gain  = delta.clip(lower=0)
        loss  = -delta.clip(upper=0)
        avg_gain = gain.ewm(com=period-1,
                            min_periods=period).mean()
        avg_loss = loss.ewm(com=period-1,
                            min_periods=period).mean()
        rs  = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return float(rsi.iloc[-1])

    def _compute_macd(self, close: pd.Series,
                      fast: int = 12,
                      slow: int = 26,
                      signal: int = 9):
        """MACD Line, Signal Line, Histogram"""
        ema_fast   = close.ewm(span=fast, adjust=False).mean()
        ema_slow   = close.ewm(span=slow, adjust=False).mean()
        macd_line  = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal,
                                     adjust=False).mean()
        histogram  = macd_line - signal_line
        return (float(macd_line.iloc[-1]),
                float(signal_line.iloc[-1]),
                float(histogram.iloc[-1]))

    def _compute_atr(self, high: pd.Series,
                     low: pd.Series,
                     close: pd.Series,
                     period: int = 14) -> float:
        """Average True Range"""
        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low  - prev_close).abs()
        ], axis=1).max(axis=1)
        atr = tr.ewm(span=period, adjust=False).mean()
        return float(atr.iloc[-1])

    def _compute_max_pain(self, calls: pd.DataFrame,
                           puts: pd.DataFrame) -> float:
        """
        Max pain = strike where total option buyer losses are maximized.
        Market makers are incentivized to let price expire here.
        """
        strikes = pd.concat([
            calls['strike'], puts['strike']
        ]).unique()

        if len(strikes) == 0:
            return 0.0

        min_pain       = float('inf')
        max_pain_price = 0.0

        for strike in sorted(strikes):
            itm_calls = calls[calls['strike'] < strike]
            call_pain = float(
                (itm_calls['openInterest'].fillna(0)
                 * (strike - itm_calls['strike'])).sum()
            )

            itm_puts = puts[puts['strike'] > strike]
            put_pain = float(
                (itm_puts['openInterest'].fillna(0)
                 * (itm_puts['strike'] - strike)).sum()
            )

            total = call_pain + put_pain
            if total < min_pain:
                min_pain       = total
                max_pain_price = strike

        return float(max_pain_price)

    # ═════════════════════════════════════════════════════════
    # DATABASE WRITES
    # ═════════════════════════════════════════════════════════

    def _save_ohlcv(self, ticker: str,
                    df: pd.DataFrame,
                    interval: str) -> int:
        """Writes OHLCV rows to TimescaleDB stock_timeseries."""
        cur   = self.conn.cursor()
        rows  = 0
        for ts, row in df.iterrows():
            try:
                cur.execute("""
                    INSERT INTO stock_timeseries
                        (time, ticker, open, high, low,
                         close, volume, interval)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (
                    ts.to_pydatetime(),
                    ticker,
                    float(row['Open'])   if pd.notna(row['Open'])   else None,
                    float(row['High'])   if pd.notna(row['High'])   else None,
                    float(row['Low'])    if pd.notna(row['Low'])    else None,
                    float(row['Close'])  if pd.notna(row['Close'])  else None,
                    int(row['Volume'])   if pd.notna(row['Volume']) else None,
                    interval,
                ))
                rows += 1
            except Exception:
                continue
        self.conn.commit()
        return rows

    def _save_snapshot(self, ticker: str, data: dict):
        """Writes the derived feature snapshot to stock_snapshots."""
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO stock_snapshots (
                ticker, float_shares, shares_outstanding,
                data_source, collected_at
            ) VALUES (%s, %s, %s, %s, %s)
        """, (
            ticker,
            data.get('float_shares'),
            data.get('shares_outstanding'),
            'yfinance',
            datetime.utcnow(),
        ))

        # Upsert stock record with latest metadata
        cur.execute("""
            INSERT INTO stocks (ticker, company_name, sector)
            VALUES (%s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE
              SET company_name = EXCLUDED.company_name,
                  sector       = EXCLUDED.sector,
                  updated_at   = NOW()
        """, (
            ticker,
            data.get('company_name', ''),
            data.get('sector', ''),
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
        """Creates any tables not in the main schema."""
        cur = self.conn.cursor()
        # yfinance_snapshots for derived features
        cur.execute("""
            CREATE TABLE IF NOT EXISTS yfinance_snapshots (
                id                      SERIAL PRIMARY KEY,
                ticker                  VARCHAR(10),
                current_price           FLOAT,
                price_change_1d         FLOAT,
                price_change_5d         FLOAT,
                price_change_20d        FLOAT,
                volume_today            BIGINT,
                volume_ratio_30d        FLOAT,
                avg_hourly_volume_30d   BIGINT,
                float_rotation          FLOAT,
                volume_acceleration     FLOAT,
                volatility_10d          FLOAT,
                distance_from_52w_high  FLOAT,
                rsi_14                  FLOAT,
                macd                    FLOAT,
                macd_signal             FLOAT,
                macd_hist               FLOAT,
                atr_14                  FLOAT,
                call_put_ratio          FLOAT,
                iv_current              FLOAT,
                max_pain                FLOAT,
                total_call_oi           BIGINT,
                total_put_oi            BIGINT,
                earnings_date           TIMESTAMP,
                news_count              INTEGER,
                collected_at            TIMESTAMP DEFAULT NOW()
            )
        """)
        self.conn.commit()

    @staticmethod
    def _fmt_shares(n) -> str:
        if n is None:
            return 'N/A'
        if n >= 1_000_000_000:
            return f'{n/1_000_000_000:.1f}B'
        if n >= 1_000_000:
            return f'{n/1_000_000:.1f}M'
        return f'{n:,}'

    def close(self):
        self.conn.close()


# ─────────────────────────────────────────────────────────────
# QUICK TEST — run this file directly
# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    c = YFinanceCollector()
    result = c.collect_all('GME')
    print('\n── Key features ──')
    keys = [
        'current_price', 'volume_ratio_30d', 'float_rotation',
        'avg_hourly_volume_30d', 'volume_acceleration',
        'rsi_14', 'macd', 'atr_14',
        'call_put_ratio', 'iv_current', 'max_pain',
        'earnings_date', 'news_count',
    ]
    for k in keys:
        print(f'  {k:35s} = {result.get(k)}')
    c.close()