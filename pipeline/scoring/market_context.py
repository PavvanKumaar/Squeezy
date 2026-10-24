# pipeline/scoring/market_context.py
#
# Reads daily bars from stock_timeseries (same table/columns the candidate
# evaluator already queries) to get: price, 20d dollar volume (for the model
# eligibility gate) and the bar history the lifecycle tracker needs.

import os
from datetime import datetime
from typing import Optional

from pipeline.scoring.config import MIN_BARS_FOR_CONTEXT
from pipeline.scoring import session


class MarketContextReader:
    def __init__(self, pg=None):
        if pg is None:
            import psycopg2
            pg = psycopg2.connect(os.getenv('POSTGRES_URL'))
        self.pg = pg

    def load_daily_bars(self, ticker: str, n: int = 80, completed_only: bool = True,
                        now=None) -> list:
        """Ascending list of {'date', 'close', 'volume'}, one per calendar day.
        completed_only drops today's partial bar until 16:05 ET (models were trained
        on finished daily bars)."""
        cur = self.pg.cursor()
        try:
            cur.execute("""
                SELECT time, close, volume
                FROM   stock_timeseries
                WHERE  ticker = %s AND interval = '1d' AND close IS NOT NULL
                ORDER  BY time DESC
                LIMIT  %s
            """, (ticker, n * 2))
            rows = cur.fetchall()
        except Exception:
            self.pg.rollback()
            return []
        by_day = {}
        for t, c, v in rows:                    # DESC: keep the newest row per day
            d = t.date() if hasattr(t, 'date') else t
            if d not in by_day:
                by_day[d] = {'date': d, 'close': float(c),
                             'volume': float(v or 0.0)}
        bars = sorted(by_day.values(), key=lambda b: b['date'])
        if completed_only:
            try:
                cutoff = session.last_completed_session_date(now)
                bars = [b for b in bars if b['date'] <= cutoff]
            except Exception:
                pass
        return bars[-n:]

    @staticmethod
    def build_context(bars: list, today=None) -> dict:
        """price, dollar_vol_20d (mean close*volume over the 20 PRIOR bars,
        matching training), n_bars. Values are None when not computable."""
        ctx = {'price': None, 'dollar_vol_20d': None, 'n_bars': len(bars),
               'last_bar_date': None, 'bar_age_days': None}
        if not bars:
            return ctx
        ctx['price'] = bars[-1]['close']
        ctx['last_bar_date'] = str(bars[-1]['date'])
        try:
            today = today or session.et_now().date()
            ctx['bar_age_days'] = (today - bars[-1]['date']).days
        except Exception:
            pass
        prior = bars[-21:-1]
        if len(prior) >= MIN_BARS_FOR_CONTEXT:
            ctx['dollar_vol_20d'] = sum(b['close'] * b['volume']
                                        for b in prior) / len(prior)
        return ctx