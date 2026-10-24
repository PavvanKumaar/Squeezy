# pipeline/scoring/session.py
#
# US market-session helpers (America/New_York). Weekends are handled; exchange
# holidays are NOT (install pandas_market_calendars if you need them).
# Windows needs:  pip install tzdata   (zoneinfo has no timezone database there)

from datetime import datetime, timedelta

import pandas as pd

CLOSE_BUFFER_MIN = 5          # a session counts as complete 5 min after 16:00 ET


def _tz():
    from zoneinfo import ZoneInfo
    return ZoneInfo('America/New_York')


def et_now(now=None):
    tz = _tz()
    return now.astimezone(tz) if now else datetime.now(tz)


def market_session_state(now=None):
    """'weekend' | 'pre' | 'open' | 'post'; None if timezone data is missing."""
    try:
        t = et_now(now)
    except Exception:
        return None
    if t.weekday() >= 5:
        return 'weekend'
    m = t.hour * 60 + t.minute
    return 'pre' if m < 9 * 60 + 30 else 'open' if m < 16 * 60 else 'post'


def last_completed_session_date(now=None):
    """Date of the most recent trading session whose daily bar is final."""
    t = et_now(now)
    d = t.date()
    done_today = t.weekday() < 5 and (t.hour * 60 + t.minute) >= 16 * 60 + CLOSE_BUFFER_MIN
    if not done_today:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def drop_partial_today(df, time_col='time', now=None):
    """Remove daily rows newer than the last completed session (partial bars).
    Safe no-op if timezone data is unavailable."""
    try:
        cutoff = last_completed_session_date(now)
        ts = pd.to_datetime(df[time_col])
        if getattr(ts.dt, 'tz', None) is not None:
            ts = ts.dt.tz_convert(_tz())
        return df[ts.dt.date <= cutoff]
    except Exception:
        return df