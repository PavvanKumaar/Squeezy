# notebooks/01_build_training_data_v1_1.py
#
# SqueezeRadar - Training Data Builder v1.1 (OHLCV-only, no synthetic data)
#
# What changed vs v1
#   1. EVENT ANCHORS FROM DATA. Your hand-picked dates are only *candidates*.
#      Ignition / peak / end are re-derived from OHLCV with fixed rules, and every
#      curated event gets a status + use_for_training flag + reason
#      (nothing is silently dropped).
#   2. UNIVERSE PANEL + EVENT MINER + NEGATIVES. The forward-return target
#      (squeezed_5d / squeezed_10d) is computed for EVERY liquid ticker-day in a
#      broad universe with one uniform rule, so positives and negatives come from
#      the same process (no selection bias). Event windows (curated + mined) are
#      kept in full; other rows are subsampled and carry sample_weight = 1/keep_frac
#      so weighted statistics recover the natural base rate.
#      Chronological split: train < 2021-07, val 2021-07..2021-12, test 2022+,
#      with a 21-day embargo before each boundary.
#   3. GROUP CV BY CLUSTER, NOT EVENT. Every row gets group_id = calendar quarter
#      (events are assigned by their IGNITION quarter, so a whole squeeze wave and
#      each full event stay together) and cv_fold (greedy-balanced by positives).
#
# Outputs (data/training/)
#   v1_1_raw.parquet          real rows only (panel, subsampled negatives + weights)
#   v1_1_events.csv           every curated + mined event with anchors/status
#   dataset_metrics_v1_1.txt  evaluation report
#   data/universe/tickers.csv frozen universe list (delete it to resample)
#
# Run:   python notebooks/01_build_training_data_v1_1.py
# Needs: pip install -U yfinance pandas numpy pyarrow
#
# Training script usage (next step):
#   d = pd.read_parquet('data/training/v1_1_raw.parquet')
#   d = d[d.row_ok & d.split.isin(['train','val'])]
#   GroupKFold-style: for k in range(5): train = d[d.cv_fold != k]; valid = d[d.cv_fold == k]
#   pass sample_weight=d.sample_weight to LightGBM; also drop train rows within
#   ~21 days of a validation block edge (embargo).
#   Final report: d[d.split == 'test'] only, once.

import io
import re
import sys
import time
import urllib.request
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / 'data' / 'training'
UNIVERSE_DIR = ROOT / 'data' / 'universe'

UNIVERSE_SIZE = 1000          # random sample of current US listings
UNIVERSE_SEED = 42
DOWNLOAD_START = '2017-01-01'  # warmup for 252d features
PANEL_START = '2018-01-01'     # first row emitted
DOWNLOAD_END = None            # None = today
BATCH_SIZE = 50
USE_CACHE = True               # cache/<SYMBOL>.parquet ; delete folder to refresh
MIN_ROWS_PER_SYMBOL = 120

# scanner eligibility (causal, per row). Mirror this in the live scanner.
# v1.2 — lowered eligibility thresholds
MIN_PRICE = 0.10
MIN_DOLLAR_VOL = 50_000        # trailing 20d average dollar volume (prior days)
MIN_HISTORY_ROWS = 60

# forward target
SQUEEZE_RETURN_THRESHOLD = 0.30

# data-driven ignition rule
IGN_VOLRATIO = 3.0
IGN_1D = 0.10
IGN_5D = 0.25
CONFIRM_MIN_GAIN = 0.30        # ignition-day close -> peak close within lookahead
PEAK_LOOKAHEAD = 30            # trading days
COOLDOWN_DAYS = 15             # trading days after peak
SETUP_DAYS = 60                # trading days before ignition
MIN_HISTORY_FOR_EVENT = 120    # trading days of history needed at ignition
FAILED_MAX_GAIN = 0.50         # a hand-labeled 'failed' event may not gain more than this

# negatives
NEG_KEEP_FRAC = 0.20           # fraction of non-event rows kept (weight = 1/frac)
NEG_SEED = 7

# splits
VAL_START = '2021-07-01'
TEST_START = '2022-01-01'
EMBARGO_CAL_DAYS = 21
N_FOLDS = 5

RAW_FEATURES = [
    'volume_ratio_30d', 'price_change_1d', 'price_change_5d', 'rsi_14',
    'distance_from_52w_high', 'atr_pct', 'volatility_10d',
]
NORM_FEATURES = [
    'volume_ratio_30d_norm', 'price_change_1d_norm', 'price_change_5d_norm',
    'rsi_14_norm', 'distance_from_52w_high_norm', 'atr_norm',
    'volatility_10d_norm',
]
RAW_TO_NORM = dict(zip(RAW_FEATURES, NORM_FEATURES))
# v1.2 — additional derived features for Model 3
V12_EXTRA_FEATURES = [
    'abs_price_change_1d_norm',
    'abs_price_change_5d_norm',
]
BOUNDS = {
    'volume_ratio_30d':       (0.0, 20.0),
    'price_change_1d':        (-1.0, 1.0),
    'price_change_5d':        (-1.0, 1.0),
    'rsi_14':                 (0.0, 100.0),
    'distance_from_52w_high': (0.0, 1.0),
    'atr_pct':                (0.0, 1.0),
    'volatility_10d':         (0.0, 0.5),
}
PHASE_STATUSES = ('confirmed', 'relabeled_squeeze', 'mined_squeeze')

# v1.2 — split-artifact guard
SPLIT_RATIOS = [0.5, 1/3, 0.25, 0.2, 0.1, 2.0, 3.0, 4.0, 5.0, 10.0]
SPLIT_TOL = 0.02
SPLIT_VOLRATIO_MAX = 1.5

# Alternate symbols (UNVERIFIED - report shows which was used)
TICKER_ALIASES = {'PROG': ['PROG', 'BRTX']}

# (ticker, setup_start, squeeze_start, squeeze_peak, squeeze_end, hand_failed, tier, event_id)
_EVENT_ROWS = [
    ('GME',  '2020-11-13', '2021-01-13', '2021-01-28', '2021-02-19', False, 1, None),
    ('AMC',  '2021-03-24', '2021-05-24', '2021-06-02', '2021-06-30', False, 1, None),
    ('KOSS', '2020-11-22', '2021-01-22', '2021-01-28', '2021-02-05', False, 1, None),
    ('BBBY', '2022-05-01', '2022-08-05', '2022-08-11', '2022-08-31', False, 1, None),
    ('AMC',  '2023-04-01', '2023-05-30', '2023-06-02', '2023-06-15', False, 1, 'AMC_2023'),
    ('BB',   '2020-11-22', '2021-01-22', '2021-01-27', '2021-02-05', False, 2, None),
    ('NOK',  '2020-11-22', '2021-01-22', '2021-01-27', '2021-02-05', False, 2, None),
    ('EXPR', '2020-11-27', '2021-01-27', '2021-01-28', '2021-02-05', False, 2, None),
    ('CLOV', '2021-04-07', '2021-06-07', '2021-06-09', '2021-06-20', False, 2, None),
    ('SENS', '2021-04-12', '2021-06-10', '2021-06-14', '2021-07-14', False, 3, None),
    ('MVIS', '2021-02-14', '2021-04-14', '2021-04-28', '2021-05-15', False, 2, None),
    ('TLRY', '2020-12-10', '2021-02-08', '2021-02-10', '2021-02-20', False, 2, None),
    ('SPCE', '2020-12-01', '2021-02-01', '2021-02-08', '2021-02-20', False, 2, None),
    ('WKHS', '2020-12-16', '2021-02-16', '2021-02-18', '2021-03-18', False, 3, None),
    ('SPRT', '2021-06-28', '2021-08-24', '2021-08-27', '2021-09-26', False, 1, 'SPRT_2021'),
    ('HKD',  '2022-06-02', '2022-08-01', '2022-08-02', '2022-09-01', False, 1, 'HKD_2022'),
    ('PROG', '2021-08-20', '2021-10-19', '2021-10-29', '2021-11-28', False, 2, 'PROG_2021'),
    ('ATER', '2021-07-22', '2021-09-20', '2021-09-22', '2021-10-22', False, 2, 'ATER_2021'),
    ('MULN', '2022-02-01', '2022-04-01', '2022-04-04', '2022-05-04', False, 3, 'MULN_2022'),
    ('FIZZ', '2020-12-15', '2021-02-15', '2021-02-20', '2021-03-15', True,  2, None),
    ('FCEL', '2020-11-09', '2021-01-08', '2021-01-21', '2021-02-20', False, 3, None),
    ('LGND', '2020-12-01', '2021-01-15', '2021-01-20', '2021-02-01', True,  2, None),
    ('BGFV', '2021-09-06', '2021-11-03', '2021-11-11', '2021-12-11', True,  3, None),
]
CURATED = [{
    'ticker': t, 'setup_start': s, 'squeeze_start': ss, 'squeeze_peak': sp,
    'squeeze_end': se, 'hand_failed': f, 'tier': tier,
    'event_id': eid or f'{t}_{ss[:4]}'} for t, s, ss, sp, se, f, tier, eid in _EVENT_ROWS]


# ─────────────────────────────────────────────────────────────
# UNIVERSE
# ─────────────────────────────────────────────────────────────
def _read_symdir(url):
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    text = urllib.request.urlopen(req, timeout=30).read().decode('utf-8', 'ignore')
    df = pd.read_csv(io.StringIO(text), sep='|')
    return df[~df.iloc[:, 0].astype(str).str.startswith('File Creation')]


def fetch_us_symbols():
    """Current US listings from nasdaqtrader.com (common stock only, best effort)."""
    bad_name = re.compile(r'Warrant|Right|Unit|Preferred|Note|Debenture|Depositary|'
                          r'Acquisition Corp|Trust|Fund|ETF|ETN', re.I)
    n = _read_symdir('https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt')
    n = n[(n['Test Issue'] == 'N') & (n['ETF'] == 'N')]
    syms = n[~n['Security Name'].astype(str).str.contains(bad_name)]['Symbol'].tolist()
    o = _read_symdir('https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt')
    o = o[(o['Test Issue'] == 'N') & (o['ETF'] == 'N')]
    syms += o[~o['Security Name'].astype(str).str.contains(bad_name)]['ACT Symbol'].tolist()
    return sorted({s for s in map(str, syms) if re.fullmatch(r'[A-Z]{1,5}', s)})


def curated_symbols():
    out = set()
    for ev in CURATED:
        out.update(TICKER_ALIASES.get(ev['ticker'], [ev['ticker']]))
    return out


def load_universe():
    UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)
    path = UNIVERSE_DIR / 'tickers.csv'
    if path.exists():
        syms = pd.read_csv(path)['symbol'].dropna().astype(str).tolist()
        print(f'  universe: loaded {len(syms)} symbols from {path}')
    else:
        try:
            allsyms = fetch_us_symbols()
        except Exception as e:  # noqa
            raise SystemExit(
                f'Could not download the symbol list ({e!r}).\n'
                f'Create {path} with a column named "symbol" and rerun.')
        rng = np.random.default_rng(UNIVERSE_SEED)
        syms = (list(rng.choice(allsyms, UNIVERSE_SIZE, replace=False))
                if len(allsyms) > UNIVERSE_SIZE else allsyms)
        pd.DataFrame({'symbol': syms}).to_csv(path, index=False)
        print(f'  universe: sampled {len(syms)} of {len(allsyms)} listings -> {path}')
    return sorted(set(syms) | curated_symbols())


# ─────────────────────────────────────────────────────────────
# DOWNLOAD
# ─────────────────────────────────────────────────────────────
def _clean_ohlcv(df):
    cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    if df is None or df.empty or any(c not in df.columns for c in cols):
        return pd.DataFrame()
    df = df[cols].astype(float).dropna(subset=['Close'])
    df['Volume'] = df['Volume'].fillna(0.0)
    df.index = pd.to_datetime(df.index)
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    df.index = df.index.normalize()
    df.index.name = 'date'
    return df[~df.index.duplicated(keep='last')].sort_index()


def _cache_read(cache, s):
    """Pickle cache first (no pyarrow needed); old parquet cache as a fallback."""
    pk = cache / f'{s}.pkl'
    if pk.exists():
        try:
            return pd.read_pickle(pk)
        except Exception:  # noqa
            pass
    pq = cache / f'{s}.parquet'
    if pq.exists():
        try:
            return pd.read_parquet(pq)
        except Exception:  # noqa
            pass
    return None


def download_symbols(symbols):
    """Returns (dict symbol -> OHLCV, dict symbol -> error)."""
    cache = OUTPUT_DIR / 'cache'
    cache.mkdir(parents=True, exist_ok=True)
    end = DOWNLOAD_END or date.today().strftime('%Y-%m-%d')
    end_excl = (pd.to_datetime(end) + timedelta(days=1)).strftime('%Y-%m-%d')
    out, errors, todo = {}, {}, []
    for s in symbols:
        df = _cache_read(cache, s) if USE_CACHE else None
        if df is not None:
            out[s] = df
        else:
            todo.append(s)
    print(f'  downloads: {len(out)} cached, {len(todo)} to fetch')
    for i in range(0, len(todo), BATCH_SIZE):
        batch = todo[i:i + BATCH_SIZE]
        try:
            raw = yf.download(batch, start=DOWNLOAD_START, end=end_excl,
                              interval='1d', auto_adjust=True,
                              group_by='ticker', threads=True, progress=False)
        except Exception as e:  # noqa
            for s in batch:
                errors[s] = repr(e)
            continue
        for s in batch:
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    sub = raw[s] if s in raw.columns.get_level_values(0) else None
                else:
                    sub = raw if len(batch) == 1 else None
                df = _clean_ohlcv(sub)
            except Exception as e:  # noqa
                errors[s] = repr(e)
                continue
            if df.empty:
                errors[s] = 'empty response (delisted, renamed, or not on Yahoo)'
            elif len(df) < MIN_ROWS_PER_SYMBOL:
                errors[s] = f'only {len(df)} rows'
            else:
                out[s] = df
                if USE_CACHE:
                    df.to_pickle(cache / f'{s}.pkl')
        print(f'    batch {i // BATCH_SIZE + 1}/{(len(todo) - 1) // BATCH_SIZE + 1} done '
              f'({len(out)} ok, {len(errors)} failed)')
        time.sleep(1.0)
    return out, errors


# ─────────────────────────────────────────────────────────────
# FEATURES + LABELS (point-in-time; row t uses data up to t)
# ─────────────────────────────────────────────────────────────
def compute_rsi(close, period=14):
    delta = close.diff()
    gain, loss = delta.clip(lower=0), -delta.clip(upper=0)
    ag = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    al = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rsi = 100 - 100 / (1 + ag / al.replace(0, np.nan))
    rsi = rsi.where(~((al == 0) & (ag > 0)), 100.0)
    return rsi.where(~((al == 0) & (ag == 0)), 50.0)


def compute_atr(high, low, close, period=14):
    prev = close.shift(1)
    tr = pd.concat([high - low, (high - prev).abs(), (low - prev).abs()],
                   axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def future_max(close, n):
    """max(close[t+1..t+n]); NaN when the window is incomplete."""
    arr = close.values
    out = np.full(len(arr), np.nan)
    if len(arr) > n:
        w = np.lib.stride_tricks.sliding_window_view(arr[1:], n)
        out[:len(arr) - n] = w.max(axis=1)
    return pd.Series(out, index=close.index)


def compute_features(ohlcv):
    close, high, low, vol = (ohlcv['Close'], ohlcv['High'],
                             ohlcv['Low'], ohlcv['Volume'])
    f = pd.DataFrame(index=ohlcv.index)
    f['close'] = close
    f['volume'] = vol
    f['history_days'] = np.arange(len(f))
    f['dollar_vol_20d'] = (close * vol).shift(1).rolling(20, min_periods=15).mean()

    # vs the PRIOR 30 days (today excluded) - live pipeline must match
    avg30 = vol.shift(1).rolling(30, min_periods=20).mean()
    f['volume_ratio_30d'] = vol / avg30.replace(0, np.nan)
    f['price_change_1d'] = close.pct_change(1)
    f['price_change_5d'] = close.pct_change(5)
    f['rsi_14'] = compute_rsi(close, 14)
    roll_high = high.rolling(252, min_periods=60).max()
    f['distance_from_52w_high'] = (roll_high - close) / roll_high.replace(0, np.nan)
    f['atr_pct'] = compute_atr(high, low, close, 14) / close.replace(0, np.nan)
    f['volatility_10d'] = close.pct_change(1).rolling(10, min_periods=10).std()

    for raw, norm in RAW_TO_NORM.items():
        lo, hi = BOUNDS[raw]
        c = f[raw].clip(lo, hi)
        f[norm] = c / max(abs(lo), abs(hi)) if lo < 0 else (c - lo) / (hi - lo)

    # forward labels; NaN if the window is incomplete OR spans a data gap
    dates = pd.Series(ohlcv.index, index=ohlcv.index)
    for n, max_span in ((5, 10), (10, 18)):
        ok = (dates.shift(-n) - dates).dt.days <= max_span
        f[f'return_{n}d'] = (close.shift(-n) / close - 1).where(ok)
        f[f'max_return_{n}d'] = (future_max(close, n) / close - 1).where(ok)
        r = f[f'return_{n}d']
        f[f'squeezed_{n}d'] = (r >= SQUEEZE_RETURN_THRESHOLD).astype('Int8').where(r.notna())

    f['eligible'] = ((close >= MIN_PRICE) & (f['dollar_vol_20d'] >= MIN_DOLLAR_VOL)
                     & (f['history_days'] >= MIN_HISTORY_ROWS))

    # ── v1.2: forward volatility and min-return labels ───────
    ret = close.pct_change()
    # fwd_vol_10d: std of 10 daily returns starting at t+1
    # built as rolling(10) shifted back so index t has the fwd window
    fwd_ret_shifted = ret.shift(-1)
    raw_fwd_vol = fwd_ret_shifted.rolling(10, min_periods=10).std().shift(9)
    dates_s = pd.Series(ohlcv.index, index=ohlcv.index)
    ok_10 = (dates_s.shift(-10) - dates_s).dt.days <= 18
    f['fwd_vol_10d'] = raw_fwd_vol.where(ok_10)

    # min_return_5d: min(close[t+1..t+5])/close[t] - 1
    arr = close.values
    n_rows = len(arr)
    min5 = np.full(n_rows, np.nan)
    if n_rows > 5:
        w5 = np.lib.stride_tricks.sliding_window_view(arr[1:], 5)
        min5[:n_rows - 5] = w5.min(axis=1)
    f['min_return_5d'] = pd.Series(min5, index=close.index) / close - 1
    ok_5 = (dates_s.shift(-5) - dates_s).dt.days <= 10
    f['min_return_5d'] = f['min_return_5d'].where(ok_5)

    # min_return_10d: min(close[t+1..t+10])/close[t] - 1
    min10 = np.full(n_rows, np.nan)
    if n_rows > 10:
        w10 = np.lib.stride_tricks.sliding_window_view(arr[1:], 10)
        min10[:n_rows - 10] = w10.min(axis=1)
    f['min_return_10d'] = pd.Series(min10, index=close.index) / close - 1
    f['min_return_10d'] = f['min_return_10d'].where(ok_10)

    # optional: baseline_vol_60d_lag10 (60d std using t-69..t-10)
    f['baseline_vol_60d_lag10'] = ret.shift(10).rolling(60, min_periods=30).std()

    # v1.2 derived norm columns
    f['abs_price_change_1d_norm'] = f['price_change_1d_norm'].abs()
    f['abs_price_change_5d_norm'] = f['price_change_5d_norm'].abs()

    # v1.2 trigger flags (from _norm columns for runtime parity)
    f['ign_trigger'] = (
        (f['volume_ratio_30d_norm'] >= 0.15) &
        ((f['price_change_1d_norm'] >= 0.10) | (f['price_change_5d_norm'] >= 0.25))
    )
    f['exh_trigger'] = (
        (f['price_change_5d_norm'] >= 0.25) | (f['rsi_14_norm'] >= 0.80)
    )

    return f


def apply_split_guard(f):
    """Flag rows whose forward windows contain a suspicious split-ratio price jump.
    Sets fwd_vol_10d, min_return_5d, min_return_10d, max_return_5d, max_return_10d,
    return_5d, return_10d, squeezed_5d, squeezed_10d to NaN for those rows.
    Returns count of rows invalidated."""
    close_vals = f['close'].values
    vol_ratio = f['volume_ratio_30d'].values
    n = len(close_vals)
    suspect = np.zeros(n, dtype=bool)
    for i in range(1, n):
        if close_vals[i - 1] <= 0:
            continue
        ratio = close_vals[i] / close_vals[i - 1]
        for sr in SPLIT_RATIOS:
            if abs(ratio - sr) / sr <= SPLIT_TOL:
                # suspicious price jump — check volume
                if vol_ratio[i] < SPLIT_VOLRATIO_MAX:
                    suspect[i] = True
                break
    # invalidate any row whose forward window contains a suspect day
    fwd_targets = ['fwd_vol_10d', 'min_return_5d', 'min_return_10d',
                   'max_return_5d', 'max_return_10d', 'return_5d', 'return_10d',
                   'squeezed_5d', 'squeezed_10d']
    invalidated = np.zeros(n, dtype=bool)
    for j in np.where(suspect)[0]:
        start = max(0, j - 10)
        invalidated[start:j] = True
    count = int(invalidated.sum())
    for col in fwd_targets:
        if col in f.columns:
            f.loc[f.index[invalidated], col] = np.nan
    return f, count


# ─────────────────────────────────────────────────────────────
# EVENT ANCHORS FROM DATA
# ─────────────────────────────────────────────────────────────
def ignition_mask(f, require_eligible, min_price):
    m = ((f['volume_ratio_30d'] >= IGN_VOLRATIO)
         & ((f['price_change_1d'] >= IGN_1D) | (f['price_change_5d'] >= IGN_5D))
         & (f['close'] >= min_price))
    if require_eligible:
        m &= f['eligible'].shift(1, fill_value=False).astype(bool)
    return m.fillna(False)


def derive_episode(f, i):
    """Anchors for an ignition at integer position i, or None if the lookahead
    window is incomplete (episode unfinished / data ends)."""
    n = len(f)
    if i + PEAK_LOOKAHEAD >= n:
        return None
    close = f['close'].values
    p = i + int(np.argmax(close[i:i + PEAK_LOOKAHEAD + 1]))
    vr = f['volume_ratio_30d'].values[max(i - 2, 0):p + 1]
    return {'i': i, 'p': p, 'end': min(p + COOLDOWN_DAYS, n - 1),
            'setup': max(i - SETUP_DAYS, 0),
            'gain': close[p] / close[i] - 1,
            'maxvr': float(np.nanmax(vr)) if np.isfinite(vr).any() else np.nan}


def phase_for(d, ign, peak):
    if d < ign:
        return 'Early Setup' if (ign - d).days > 14 else 'Heating Up'
    if d <= peak:
        return 'Squeeze Zone'
    return 'Danger Zone' if (d - peak).days <= 5 else 'Cooldown'


def pick_curated_symbol(ev, ohlcv_map):
    ss, se = pd.to_datetime(ev['setup_start']), pd.to_datetime(ev['squeeze_end'])
    best, best_n = None, 0
    for sym in TICKER_ALIASES.get(ev['ticker'], [ev['ticker']]):
        df = ohlcv_map.get(sym)
        if df is None:
            continue
        n = int(((df.index >= ss) & (df.index <= se)).sum())
        if n > best_n:
            best, best_n = sym, n
    return best


# ─────────────────────────────────────────────────────────────
# PER-SYMBOL PROCESSING
# ─────────────────────────────────────────────────────────────
def process_symbol(sym, ohlcv, curated_evs, rng):
    f = compute_features(ohlcv)
    n, dates = len(f), f.index
    close = f['close'].values
    ev_idx = np.full(n, -1, dtype=int)
    phase = np.full(n, None, dtype=object)
    dfi = np.full(n, np.nan)
    events = []

    def mark(a, b, k, ign, peak_or_none, use, status):
        for pos in range(a, b + 1):
            if ev_idx[pos] != -1:
                continue
            ev_idx[pos] = k
            dfi[pos] = (dates[pos] - ign).days
            if use and status in PHASE_STATUSES:
                phase[pos] = phase_for(dates[pos], ign, peak_or_none)
            elif use and status == 'confirmed_no_squeeze':
                phase[pos] = 'Near Miss'

    # ── curated events: re-anchor from data ───────────────────
    ign_all = ignition_mask(f, False, 0.0)
    for ev in curated_evs:
        hs, he = pd.to_datetime(ev['squeeze_start']), pd.to_datetime(ev['squeeze_end'])
        hsetup, hp = pd.to_datetime(ev['setup_start']), pd.to_datetime(ev['squeeze_peak'])
        cands = np.where(ign_all.values & (dates >= hs - timedelta(days=21))
                         & (dates <= he))[0]
        chosen = None
        for i in cands:
            ep = derive_episode(f, int(i))
            if ep and ep['gain'] >= CONFIRM_MIN_GAIN:
                chosen = ep
                break
        rec = {'event_id': ev['event_id'], 'source': 'curated', 'ticker': ev['ticker'],
               'symbol': sym, 'tier': ev['tier'], 'hand_failed': ev['hand_failed'],
               'hand_start': hs.date(), 'hand_peak': hp.date(),
               'ignition_date': pd.NaT, 'peak_date': pd.NaT, 'end_date': pd.NaT,
               'gain_pct': np.nan, 'max_vol_ratio': np.nan, 'hist_days': np.nan,
               'status': None, 'use_for_training': False, 'reason': ''}
        k = len(events)
        if chosen:
            i, p = chosen['i'], chosen['p']
            ign, peak = dates[i], dates[p]
            rec.update(ignition_date=ign.date(), peak_date=peak.date(),
                       end_date=dates[chosen['end']].date(),
                       gain_pct=round(100 * chosen['gain'], 1),
                       max_vol_ratio=round(chosen['maxvr'], 1),
                       hist_days=int(f['history_days'].iat[i]))
            if rec['hist_days'] < MIN_HISTORY_FOR_EVENT:
                rec.update(status='insufficient_history',
                           reason=f'only {rec["hist_days"]} trading days of history at ignition (IPO)')
            else:
                rec.update(status='relabeled_squeeze' if ev['hand_failed'] else 'confirmed',
                           use_for_training=True,
                           reason='hand label failed but OHLCV shows a squeeze'
                           if ev['hand_failed'] else '')
            mark(chosen['setup'], chosen['end'], k, ign, peak, rec['use_for_training'], rec['status'])
            rec['_ign'] = ign
        else:
            a = int(np.searchsorted(dates, hsetup, 'left'))
            b = int(np.searchsorted(dates, he, 'right')) - 1
            pos_hs = int(np.searchsorted(dates, hs, 'left'))
            if b - a < 5:
                rec.update(status='no_data', reason='symbol has <5 rows inside the hand window')
                events.append(rec)
                continue
            gain_win = (close[pos_hs:b + 1].max() / close[pos_hs] - 1
                        if pos_hs <= b else np.nan)
            rec['gain_pct'] = round(100 * gain_win, 1) if pd.notna(gain_win) else np.nan
            rec['hist_days'] = int(f['history_days'].iat[min(pos_hs, n - 1)])
            if ev['hand_failed'] and pd.notna(gain_win) and gain_win <= FAILED_MAX_GAIN:
                rec.update(status='confirmed_no_squeeze', use_for_training=True,
                           reason='no ignition found; price gain stayed small (clean near-miss)')
            elif ev['hand_failed']:
                rec.update(status='ambiguous',
                           reason=f'labeled failed but gained {rec["gain_pct"]}% in window')
            else:
                rec.update(status='unconfirmed',
                           reason='no ignition (vol>=3x & jump) followed by a >=30% gain')
            mark(a, b, k, hs, hp, rec['use_for_training'], rec['status'])
            rec['_ign'] = hs
        events.append(rec)

    # ── mined events ──────────────────────────────────────────
    ign_mine = ignition_mask(f, True, MIN_PRICE).values & (dates >= pd.to_datetime(PANEL_START))
    next_ok = 0
    for i in np.where(ign_mine)[0]:
        i = int(i)
        if i < next_ok or ev_idx[i] != -1 or f['history_days'].iat[i] < MIN_HISTORY_FOR_EVENT:
            continue
        ep = derive_episode(f, i)
        if ep is None or ep['gain'] < CONFIRM_MIN_GAIN:
            continue
        ign, peak = dates[i], dates[ep['p']]
        k = len(events)
        events.append({'event_id': f'M_{sym}_{ign:%Y%m%d}', 'source': 'mined', 'ticker': sym,
                       'symbol': sym, 'tier': 4, 'hand_failed': False,
                       'hand_start': None, 'hand_peak': None,
                       'ignition_date': ign.date(), 'peak_date': peak.date(),
                       'end_date': dates[ep['end']].date(),
                       'gain_pct': round(100 * ep['gain'], 1),
                       'max_vol_ratio': round(ep['maxvr'], 1),
                       'hist_days': int(f['history_days'].iat[i]),
                       'status': 'mined_squeeze', 'use_for_training': True,
                       'reason': '', '_ign': ign})
        mark(ep['setup'], ep['end'], k, ign, peak, True, 'mined_squeeze')
        next_ok = ep['end'] + 1

    # ── v1.2: apply split-artifact guard ─────────────────────
    f, n_split_guard = apply_split_guard(f)
    stats_extra = {'split_guard_invalidated': n_split_guard}

    # ── emit rows ─────────────────────────────────────────────
    start = int(np.searchsorted(dates, pd.to_datetime(PANEL_START), 'left'))
    out = f.iloc[start:].copy()
    k_arr = ev_idx[start:]
    stats = {'panel_rows': len(out), 'ineligible_rows': int((~out['eligible']).sum())}
    keep_elig = out['eligible'].values
    out, k_arr = out[keep_elig], k_arr[keep_elig]
    in_win = k_arr >= 0
    stats['eligible_rows'] = len(out)
    stats['event_rows'] = int(in_win.sum())
    # v1.2 sampling: keep event-window rows (100%), trigger rows (100%), rest 20%
    is_trigger = (out['ign_trigger'].fillna(False) | out['exh_trigger'].fillna(False)).values
    keep = in_win | is_trigger | (rng.random(len(out)) < NEG_KEEP_FRAC)
    # weight: event-window and trigger rows = 1.0; background rows = 1/NEG_KEEP_FRAC
    weight_override = np.where(in_win | is_trigger, 1.0, 1.0 / NEG_KEEP_FRAC)
    out, k_arr, in_win = out[keep], k_arr[keep], in_win[keep]
    weight_override = weight_override[keep]
    sel = np.where(keep_elig)[0][keep] + start   # absolute positions
    stats.update(stats_extra)

    def col(key, default=None):
        arr = np.array([e.get(key, default) for e in events] + [default], dtype=object)
        return arr[k_arr]    # k=-1 hits the sentinel

    out['symbol'] = sym
    out['event_id'] = col('event_id')
    out['event_source'] = col('source')
    out['event_status'] = col('status')
    out['event_use'] = col('use_for_training', False).astype(bool)
    out['event_ignition'] = pd.to_datetime(col('_ign', pd.NaT))
    out['lifecycle_phase'] = phase[sel]
    out['days_from_ignition'] = dfi[sel]
    out['sample_weight'] = weight_override
    out['date'] = out.index
    return out.reset_index(drop=True), events, stats


# ─────────────────────────────────────────────────────────────
# SPLITS / GROUPS / FOLDS
# ─────────────────────────────────────────────────────────────
def assign_split_groups_folds(df):
    d = df['date']
    val_s, test_s = pd.to_datetime(VAL_START), pd.to_datetime(TEST_START)
    emb = timedelta(days=EMBARGO_CAL_DAYS)
    split = np.where(d < val_s, 'train', np.where(d < test_s, 'val', 'test')).astype(object)
    split[(d >= val_s - emb) & (d < val_s)] = 'purged'
    split[(d >= test_s - emb) & (d < test_s)] = 'purged'
    df['split'] = split

    # group = quarter of the event IGNITION (whole event + wave stay together);
    # non-event rows use their own date's quarter
    gdate = df['event_ignition'].where(df['event_ignition'].notna(), d)
    df['group_id'] = gdate.dt.to_period('Q').astype(str)

    df['row_ok'] = (df[RAW_FEATURES].notna().all(axis=1) & df['squeezed_10d'].notna()
                    & df['squeezed_5d'].notna())
    # v1.2 additional row_ok flags for each model's target
    df['row_ok_m1'] = df['row_ok'] & df['ign_trigger'].fillna(False) & df['max_return_10d'].notna()
    df['row_ok_m2'] = (df['squeezed_10d'].notna() & df['exh_trigger'].fillna(False)
                       & df['min_return_5d'].notna() & df[RAW_FEATURES].notna().all(axis=1))
    df['row_ok_m3'] = (df[RAW_FEATURES].notna().all(axis=1)
                       & df[['abs_price_change_1d_norm', 'abs_price_change_5d_norm',
                              'volatility_10d_norm', 'atr_norm']].notna().all(axis=1)
                       & df['fwd_vol_10d'].notna())

    # greedy fold balancing by positive count (train+val, non-purged)
    df['cv_fold'] = -1
    m = df['split'].isin(['train', 'val']).values
    g = (df[m].groupby('group_id')['squeezed_10d']
         .agg(lambda s: int((s == 1).sum())).astype('int64').sort_values(ascending=False))
    loads, fold_of = [0] * N_FOLDS, {}
    for gid, pos in g.items():
        j = int(np.argmin(loads))
        fold_of[gid] = j
        loads[j] += int(pos) + 1
    df.loc[m, 'cv_fold'] = df.loc[m, 'group_id'].map(fold_of).astype(int)
    return df


# ─────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────
class Report:
    def __init__(self):
        self.lines = []

    def add(self, s=''):
        self.lines.append(str(s))

    def h(self, t):
        self.add('')
        self.add('=' * 78)
        self.add(t)
        self.add('=' * 78)

    def sub(self, t):
        self.add('')
        self.add(f'--- {t} ---')

    def table(self, df, **kw):
        self.add(df.to_string(**kw))

    def save(self, p):
        Path(p).write_text('\n'.join(self.lines), encoding='utf-8')


def wmean(y, w):
    y = pd.to_numeric(pd.Series(y).astype('float64'), errors='coerce').values
    w = np.asarray(w, dtype=float)
    m = ~np.isnan(y)
    return float((y[m] * w[m]).sum() / w[m].sum()) if m.any() and w[m].sum() > 0 else np.nan


def wauc(y, x, w):
    d = pd.DataFrame({'y': pd.Series(y).astype('float64').values,
                      'x': pd.Series(x).astype('float64').values,
                      'w': np.asarray(w, dtype=float)}).dropna()
    if d.empty:
        return np.nan
    d['pw'], d['nw'] = d['w'] * d['y'], d['w'] * (1 - d['y'])
    g = d.groupby('x')[['pw', 'nw']].sum().sort_index()
    P, N = g['pw'].sum(), g['nw'].sum()
    if P == 0 or N == 0:
        return np.nan
    cum_neg = g['nw'].cumsum() - g['nw']
    return float((g['pw'] * (cum_neg + 0.5 * g['nw'])).sum() / (P * N))


def build_report(df, ev, uni):
    rep = Report()
    pd.set_option('display.width', 250)
    pd.set_option('display.max_columns', 60)
    rep.h('SQUEEZERADAR v1.2 - DATASET METRICS')
    rep.add(f'Generated        : {datetime.now().isoformat(timespec="seconds")}')
    rep.add(f'yfinance / pandas: {getattr(yf, "__version__", "?")} / {pd.__version__}')
    rep.add(f'Target           : squeezed_Nd = forward N-day return >= {SQUEEZE_RETURN_THRESHOLD:.0%} '
            f'(NaN if window incomplete or spans a data gap)')
    rep.add(f'Eligibility rule : close >= {MIN_PRICE}, 20d avg $-volume >= {MIN_DOLLAR_VOL:,}, '
            f'>= {MIN_HISTORY_ROWS} rows of history (mirror this in the live scanner)')
    rep.add(f'Negatives        : non-event rows kept with p={NEG_KEEP_FRAC}, sample_weight={1 / NEG_KEEP_FRAC:.1f}')
    rep.add('Synthetic rows   : 0')

    # 1 universe
    rep.h('1. UNIVERSE & DOWNLOAD COVERAGE')
    rep.add(f'Symbols requested : {uni["requested"]}')
    rep.add(f'Downloaded OK     : {uni["ok"]}')
    rep.add(f'Failed / too short: {len(uni["errors"])} ({100 * len(uni["errors"]) / max(uni["requested"], 1):.1f}%)')
    cur_missing = sorted(s for s in curated_symbols() if s in uni['errors'] or s not in uni['symbols_ok'])
    rep.add(f'Curated symbols with NO data: {", ".join(cur_missing) if cur_missing else "none"}')
    rep.sub('Failed symbols (first 80) and reasons')
    for s, e in list(uni['errors'].items())[:80]:
        rep.add(f'  {s:8s} {e}')
    rep.add('')
    rep.add('SURVIVORSHIP: the universe is CURRENT listings. Delisted/bankrupt names are '
            'absent, which biases results toward companies that survived.')

    # 2 curated
    rep.h('2. CURATED EVENTS - RE-ANCHORED FROM OHLCV')
    c = ev[ev['source'] == 'curated'].copy()
    c['ign_offset_d'] = [(pd.Timestamp(a) - pd.Timestamp(b)).days if pd.notna(a) and b else np.nan
                         for a, b in zip(c['ignition_date'], c['hand_start'])]
    c['peak_offset_d'] = [(pd.Timestamp(a) - pd.Timestamp(b)).days if pd.notna(a) and b else np.nan
                          for a, b in zip(c['peak_date'], c['hand_peak'])]
    rep.table(c[['event_id', 'symbol', 'hand_failed', 'hand_start', 'ignition_date', 'ign_offset_d',
                 'hand_peak', 'peak_date', 'peak_offset_d', 'gain_pct', 'max_vol_ratio',
                 'hist_days', 'status', 'use_for_training']], index=False)
    rep.sub('Status counts')
    rep.table(c['status'].value_counts().to_frame('events'))
    rep.sub('Reasons for events NOT used for training')
    for r in c[~c['use_for_training']].itertuples():
        rep.add(f'  {r.event_id:12s} {r.status:22s} {r.reason}')

    # 3 mined
    rep.h('3. MINED EVENTS (data-defined, whole universe)')
    mi = ev[ev['source'] == 'mined'].copy()
    rep.add(f'Mined episodes: {len(mi)}   (ignition: vol ratio >= {IGN_VOLRATIO}x and '
            f'(1d >= {IGN_1D:.0%} or 5d >= {IGN_5D:.0%}); gain >= {CONFIRM_MIN_GAIN:.0%} '
            f'within {PEAK_LOOKAHEAD} trading days)')
    if len(mi):
        mi['year'] = pd.to_datetime(mi['ignition_date']).dt.year
        rep.table(mi.groupby('year').agg(episodes=('event_id', 'size'),
                                         median_gain_pct=('gain_pct', 'median'),
                                         p90_gain_pct=('gain_pct', lambda s: s.quantile(.9)),
                                         symbols=('symbol', 'nunique')).round(1))
        rep.sub('Top 15 mined episodes by gain (sanity check them on a chart)')
        rep.table(mi.sort_values('gain_pct', ascending=False).head(15)[
            ['event_id', 'ignition_date', 'peak_date', 'gain_pct', 'max_vol_ratio']], index=False)
    usable = ev[ev['use_for_training']]
    rep.add(f'\nUsable episodes total (curated + mined): {len(usable)}')

    # 4 panel
    rep.h('4. PANEL OVERVIEW')
    rep.add(f'Rows kept                : {len(df):,}')
    rep.add(f'Symbols with rows        : {df["symbol"].nunique()}')
    rep.add(f'Date range               : {df["date"].min().date()} -> {df["date"].max().date()}')
    rep.add(f'Event-window rows        : {int(df["event_id"].notna().sum()):,}')
    rep.add(f'Sum of sample_weight     : {df["sample_weight"].sum():,.0f}  '
            f'(~ natural eligible row count)')
    rep.add(f'rows with row_ok         : {int(df["row_ok"].sum()):,} '
            f'({100 * df["row_ok"].mean():.1f}%)')
    rep.sub('Natural base rate (weighted) of forward squeezes')
    ok = df[df['row_ok']]
    rows = []
    for n in (5, 10):
        rows.append({'target': f'squeezed_{n}d',
                     'weighted_base_rate_%': round(100 * wmean(ok[f'squeezed_{n}d'], ok['sample_weight']), 3),
                     'unweighted_rate_%': round(100 * float((ok[f'squeezed_{n}d'] == 1).mean()), 3),
                     'positives(raw rows)': int((ok[f'squeezed_{n}d'] == 1).sum())})
    rep.table(pd.DataFrame(rows), index=False)
    rep.sub('Selection effect: squeezed_10d rate inside vs outside event windows (weighted)')
    inw = ok['event_id'].notna()
    rep.add(f'  inside event windows : {100 * wmean(ok[inw]["squeezed_10d"], ok[inw]["sample_weight"]):.2f}%')
    rep.add(f'  outside event windows: {100 * wmean(ok[~inw]["squeezed_10d"], ok[~inw]["sample_weight"]):.2f}%')
    rep.sub('Weighted base rate by year (watch for regime shifts)')
    yr = ok.assign(year=ok['date'].dt.year).groupby('year').apply(
        lambda g: pd.Series({'rows': len(g),
                             'rate5d_%': 100 * wmean(g['squeezed_5d'], g['sample_weight']),
                             'rate10d_%': 100 * wmean(g['squeezed_10d'], g['sample_weight'])}))
    rep.table(yr.round(3))
    rep.sub('Phase labels (descriptive only; events with use_for_training)')
    pc = df['lifecycle_phase'].value_counts()
    rep.table(pc.to_frame('rows'))

    # 5 nulls
    rep.h('5. NULLS / FEATURE QUALITY')
    el = df[RAW_FEATURES]
    rep.table(pd.DataFrame({'nan_count': el.isna().sum(),
                            'nan_pct': (100 * el.isna().mean()).round(2)}))
    rep.sub('Descriptive statistics (raw)')
    rep.table(el.describe(percentiles=[.01, .05, .5, .95, .99]).T.round(4))
    rep.sub('Rows hitting normalization clip bounds')
    cl = []
    for raw in RAW_FEATURES:
        lo, hi = BOUNDS[raw]
        s = df[raw].dropna()
        cl.append({'feature': raw, 'below': int((s < lo).sum()), 'above': int((s > hi).sum()),
                   'pct': round(100 * ((s < lo) | (s > hi)).mean(), 2)})
    rep.table(pd.DataFrame(cl), index=False)
    rep.add('Trees do not need normalization: train on RAW features; keep _norm only for parity.')

    # 6 splits
    rep.h('6. SPLITS (chronological, embargo before each boundary)')
    rep.add(f'train < {VAL_START} | val {VAL_START}..{TEST_START} | test >= {TEST_START} | '
            f'purged = {EMBARGO_CAL_DAYS} days before each boundary')
    sp = []
    for s in ('train', 'val', 'test', 'purged'):
        g = df[df['split'] == s]
        gk = g[g['row_ok']]
        sp.append({'split': s, 'rows': len(g), 'row_ok': len(gk),
                   'symbols': g['symbol'].nunique(),
                   'usable_events': g[g['event_use']]['event_id'].nunique(),
                   'pos_5d': int((gk['squeezed_5d'] == 1).sum()),
                   'pos_10d': int((gk['squeezed_10d'] == 1).sum()),
                   'base5d_%': round(100 * wmean(gk['squeezed_5d'], gk['sample_weight']), 3) if len(gk) else np.nan,
                   'base10d_%': round(100 * wmean(gk['squeezed_10d'], gk['sample_weight']), 3) if len(gk) else np.nan,
                   'from': g['date'].min().date() if len(g) else None,
                   'to': g['date'].max().date() if len(g) else None})
    rep.table(pd.DataFrame(sp), index=False)

    # 7 folds
    rep.h('7. CV FOLDS (groups = calendar quarter; events grouped by ignition quarter)')
    fd = df[df['cv_fold'] >= 0]
    fr = []
    for k in range(N_FOLDS):
        g = fd[fd['cv_fold'] == k]
        gk = g[g['row_ok']]
        fr.append({'fold': k, 'groups': g['group_id'].nunique(), 'rows': len(g),
                   'pos_10d': int((gk['squeezed_10d'] == 1).sum()),
                   'base10d_%': round(100 * wmean(gk['squeezed_10d'], gk['sample_weight']), 3) if len(gk) else np.nan,
                   'usable_events': g[g['event_use']]['event_id'].nunique()})
    rep.table(pd.DataFrame(fr), index=False)
    rep.add('Quarter -> fold: ' + ', '.join(
        f'{q}:{int(fd[fd["group_id"] == q]["cv_fold"].iloc[0])}'
        for q in sorted(fd['group_id'].unique())))
    rep.add(f'Independent episodes represented: usable events {int(ev["use_for_training"].sum())}; '
            f'distinct groups with >=1 usable event: {df[df["event_use"]]["group_id"].nunique()}')

    # 8 AUC
    rep.h('8. UNIVARIATE AUC (weighted, row_ok rows) - stability across time')
    rep.add('0.5 = no signal. Large train-vs-test differences = regime shift or overfitting risk.')
    rows = []
    for raw in RAW_FEATURES:
        r = {'feature': raw}
        for s in ('train', 'val', 'test'):
            g = ok[ok['split'] == s]
            for n in (5, 10):
                r[f'{s}_{n}d'] = round(wauc(g[f'squeezed_{n}d'], g[raw], g['sample_weight']), 3) \
                    if len(g) else np.nan
        rows.append(r)
    rep.table(pd.DataFrame(rows), index=False)

    # 9 baseline
    rep.h('9. RULE BASELINE (the bar a model must beat)')
    rep.add('Rule: volume_ratio_30d >= 5 AND price_change_5d >= 20%')
    br = []
    for s in ('train', 'val', 'test'):
        g = ok[ok['split'] == s]
        if g.empty:
            continue
        rule = ((g['volume_ratio_30d'] >= 5) & (g['price_change_5d'] >= 0.20)).to_numpy(dtype=bool, na_value=False)
        w = g['sample_weight'].values
        for n in (5, 10):
            y = (g[f'squeezed_{n}d'] == 1).to_numpy(dtype=bool, na_value=False)
            tp, fp, fn = (w[rule & y].sum(), w[rule & ~y].sum(), w[~rule & y].sum())
            base = w[y].sum() / w.sum()
            prec = tp / (tp + fp) if tp + fp else np.nan
            br.append({'split': s, 'target': f'{n}d', 'rule_fires_rows': int(rule.sum()),
                       'precision_%': round(100 * prec, 2) if pd.notna(prec) else np.nan,
                       'recall_%': round(100 * tp / (tp + fn), 2) if tp + fn else np.nan,
                       'base_rate_%': round(100 * base, 3),
                       'lift': round(prec / base, 1) if pd.notna(prec) and base else np.nan})
    rep.table(pd.DataFrame(br), index=False)

    # ── v1.2 additions ───────────────────────────────────────
    rep.h('10. v1.2 TRIGGER ROWS')
    tr = []
    for s in ('train', 'val', 'test'):
        g = df[df['split'] == s]
        tr.append({'split': s,
                   'ign_trigger_rows': int(g['ign_trigger'].sum()),
                   'exh_trigger_rows': int(g['exh_trigger'].sum()),
                   'both_triggers': int((g['ign_trigger'] & g['exh_trigger']).sum()),
                   'total_rows': len(g)})
    rep.table(pd.DataFrame(tr), index=False)

    rep.sub('Per-year trigger counts')
    yr_trg = df.assign(year=df['date'].dt.year).groupby('year').agg(
        ign=('ign_trigger', 'sum'), exh=('exh_trigger', 'sum'), rows=('date', 'count')).reset_index()
    rep.table(yr_trg, index=False)

    rep.h('11. v1.2 MODEL TARGET BASE RATES')
    rep.add('y1 (Model 1: max_return_10d >= 30% | ign_trigger rows):')
    m1_rows = []
    for s in ('train', 'val', 'test'):
        g = df[(df['split'] == s) & df['row_ok_m1']]
        if len(g):
            y1 = (g['max_return_10d'] >= 0.30).astype(float)
            m1_rows.append({'split': s, 'rows': len(g),
                            'positives': int(y1.sum()),
                            'base_rate_%': round(100 * float(y1.mean()), 3)})
    if m1_rows:
        rep.table(pd.DataFrame(m1_rows), index=False)

    rep.add('y2 (Model 2: min_return_5d <= -20% | exh_trigger rows):')
    m2_rows = []
    for s in ('train', 'val', 'test'):
        g = df[(df['split'] == s) & df['row_ok_m2']]
        if len(g):
            y2 = (g['min_return_5d'] <= -0.20).astype(float)
            m2_rows.append({'split': s, 'rows': len(g),
                            'positives': int(y2.sum()),
                            'base_rate_%': round(100 * float(y2.mean()), 3)})
    if m2_rows:
        rep.table(pd.DataFrame(m2_rows), index=False)

    rep.add('y3 (Model 3: log(fwd_vol_10d) | all eligible rows):')
    m3_rows = []
    for s in ('train', 'val', 'test'):
        g = df[(df['split'] == s) & df['row_ok_m3']]
        if len(g):
            y3 = np.log(np.maximum(g['fwd_vol_10d'], 0.003))
            m3_rows.append({'split': s, 'rows': len(g),
                            'median_fwd_vol': round(float(g['fwd_vol_10d'].median()), 5),
                            'p10_fwd_vol': round(float(g['fwd_vol_10d'].quantile(0.1)), 5),
                            'p90_fwd_vol': round(float(g['fwd_vol_10d'].quantile(0.9)), 5)})
    if m3_rows:
        rep.table(pd.DataFrame(m3_rows), index=False)

    rep.h('12. v1.2 CLIPPING SHARE (norm columns)')
    clip_rows = []
    for raw, norm in RAW_TO_NORM.items():
        lo, hi = BOUNDS[raw]
        s = df[raw].dropna()
        clip_rows.append({'feature': raw,
                          'below_lo': int((s < lo).sum()),
                          'above_hi': int((s > hi).sum()),
                          'clip_%': round(100 * ((s < lo) | (s > hi)).mean(), 2)})
    rep.table(pd.DataFrame(clip_rows), index=False)

    rep.add(f'Rows between 0.10 and 0.50 in price: {int(((df["close"] >= 0.10) & (df["close"] < 0.50)).sum())}')
    split_guard_count = df[['fwd_vol_10d', 'min_return_5d']].isna().all(axis=1).sum()
    rep.add(f'Rows with forward targets invalidated by split guard: {split_guard_count}')

    # 10 warnings
    rep.h('10. READINESS & WARNINGS')
    warns = []
    ok_ev = ev[ev['use_for_training']]
    te = df[(df['split'] == 'test') & df['row_ok']]
    if len(uni['errors']) / max(uni['requested'], 1) > 0.3:
        warns.append('More than 30% of the universe failed to download; coverage is thin.')
    if len(ok_ev) < 100:
        warns.append(f'Only {len(ok_ev)} usable episodes; aim for 100+ for stable results.')
    if int((te['squeezed_10d'] == 1).sum()) < 100:
        warns.append('Fewer than 100 positive rows in test; test metrics will be noisy.')
    mn = [r['pos_10d'] for r in fr]
    if mn and min(mn) < 30:
        warns.append(f'A CV fold has only {min(mn)} positive rows.')
    nonuse = ev[(ev['source'] == 'curated') & (~ev['use_for_training'])]
    if len(nonuse):
        warns.append(f'{len(nonuse)} curated events excluded: ' + ', '.join(nonuse['event_id']))
    warns.append('Survivorship bias: universe = current listings.')
    warns.append('Mined labels are price-defined; they measure momentum continuation, '
                 'not proof of a short squeeze (no short-interest data here).')
    warns.append('volume_ratio_30d uses the PRIOR 30 days; the live pipeline must match.')
    warns.append('Embargo: during CV also drop training rows within ~21 days of a validation '
                 'block edge (adjacent quarters sit in different folds).')
    warns.append('The test period (2022+) should be scored ONCE, after model choices are frozen.')
    for w in warns:
        rep.add(f'  ! {w}')
    return rep


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print('\n' + '=' * 60)
    print('  SqueezeRadar - Training Data Builder v1.2')
    print('=' * 60)

    symbols = load_universe()
    ohlcv_map, errors = download_symbols(symbols)
    print(f'  {len(ohlcv_map)} symbols with data, {len(errors)} failed')

    # assign curated events to symbols
    cur_by_sym, no_data_events = {}, []
    for ev in CURATED:
        sym = pick_curated_symbol(ev, ohlcv_map)
        if sym is None:
            no_data_events.append({
                'event_id': ev['event_id'], 'source': 'curated', 'ticker': ev['ticker'],
                'symbol': '-', 'tier': ev['tier'], 'hand_failed': ev['hand_failed'],
                'hand_start': pd.to_datetime(ev['squeeze_start']).date(),
                'hand_peak': pd.to_datetime(ev['squeeze_peak']).date(),
                'ignition_date': pd.NaT, 'peak_date': pd.NaT, 'end_date': pd.NaT,
                'gain_pct': np.nan, 'max_vol_ratio': np.nan, 'hist_days': np.nan,
                'status': 'no_data', 'use_for_training': False,
                'reason': errors.get(ev['ticker'], 'no data in window')})
        else:
            cur_by_sym.setdefault(sym, []).append(ev)

    rng = np.random.default_rng(NEG_SEED)
    frames, all_events, tot = [], list(no_data_events), {'panel': 0, 'inelig': 0, 'elig': 0, 'event': 0}
    for j, (sym, ohlcv) in enumerate(sorted(ohlcv_map.items()), 1):
        try:
            rows, evs, st = process_symbol(sym, ohlcv, cur_by_sym.get(sym, []), rng)
        except Exception as e:  # noqa
            errors[sym] = f'processing failed: {e!r}'
            continue
        if len(rows):
            frames.append(rows)
        all_events += evs
        tot['panel'] += st['panel_rows']
        tot['inelig'] += st['ineligible_rows']
        tot['elig'] += st['eligible_rows']
        tot['event'] += st['event_rows']
        if j % 100 == 0:
            print(f'    processed {j}/{len(ohlcv_map)} symbols')

    if not frames:
        print('No rows produced. Check connectivity / yfinance version.')
        return None

    df = pd.concat(frames, ignore_index=True)
    ev = pd.DataFrame(all_events)
    df = assign_split_groups_folds(df)

    id_cols = ['date', 'symbol', 'split', 'group_id', 'cv_fold', 'row_ok',
               'row_ok_m1', 'row_ok_m2', 'row_ok_m3',
               'sample_weight', 'event_id', 'event_source', 'event_status', 'event_use',
               'event_ignition', 'lifecycle_phase', 'days_from_ignition',
               'close', 'volume', 'history_days', 'dollar_vol_20d']
    label_cols = ['return_5d', 'return_10d', 'max_return_5d', 'max_return_10d',
                  'squeezed_5d', 'squeezed_10d',
                  'min_return_5d', 'min_return_10d', 'fwd_vol_10d', 'baseline_vol_60d_lag10']
    trigger_cols = ['ign_trigger', 'exh_trigger']
    extra_norm_cols = V12_EXTRA_FEATURES
    all_cols = id_cols + RAW_FEATURES + NORM_FEATURES + extra_norm_cols + trigger_cols + label_cols
    # only keep columns that actually exist
    all_cols = [c for c in all_cols if c in df.columns]
    df = df[all_cols]
    # v1.2: save as v1_2_raw (do NOT overwrite v1_1)
    try:
        df.to_parquet(OUTPUT_DIR / 'v1_2_raw.parquet', index=False)
        print(f'  saved v1_2_raw.parquet')
    except Exception as e:  # noqa
        df.to_pickle(OUTPUT_DIR / 'v1_2_raw.pkl')
        print(f'  ! parquet write failed ({e!r}); saved v1_2_raw.pkl instead.')
    ev.drop(columns=[c for c in ev.columns if c.startswith('_')]).to_csv(
        OUTPUT_DIR / 'v1_2_events.csv', index=False)

    uni = {'requested': len(symbols), 'ok': len(ohlcv_map), 'errors': errors,
           'symbols_ok': set(ohlcv_map)}
    rep = build_report(df, ev, uni)
    rep.add('')
    rep.add(f'Panel accounting: {tot["panel"]:,} rows in panel period, {tot["inelig"]:,} dropped as '
            f'ineligible, {tot["elig"]:,} eligible, {tot["event"]:,} inside event windows, '
            f'{len(df):,} kept after negative subsampling.')
    rep.save(OUTPUT_DIR / 'dataset_metrics_v1_2.txt')

    print(f'\n  dataset saved to {OUTPUT_DIR}  ({len(df):,} rows)')
    print(f'  saved {OUTPUT_DIR / "v1_2_events.csv"}  ({len(ev)} events)')
    print(f'  saved {OUTPUT_DIR / "dataset_metrics_v1_2.txt"}')
    return df


if __name__ == '__main__':
    main()