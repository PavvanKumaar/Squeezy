# pipeline/scoring/model_router.py
#
# The logic that decides WHICH ML models run, and runs them.
#
#   Model 3 (volatility)  : every eligible stock, every cycle
#   Model 1 (ignition)    : only when ign_trigger fires
#   Model 2 (exhaustion)  : only when exh_trigger fires
#
# Inputs are the assembled `normalized` dict (names exactly as FEATURE_ORDER in
# feature_assembler.py). Note the assembler's price changes have no _norm suffix;
# the trained models call them price_change_*_norm (same values, clipped to +/-1).

import json
import math
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from pipeline.scoring import config as C


def _logit(p):
    p = min(max(float(p), 1e-6), 1 - 1e-6)
    return math.log(p / (1 - p))


def build_model_inputs(normalized: dict) -> dict:
    def v(k):
        x = normalized.get(k, 0.0)
        return 0.0 if x is None else float(x)
    p1, p5 = v('price_change_1d'), v('price_change_5d')
    return {
        'volume_ratio_30d_norm': v('volume_ratio_30d_norm'),
        'price_change_1d_norm': p1,
        'price_change_5d_norm': p5,
        'rsi_14_norm': v('rsi_14_norm'),
        'volatility_10d_norm': v('volatility_10d_norm'),
        'atr_norm': v('atr_norm'),
        'abs_price_change_1d_norm': abs(p1),
        'abs_price_change_5d_norm': abs(p5),
    }


def compute_triggers(x: dict) -> dict:
    ign = (x['volume_ratio_30d_norm'] >= C.IGN_VOLRATIO_NORM and
           (x['price_change_1d_norm'] >= C.IGN_1D or
            x['price_change_5d_norm'] >= C.IGN_5D))
    exh = (x['price_change_5d_norm'] >= C.EXH_5D or
           x['rsi_14_norm'] >= C.EXH_RSI_NORM)
    return {'ign_trigger': bool(ign), 'exh_trigger': bool(exh)}


def lift_band(lift: float) -> str:
    for cut, label in C.LIFT_BANDS:
        if lift < cut:
            return label
    return 'well above typical'


class ModelRouter:
    def __init__(self, models_dir=None):
        self.dir = Path(models_dir or C.MODELS_DIR)
        self._cache = {}

    # ── artifact loading ─────────────────────────────────────
    def _load(self, key):
        if key in self._cache:
            return self._cache[key]
        d = self.dir / C.MODEL_SUBDIRS[key]
        meta = json.loads((d / 'features.json').read_text())
        model = joblib.load(d / 'model.pkl')
        cal = None
        cp = d / 'calibrator.pkl'
        if cp.exists():
            try:
                cal = joblib.load(cp)
            except Exception:
                cal = None
        self._cache[key] = (model, cal, meta)
        return self._cache[key]

    def _frame(self, x, features):
        missing = [f for f in features if f not in x]
        if missing:
            raise KeyError(f'model inputs missing: {missing}')
        row = pd.DataFrame([[x[f] for f in features]], columns=features)
        if not np.isfinite(row.values.astype(float)).all():
            raise ValueError('non-finite model input')
        return row

    # ── eligibility ──────────────────────────────────────────
    def eligibility(self, context: dict):
        try:
            _, _, meta = self._load('volatility')
        except Exception:
            meta = {}
        min_price = meta.get('min_price', C.DEFAULT_MIN_PRICE)
        min_dv = meta.get('min_dollar_vol', C.DEFAULT_MIN_DOLLAR_VOL)
        price = (context or {}).get('price')
        dv = (context or {}).get('dollar_vol_20d')
        age = (context or {}).get('bar_age_days')
        if price is None:
            return False, 'no price data in stock_timeseries'
        if age is not None and age > C.MAX_BAR_AGE_DAYS:
            return False, (f'newest daily bar is {age} days old '
                           '(are the collectors running?)')
        if price < min_price:
            return False, f'price {price:.3f} below {min_price} (models not trained there)'
        if dv is None:
            return False, (f'fewer than {C.MIN_BARS_FOR_CONTEXT} daily bars: '
                           'cannot compute 20-day dollar volume')
        if dv < min_dv:
            return False, f'20d dollar volume {dv:,.0f} below {min_dv:,.0f}'
        return True, 'eligible'

    # ── classifiers (models 1 and 2) ─────────────────────────
    def _classify(self, key, x, prob_field):
        model, cal, meta = self._load(key)
        row = self._frame(x, meta['features'])
        p_raw = float(model.predict_proba(row)[:, 1][0])
        p_cal = None
        if cal is not None:
            try:
                p_cal = float(cal.predict_proba([[_logit(p_raw)]])[:, 1][0])
            except Exception:
                p_cal = None
        use_cal = C.USE_CALIBRATOR.get(key, False) and p_cal is not None
        p = p_cal if use_cal else p_raw
        base = float(meta.get('base_rate') or 0.0)
        lift = p / base if base > 0 else None
        return {
            'applicable': True,
            prob_field: round(p, 4),
            'p_raw': round(p_raw, 4),
            'p_calibrated': None if p_cal is None else round(p_cal, 4),
            'calibrated_used': bool(use_cal),
            'base_rate': round(base, 4),
            'lift': None if lift is None else round(lift, 2),
            'band': None if lift is None else lift_band(lift),
            'n_train': meta.get('n_train'),
            'model_type': meta.get('model_type'),
            'caveat': C.BASE_RATE_CAVEAT,
        }

    # ── volatility (model 3) ─────────────────────────────────
    def _volatility(self, x):
        models, _, meta = self._load('volatility')
        row = self._frame(x, meta['features'])
        preds = {a: float(models[a].predict(row)[0]) for a in (0.1, 0.5, 0.9)}
        p10, p50, p90 = np.exp(np.sort(list(preds.values())))   # fix quantile crossing
        move = float(p50) * math.sqrt(10)
        return {
            'applicable': True,
            'daily_vol_p10': round(float(p10), 4),
            'daily_vol_p50': round(float(p50), 4),
            'daily_vol_p90': round(float(p90), 4),
            'expected_10d_move_1sigma': round(move, 4),
            'sigmas_to_30pct': round(0.30 / move, 2) if move > 0 else None,
            'n_train': meta.get('n_train'),
        }

    # ── main entry ───────────────────────────────────────────
    def route(self, normalized: dict, context: dict) -> dict:
        x = build_model_inputs(normalized)
        trig = compute_triggers(x)
        ok, reason = self.eligibility(context)
        out = {'eligible': ok, 'eligibility_reason': reason, 'triggers': trig,
               'inputs': {k: round(v, 4) for k, v in x.items()}}

        na = lambda why: {'applicable': False, 'reason': why}   # noqa: E731
        if not ok:
            for k in ('ignition', 'exhaustion', 'volatility'):
                out[k] = na(reason)
            return out

        def run(fn, *a):
            try:
                return fn(*a)
            except Exception as e:      # never let one model break the cycle
                return {'applicable': True, 'error': f'{type(e).__name__}: {e}'}

        out['volatility'] = run(self._volatility, x)
        out['ignition'] = (run(self._classify, 'ignition', x, 'p_continue')
                           if trig['ign_trigger'] else
                           na('no ignition trigger (needs volume >= 3x and a sharp up-move)'))
        out['exhaustion'] = (run(self._classify, 'exhaustion', x, 'p_drop')
                             if trig['exh_trigger'] else
                             na('no exhaustion trigger (needs 5-day gain >= 25% or RSI >= 80)'))
        return out