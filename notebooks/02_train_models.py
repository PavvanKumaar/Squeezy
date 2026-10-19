# notebooks/02_train_models.py
#
# SqueezeRadar - preprocessing + LightGBM / XGBoost + Platt scaling
#
# Input : data/training/v1_1_raw.parquet (or .pkl fallback) from the v1.1 builder
# Output: models/<target>/  lgbm.txt, xgb.json, platt_*.joblib, features.json,
#                  oof_predictions.(parquet|pkl), training_report.txt
#
# Pipeline
#   1. Load, keep row_ok rows, enforce a LEAKAGE GUARD on the feature list
#   2. test split is untouched unless you pass --score-test
#   3. 5-fold grouped CV (cv_fold = time clusters) with a 21-day embargo:
#        train folds | early-stop fold | held-out fold, all purged against each other
#   4. LightGBM and XGBoost, weighted (sample_weight restores the natural base rate;
#      NO scale_pos_weight, so probabilities stay meaningful)
#   5. Out-of-fold predictions -> Platt scaling (cross-fitted for honest reporting,
#      then fitted on all OOF for deployment)
#   6. Report: weighted PR-AUC, ROC-AUC, Brier, log loss, lift at fixed alert rates,
#      precision vs the volume/momentum rule at the SAME alert rate, calibration
#      table, by-year stability, feature importance, label-shuffle sanity check
#   7. Final models fit on all train+val rows (rounds = median best iteration * 1.1)
#
# Run (CV only):            py -m notebooks.02_train_models --target squeezed_10d_vol
# Run (CV + one-time test): py -m notebooks.02_train_models --score-test
# Needs: pip install scikit-learn lightgbm xgboost joblib pandas numpy

import argparse
import json
import warnings
from datetime import datetime
from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (average_precision_score, brier_score_loss,
                             log_loss, roc_auc_score)

warnings.filterwarnings('ignore')

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / 'data' / 'training'
MODEL_DIR = ROOT / 'models'

# Features the live pipeline can compute from OHLCV. Add new columns here
# (and rebuild the dataset with them) - nothing else in this script changes.
FEATURES = [
    'volume_ratio_30d', 'price_change_1d', 'price_change_5d', 'rsi_14',
    'distance_from_52w_high', 'atr_pct', 'volatility_10d',
]
TARGET = 'squeezed_10d_vol'      # set from --target; see TARGET OPTIONS below
SQUEEZE_THRESHOLD = 0.30         # same 30% bar as the builder
VOL_K = 3.0                      # vol-adjusted target: return >= VOL_K x own N-day sigma
VOL_FLOOR = 0.02                 # floor on daily vol so calm stocks don't get tiny sigma
# TARGET OPTIONS:
#   squeezed_10d / squeezed_5d          raw: forward return >= 30%
#   squeezed_10d_vol / squeezed_5d_vol  vol-adjusted: >= 30% AND >= VOL_K x
#                                       max(volatility_10d, VOL_FLOOR) x sqrt(N)
#                                       (volatility_10d is trailing, so this is causal)
N_FOLDS = 5
EMBARGO_DAYS = 21
SEED = 42
ALERT_RATES = [0.01, 0.05]       # share of (weighted) rows flagged as alerts
CAL_BINS = [0, 0.005, 0.01, 0.02, 0.04, 0.08, 0.15, 0.30, 1.0]

# anything starting with these can never be a feature (future info / bookkeeping)
FORBIDDEN_PREFIXES = ('return_', 'max_return_', 'squeezed_', 'event_', 'lifecycle',
                      'days_from', 'cv_', 'group', 'split', 'sample_weight',
                      'row_ok', 'symbol', 'date')

LGB_PARAMS = dict(n_estimators=3000, learning_rate=0.03, num_leaves=15,
                  min_child_samples=200, subsample=0.8, subsample_freq=1,
                  colsample_bytree=0.8, reg_lambda=10.0, random_state=SEED,
                  n_jobs=-1, verbose=-1)
XGB_PARAMS = dict(n_estimators=3000, learning_rate=0.03, max_depth=4,
                  min_child_weight=5, subsample=0.8, colsample_bytree=0.8,
                  reg_lambda=10.0, tree_method='hist', eval_metric='logloss',
                  random_state=SEED, n_jobs=-1)


# ─────────────────────────────────────────────────────────────
# DATA + PREPROCESSING
# ─────────────────────────────────────────────────────────────
def load_data():
    pq, pk = DATA_DIR / 'v1_1_raw.parquet', DATA_DIR / 'v1_1_raw.pkl'
    if pq.exists():
        try:
            return pd.read_parquet(pq)
        except Exception as e:  # noqa
            print(f'  parquet read failed ({e!r}); trying pickle')
    if pk.exists():
        return pd.read_pickle(pk)
    raise SystemExit(f'No dataset found in {DATA_DIR}. Run the v1.1 builder first.')


def leakage_guard(features):
    bad = [f for f in features if f.startswith(FORBIDDEN_PREFIXES)]
    if bad:
        raise SystemExit(f'LEAKAGE GUARD: these cannot be features: {bad}')


def add_vol_targets(df):
    for n in (5, 10):
        sig = np.maximum(df['volatility_10d'].astype(float), VOL_FLOOR) * np.sqrt(n)
        r = df[f'return_{n}d'].astype(float)
        hit = (r >= SQUEEZE_THRESHOLD) & (r >= VOL_K * sig)
        df[f'squeezed_{n}d_vol'] = hit.astype('Int8').where(r.notna())
    return df


def preprocess(df):
    leakage_guard(FEATURES)
    df = add_vol_targets(df.copy())
    missing = [c for c in FEATURES + [TARGET, 'row_ok', 'split', 'cv_fold', 'sample_weight',
                                      'group_id', 'date'] if c not in df.columns]
    if missing:
        raise SystemExit(f'Missing columns in dataset: {missing}')
    n0 = len(df)
    df = df[df['row_ok']].copy()                        # complete features + valid labels
    df = df[np.isfinite(df[FEATURES].astype(float)).all(axis=1)]   # no inf
    df['y'] = (df[TARGET] == 1).astype(int)
    df['w'] = df['sample_weight'].astype(float)
    df['date'] = pd.to_datetime(df['date'])
    print(f'  rows: {n0:,} -> {len(df):,} after row_ok / finite filter')
    tv = df[df['split'].isin(['train', 'val'])].copy()
    tv = assign_quarter_folds(tv)
    test = df[df['split'] == 'test'].copy()
    return tv, test


# ─────────────────────────────────────────────────────────────
# FOLDS + EMBARGO
# ─────────────────────────────────────────────────────────────
def assign_quarter_folds(tv):
    """Folds from each row's OWN date quarter (events straddling quarters are
    handled by the embargo, not by regrouping), balanced by positive count.
    Overwrites the builder's cv_fold on purpose: the builder groups events by
    ignition quarter, whose setup rows spill into earlier quarters and make
    date-based embargoes delete most of the training data."""
    tv = tv.copy()
    tv['qgroup'] = tv['date'].dt.to_period('Q').astype(str)
    pos = tv.groupby('qgroup')['y'].sum().astype('int64').sort_values(ascending=False)
    loads, fold_of = [0] * N_FOLDS, {}
    for g, pv in pos.items():
        j = int(np.argmin(loads))
        fold_of[g] = j
        loads[j] += int(pv) + 1
    tv['cv_fold'] = tv['qgroup'].map(fold_of).astype(int)
    return tv


def purge(train, ref, embargo=EMBARGO_DAYS):
    """Drop train rows within `embargo` days of any calendar quarter present in ref."""
    if train.empty or ref.empty:
        return train
    keep = np.ones(len(train), dtype=bool)
    d = train['date']
    for per in ref['date'].dt.to_period('Q').unique():
        lo = per.start_time - pd.Timedelta(days=embargo)
        hi = per.end_time + pd.Timedelta(days=embargo)
        keep &= ~((d >= lo) & (d <= hi)).values
    return train[keep]


# ─────────────────────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────────────────────
def fit_lgbm(tr, es):
    m = lgb.LGBMClassifier(**LGB_PARAMS)
    m.fit(tr[FEATURES], tr['y'], sample_weight=tr['w'],
          eval_set=[(es[FEATURES], es['y'])], eval_sample_weight=[es['w']],
          eval_metric='binary_logloss',
          callbacks=[lgb.early_stopping(100, verbose=False)])
    return m, int(m.best_iteration_ or LGB_PARAMS['n_estimators'])


def fit_xgb(tr, es):
    m = xgb.XGBClassifier(early_stopping_rounds=100, **XGB_PARAMS)
    m.fit(tr[FEATURES], tr['y'], sample_weight=tr['w'],
          eval_set=[(es[FEATURES], es['y'])], sample_weight_eval_set=[es['w']],
          verbose=False)
    return m, int(m.best_iteration) + 1


def fit_final(name, data, n_rounds):
    if name == 'lgbm':
        p = dict(LGB_PARAMS, n_estimators=n_rounds)
        m = lgb.LGBMClassifier(**p)
    else:
        p = dict(XGB_PARAMS, n_estimators=n_rounds)
        m = xgb.XGBClassifier(**p)
    m.fit(data[FEATURES], data['y'], sample_weight=data['w'])
    return m


# ─────────────────────────────────────────────────────────────
# METRICS / CALIBRATION
# ─────────────────────────────────────────────────────────────
def logit(p):
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return np.log(p / (1 - p))


def fit_platt(p, y, w):
    lr = LogisticRegression(C=1e6, max_iter=1000)
    lr.fit(logit(p).reshape(-1, 1), y, sample_weight=w)
    return lr


def apply_platt(lr, p):
    return lr.predict_proba(logit(p).reshape(-1, 1))[:, 1]


def top_rate(y, p, w, q):
    """Weighted precision when flagging the top-q share of weighted mass."""
    o = np.argsort(-p, kind='stable')
    y, w = y[o], w[o]
    cw = np.cumsum(w)
    k = int(np.searchsorted(cw, q * cw[-1])) + 1
    return float((w[:k] * y[:k]).sum() / w[:k].sum())


def metrics(y, p, w):
    y, p, w = np.asarray(y), np.asarray(p), np.asarray(w)
    base = float((w * y).sum() / w.sum())
    r = {'n': len(y), 'pos': int(y.sum()), 'base_%': round(100 * base, 3)}
    if y.sum() == 0 or y.sum() == len(y):
        return r
    r['PR_AUC'] = round(average_precision_score(y, p, sample_weight=w), 4)
    r['ROC_AUC'] = round(roc_auc_score(y, p, sample_weight=w), 4)
    r['brier'] = round(brier_score_loss(y, p, sample_weight=w), 5)
    r['logloss'] = round(log_loss(y, np.clip(p, 1e-6, 1 - 1e-6), sample_weight=w), 5)
    r['mean_pred_%'] = round(100 * float((w * p).sum() / w.sum()), 3)
    for q in ALERT_RATES:
        r[f'prec@{int(q * 100)}%'] = round(100 * top_rate(y, p, w, q), 2)
        r[f'lift@{int(q * 100)}%'] = round(top_rate(y, p, w, q) / base, 1)
    return r


def rule_flags(df):
    return ((df['volume_ratio_30d'] >= 5) & (df['price_change_5d'] >= 0.20)).values


def rule_vs_model(df, p):
    """Precision of the rule vs the model flagging the SAME share of weighted rows."""
    y, w = df['y'].values, df['w'].values
    rule = rule_flags(df)
    q = w[rule].sum() / w.sum()
    rp = float((w[rule] * y[rule]).sum() / max(w[rule].sum(), 1e-12))
    mp = top_rate(y, p, w, q) if q > 0 else np.nan
    return {'alert_rate_%': round(100 * q, 3), 'rule_prec_%': round(100 * rp, 2),
            'model_prec_%': round(100 * mp, 2),
            'model/rule': round(mp / rp, 2) if rp > 0 else np.nan}


def rank_metrics(y, score, w):
    """Ranking-only metrics (score need not be a probability)."""
    y, score, w = np.asarray(y), np.asarray(score, dtype=float), np.asarray(w)
    base = float((w * y).sum() / w.sum())
    r = {'PR_AUC': round(average_precision_score(y, score, sample_weight=w), 4),
         'ROC_AUC': round(roc_auc_score(y, score, sample_weight=w), 4)}
    for q in ALERT_RATES:
        pr = top_rate(y, score, w, q)
        r[f'prec@{int(q * 100)}%'] = round(100 * pr, 2)
        r[f'lift@{int(q * 100)}%'] = round(pr / base, 1)
    return r


def baseline_table(df, p_model):
    rows = []
    for name, sc in (('model (lgbm)', p_model),
                     ('atr_pct only', df['atr_pct'].values),
                     ('volatility_10d only', df['volatility_10d'].values),
                     ('volume_ratio only', df['volume_ratio_30d'].values),
                     ('5d move only', df['price_change_5d'].values)):
        rows.append({'score': name, **rank_metrics(df['y'].values, sc, df['w'].values)})
    return pd.DataFrame(rows)


def bucket_table(df, score, col='atr_pct', n_buckets=5):
    """Skill WITHIN buckets of `col` (quintiles). ROC ~0.5 inside buckets means no
    signal beyond that column. `<col>_ROC` shows what that column alone still ranks
    inside the bucket, so compare it with model_ROC."""
    d = df[['y', 'w', col, 'volume_ratio_30d', 'price_change_5d']].copy()
    d['_s'] = np.asarray(score, dtype=float)
    d['bucket'] = pd.qcut(d[col], n_buckets, duplicates='drop')
    rows = []
    for iv, g in d.groupby('bucket', observed=True):
        r = {f'{col}_bucket': f'{iv.left:.3f}-{iv.right:.3f}', 'rows': len(g),
             'pos': int(g['y'].sum()),
             'base_%': round(100 * float((g['w'] * g['y']).sum() / g['w'].sum()), 3)}
        if g['y'].sum() >= 10 and g['y'].sum() < len(g):
            m = rank_metrics(g['y'].values, g['_s'].values, g['w'].values)
            r.update({'model_ROC': m['ROC_AUC'], 'model_PR': m['PR_AUC'],
                      'model_lift@5%': m['lift@5%'],
                      'volratio_ROC': round(roc_auc_score(g['y'], g['volume_ratio_30d'], sample_weight=g['w']), 3),
                      'move5d_ROC': round(roc_auc_score(g['y'], g['price_change_5d'], sample_weight=g['w']), 3),
                      f'{col}_ROC': round(roc_auc_score(g['y'], g[col], sample_weight=g['w']), 3)})
        rows.append(r)
    return pd.DataFrame(rows)


def bucket_verdict(tbl):
    v = tbl['model_ROC'].dropna()
    if v.empty:
        return 'not enough positives per bucket to judge'
    m = float(v.mean())
    base_col = [c for c in tbl.columns if c.endswith('_ROC') and c not in
                ('model_ROC', 'volratio_ROC', 'move5d_ROC')][0]
    b = float(tbl.loc[v.index, base_col].mean())
    # thresholds are rough rules of thumb, not statistical tests
    tag = ('no signal beyond the bucket variable' if m < 0.55 else
           'weak' if m < 0.60 else 'meaningful')
    beat = 'model beats' if m > b + 0.02 else ('model ~ties' if m > b - 0.02 else 'model LOSES to')
    return (f'mean within-bucket model ROC = {m:.3f} ({tag}); {base_col} alone = {b:.3f} -> '
            f'{beat} the bucket variable alone. Rule of thumb: <0.55 none, 0.55-0.60 weak, '
            f'>0.60 meaningful; each bucket has ~+-0.03 noise.')


def reliability(y, p, w):
    rows = []
    b = np.digitize(p, CAL_BINS[1:-1])
    tot = w.sum()
    for k in range(len(CAL_BINS) - 1):
        m = b == k
        if not m.any():
            continue
        rows.append({'pred_bin': f'{CAL_BINS[k]:.3f}-{CAL_BINS[k + 1]:.3f}',
                     'weighted_share_%': round(100 * w[m].sum() / tot, 2),
                     'mean_pred_%': round(100 * (w[m] * p[m]).sum() / w[m].sum(), 2),
                     'actual_%': round(100 * (w[m] * y[m]).sum() / w[m].sum(), 2),
                     'rows': int(m.sum())})
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────
class Report:
    def __init__(self):
        self.lines = []

    def add(self, s=''):
        self.lines.append(str(s))
        print(s)

    def h(self, t):
        self.add('')
        self.add('=' * 78)
        self.add(t)
        self.add('=' * 78)

    def table(self, df, **kw):
        self.add(df.to_string(**kw))

    def save(self, p):
        Path(p).write_text('\n'.join(self.lines), encoding='utf-8')


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main(score_test=False, target=None, drop=()):
    global TARGET, FEATURES
    if target:
        TARGET = target
    drop = [d for d in drop if d]
    unknown = [d for d in drop if d not in FEATURES]
    if unknown:
        raise SystemExit(f'--drop names not in FEATURES: {unknown}')
    FEATURES = [f for f in FEATURES if f not in drop]
    tag = ('_drop_' + '+'.join(drop)) if drop else ''
    out_dir = MODEL_DIR / (TARGET + tag)
    out_dir.mkdir(parents=True, exist_ok=True)
    rep = Report()
    rep.h('SQUEEZERADAR - MODEL TRAINING REPORT')
    rep.add(f'Generated : {datetime.now().isoformat(timespec="seconds")}')
    rep.add(f'Target    : {TARGET}   Features ({len(FEATURES)}): {", ".join(FEATURES)}')
    rep.add(f'Versions  : lightgbm {lgb.__version__}, xgboost {xgb.__version__}')

    tv, test = preprocess(load_data())
    rep.add(f'Train+val rows: {len(tv):,} (positives {int(tv["y"].sum()):,}, '
            f'weighted base rate {100 * (tv["w"] * tv["y"]).sum() / tv["w"].sum():.3f}%)')
    rep.add(f'Test rows     : {len(test):,} (held out; scored only with --score-test)')

    # ── grouped, embargoed CV ────────────────────────────────
    rep.h('1. CROSS-VALIDATION (5 time-cluster folds, 21-day embargo)')
    oof_parts, best = [], {'lgbm': [], 'xgb': []}
    for k in range(N_FOLDS):
        val = tv[tv['cv_fold'] == k]
        es_fold = (k + 1) % N_FOLDS
        es = purge(tv[tv['cv_fold'] == es_fold], val)
        tr = tv[~tv['cv_fold'].isin([k, es_fold])]
        tr = purge(purge(tr, val), es)
        if val['y'].sum() == 0 or tr['y'].sum() == 0 or es['y'].sum() == 0:
            rep.add(f'  fold {k}: skipped (no positives in a part)')
            continue
        part = val[['date', 'symbol', 'cv_fold', 'y', 'w', 'volume_ratio_30d',
                    'price_change_5d', 'atr_pct', 'volatility_10d']].copy()
        for name, fit in (('lgbm', fit_lgbm), ('xgb', fit_xgb)):
            m, bi = fit(tr, es)
            best[name].append(bi)
            part[f'p_{name}'] = m.predict_proba(val[FEATURES])[:, 1]
        part['p_avg'] = (part['p_lgbm'] + part['p_xgb']) / 2
        oof_parts.append(part)
        rep.add(f'  fold {k}: train {len(tr):,} | early-stop {len(es):,} | val {len(val):,} '
                f'| best iters lgbm {best["lgbm"][-1]}, xgb {best["xgb"][-1]}')
    oof = pd.concat(oof_parts)
    oof['year'] = oof['date'].dt.year

    # ── Platt: cross-fitted for honest numbers ───────────────
    for name in ('lgbm', 'xgb', 'avg'):
        oof[f'p_{name}_platt'] = np.nan
        for k in oof['cv_fold'].unique():
            fit_m, app_m = oof['cv_fold'] != k, oof['cv_fold'] == k
            lr = fit_platt(oof.loc[fit_m, f'p_{name}'].values,
                           oof.loc[fit_m, 'y'].values, oof.loc[fit_m, 'w'].values)
            oof.loc[app_m, f'p_{name}_platt'] = apply_platt(
                lr, oof.loc[app_m, f'p_{name}'].values)

    # ── pooled metrics ───────────────────────────────────────
    rep.h('2. OUT-OF-FOLD METRICS (pooled, weighted)')
    rows = []
    for name in ('lgbm', 'xgb', 'avg'):
        for suffix, label in (('', 'raw'), ('_platt', 'platt')):
            r = metrics(oof['y'], oof[f'p_{name}{suffix}'], oof['w'])
            rows.append({'model': f'{name}_{label}', **r})
    rep.table(pd.DataFrame(rows), index=False)
    rep.add('Read: PR_AUC vs base_% shows skill (random PR_AUC = base rate). Brier/logloss '
            'and mean_pred_% vs base_% show calibration. Platt rows are cross-fitted.')

    rep.h('3. PER-FOLD STABILITY (lgbm_platt) - lift should be > 1 in every fold')
    rows = []
    for k, g in oof.groupby('cv_fold'):
        rows.append({'fold': k, **metrics(g['y'], g['p_lgbm_platt'], g['w'])})
    rep.table(pd.DataFrame(rows), index=False)

    rep.h('4. BY YEAR (lgbm_platt) - regime / survivorship check')
    rows = []
    for yr, g in oof.groupby('year'):
        rows.append({'year': yr, **metrics(g['y'], g['p_lgbm_platt'], g['w'])})
    rep.table(pd.DataFrame(rows), index=False)

    rep.h('5. MODEL vs RULE BASELINE at the same alert rate')
    rows = []
    for name in ('lgbm', 'xgb', 'avg'):
        rows.append({'model': name, **rule_vs_model(oof.assign(), oof[f'p_{name}'].values)})
    rep.table(pd.DataFrame(rows), index=False)

    rep.add('')
    rep.add('Does the model add anything beyond single features? (OOF, weighted)')
    rep.table(baseline_table(oof, oof['p_lgbm'].values), index=False)

    rep.h('5b. VOLATILITY-CONTROLLED SKILL (OOF, lgbm): ROC within ATR quintiles')
    for col in ('atr_pct', 'volatility_10d'):
        bt = bucket_table(oof, oof['p_lgbm'].values, col)
        rep.add(f'Buckets of {col}:')
        rep.table(bt, index=False)
        rep.add(bucket_verdict(bt))
        rep.add('')
    if TARGET.endswith('_vol'):
        rep.add('NOTE: this target\'s threshold depends on volatility_10d, so the '
                'volatility_10d buckets are the clean test (threshold ~constant inside a bucket).')

    rep.h('6. CALIBRATION TABLE (lgbm): raw vs Platt, pooled OOF')
    y, w = oof['y'].values, oof['w'].values
    rep.add('raw:')
    rep.table(reliability(y, oof['p_lgbm'].values, w), index=False)
    rep.add('platt:')
    rep.table(reliability(y, oof['p_lgbm_platt'].values, w), index=False)

    # ── label-shuffle sanity check ───────────────────────────
    rep.h('7. SANITY: label-shuffle test (should be ~0.5 ROC_AUC)')
    k0 = int(oof['cv_fold'].min())
    val = tv[tv['cv_fold'] == k0]
    es = purge(tv[tv['cv_fold'] == (k0 + 1) % N_FOLDS], val)
    tr = purge(purge(tv[~tv['cv_fold'].isin([k0, (k0 + 1) % N_FOLDS])], val), es)
    aucs = []
    for rep_i in range(3):
        rng = np.random.default_rng(SEED + rep_i)
        trs, ess = tr.copy(), es.copy()
        trs['y'] = rng.permutation(trs['y'].values)
        ess['y'] = rng.permutation(ess['y'].values)
        m, _ = fit_lgbm(trs, ess)
        pv = m.predict_proba(val[FEATURES])[:, 1]
        aucs.append(roc_auc_score(val['y'], pv, sample_weight=val['w']))
    rep.add(f'  shuffled-label ROC_AUC on fold {k0} (3 shuffles): '
            f'mean {np.mean(aucs):.3f}, range {min(aucs):.3f}-{max(aucs):.3f}')
    rep.add('  Real models should sit clearly above this; if the shuffled AUC is '
            'well above 0.55, something leaks.')

    # ── final models ─────────────────────────────────────────
    rep.h('8. FINAL MODELS (all train+val rows) + deployment calibrators')
    finals = {}
    for name in ('lgbm', 'xgb'):
        n_rounds = max(int(np.median(best[name]) * 1.1), 50)
        finals[name] = fit_final(name, tv, n_rounds)
        rep.add(f'  {name}: {n_rounds} rounds')
    finals['lgbm'].booster_.save_model(str(out_dir / 'lgbm.txt'))
    finals['xgb'].save_model(str(out_dir / 'xgb.json'))
    platts = {n: fit_platt(oof[f'p_{n}'].values, oof['y'].values, oof['w'].values)
              for n in ('lgbm', 'xgb', 'avg')}
    for n, lr in platts.items():
        joblib.dump(lr, out_dir / f'platt_{n}.joblib')
    (out_dir / 'features.json').write_text(json.dumps(
        {'features': FEATURES, 'target': TARGET, 'embargo_days': EMBARGO_DAYS}, indent=2))
    try:
        oof.to_parquet(out_dir / 'oof_predictions.parquet', index=False)
    except Exception:  # noqa
        oof.to_pickle(out_dir / 'oof_predictions.pkl')

    rep.add('')
    rep.add('Feature importance (gain, normalized):')
    imp = pd.DataFrame({
        'lgbm': finals['lgbm'].booster_.feature_importance('gain'),
        'xgb': pd.Series(finals['xgb'].get_booster().get_score(importance_type='gain'))
                 .reindex(FEATURES).fillna(0).values}, index=FEATURES)
    imp = (imp / imp.sum()).round(3).sort_values('lgbm', ascending=False)
    rep.table(imp)

    # ── test (one shot) ──────────────────────────────────────
    rep.h('9. TEST SPLIT (2022+)')
    if not score_test:
        rep.add('Not scored. Freeze your settings, then rerun once with --score-test.')
    else:
        pl = finals['lgbm'].predict_proba(test[FEATURES])[:, 1]
        px = finals['xgb'].predict_proba(test[FEATURES])[:, 1]
        pa = (pl + px) / 2
        rows = []
        for name, p in (('lgbm', pl), ('xgb', px), ('avg', pa)):
            for lab, pp in (('raw', p), ('platt', apply_platt(platts[name], p))):
                rows.append({'model': f'{name}_{lab}', **metrics(test['y'], pp, test['w'])})
        rep.table(pd.DataFrame(rows), index=False)
        rep.add('')
        rep.add('Rule baseline vs model at the same alert rate (lgbm):')
        rep.table(pd.DataFrame([rule_vs_model(test, pl)]), index=False)
        rep.add('')
        rep.add('Model vs single-feature baselines (test):')
        rep.table(baseline_table(test, pl), index=False)
        rep.add('')
        rep.add('Volatility-controlled skill (test, lgbm):')
        for col in ('atr_pct', 'volatility_10d'):
            bt2 = bucket_table(test, pl, col)
            rep.add(f'Buckets of {col}:')
            rep.table(bt2, index=False)
            rep.add(bucket_verdict(bt2))
        rep.add('')
        rep.add('By year (lgbm_platt):')
        pp = apply_platt(platts['lgbm'], pl)
        ty = test['date'].dt.year.values
        rows = [{'year': yr, **metrics(test['y'].values[ty == yr], pp[ty == yr],
                                       test['w'].values[ty == yr])}
                for yr in sorted(set(ty))]
        rep.table(pd.DataFrame(rows), index=False)
        rep.add('Do not tune on these numbers; if you do, the test split is spent.')

    rep.save(out_dir / 'training_report.txt')
    print(f'\nSaved models and report to {out_dir}')


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--target', default='squeezed_10d_vol',
                    choices=['squeezed_10d', 'squeezed_5d',
                             'squeezed_10d_vol', 'squeezed_5d_vol'])
    ap.add_argument('--drop', default='',
                    help='comma-separated features to remove (ablation), e.g. atr_pct,volatility_10d')
    ap.add_argument('--score-test', action='store_true',
                    help='score the held-out 2022+ split (do this once)')
    a = ap.parse_args()
    main(score_test=a.score_test, target=a.target,
         drop=[x.strip() for x in a.drop.split(',')])