# notebooks/03_train_three_models.py
#
# SqueezeRadar - Three-Model Trainer v1.2
#
# Trains and evaluates all three models defined in squeezeradar_v1_2_spec.md:
#   Model 1 - Ignition Continuation  (p_continue)
#   Model 2 - Exhaustion / Fade Risk (p_drop)
#   Model 3 - Realized-Volatility Forecast (daily_vol_p10/p50/p90)
#
# Input : data/training/v1_2_raw.parquet (or .pkl fallback)
# Output: models/model1_ignition/
#         models/model2_exhaustion/
#         models/model3_volatility/
#         Each subdir: model.pkl, calibrator.pkl, features.json,
#                      oof_predictions.csv, report.txt
#
# Reuses helpers from 02_train_models.py (assign_quarter_folds, purge,
#   fit_platt, apply_platt, metrics, reliability).
#
# Run:   python notebooks/03_train_three_models.py
#        python notebooks/03_train_three_models.py --score-test
# Needs: pip install scikit-learn lightgbm joblib pandas numpy

import argparse
import json
import warnings
from datetime import datetime
from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (average_precision_score, brier_score_loss,
                              log_loss, roc_auc_score, mean_squared_error)

warnings.filterwarnings('ignore')

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / 'data' / 'training'
MODEL_DIR = ROOT / 'models'

# ─── Eligibility / feature constants (must match builder) ────
MIN_PRICE = 0.10
MIN_DOLLAR_VOL = 50_000

# ─── Leakage guard ───────────────────────────────────────────
FORBIDDEN_PREFIXES = ('return_', 'max_return_', 'min_return_', 'fwd_',
                      'squeezed_', 'event_', 'lifecycle', 'days_from',
                      'cv_', 'group', 'split', 'sample_weight',
                      'row_ok', 'symbol', 'date', 'baseline_vol')

# ─── Model feature lists (norm space, as per spec) ───────────
M1_FEATURES = [
    'volume_ratio_30d_norm', 'price_change_1d_norm',
    'price_change_5d_norm', 'rsi_14_norm',
]
M2_FEATURES = M1_FEATURES  # identical feature set
M3_FEATURES = [
    'volume_ratio_30d_norm', 'price_change_1d_norm',
    'price_change_5d_norm', 'rsi_14_norm',
    'volatility_10d_norm', 'atr_norm',
    'abs_price_change_1d_norm', 'abs_price_change_5d_norm',
]

# ─── CV / embargo ────────────────────────────────────────────
N_FOLDS = 5
EMBARGO_DAYS = 21
SEED = 42
CAL_BINS = [0, 0.005, 0.01, 0.02, 0.04, 0.08, 0.15, 0.30, 1.0]

# ─── LightGBM hyperparams ────────────────────────────────────
# Models 1 & 2: shallow classifier
LGB_CLS_PARAMS = dict(
    n_estimators=2000, learning_rate=0.03,
    num_leaves=6, min_child_samples=75,
    subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
    reg_lambda=10.0, random_state=SEED, n_jobs=-1, verbose=-1,
)
# Model 3: quantile regression
LGB_Q_PARAMS = dict(
    n_estimators=2000, learning_rate=0.03,
    num_leaves=15, min_child_samples=200,
    subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
    reg_lambda=5.0, random_state=SEED, n_jobs=-1, verbose=-1,
)


# ─────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────

def load_data():
    pq = DATA_DIR / 'v1_2_raw.parquet'
    pk = DATA_DIR / 'v1_2_raw.pkl'
    if pq.exists():
        try:
            return pd.read_parquet(pq)
        except Exception as e:
            print(f'  parquet read failed ({e!r}); trying pickle')
    if pk.exists():
        return pd.read_pickle(pk)
    raise SystemExit(
        f'No v1_2 dataset found in {DATA_DIR}.\n'
        'Run notebooks/01_build_training_data_v11.py first.'
    )


def leakage_guard(features):
    bad = [f for f in features if f.startswith(FORBIDDEN_PREFIXES)]
    if bad:
        raise SystemExit(f'LEAKAGE GUARD: these cannot be features: {bad}')


# ─────────────────────────────────────────────────────────────
# CV HELPERS (reused from 02_train_models.py logic)
# ─────────────────────────────────────────────────────────────

def assign_quarter_folds(tv, target_col='y'):
    """Folds from each row's OWN date quarter, balanced by positive count."""
    tv = tv.copy()
    tv['qgroup'] = tv['date'].dt.to_period('Q').astype(str)
    pos = tv.groupby('qgroup')[target_col].sum().astype('int64').sort_values(ascending=False)
    loads, fold_of = [0] * N_FOLDS, {}
    for g, pv in pos.items():
        j = int(np.argmin(loads))
        fold_of[g] = j
        loads[j] += int(pv) + 1
    tv['cv_fold'] = tv['qgroup'].map(fold_of).astype(int)
    return tv


def assign_quarter_folds_regression(tv):
    """Folds by quarter for regression — no positive-balancing needed."""
    tv = tv.copy()
    tv['qgroup'] = tv['date'].dt.to_period('Q').astype(str)
    quarters = sorted(tv['qgroup'].unique())
    fold_of = {q: i % N_FOLDS for i, q in enumerate(quarters)}
    tv['cv_fold'] = tv['qgroup'].map(fold_of).astype(int)
    return tv


def purge(train, ref, embargo=EMBARGO_DAYS):
    """Drop train rows within `embargo` days of any calendar quarter in ref."""
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
# PLATT SCALING HELPERS
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


# ─────────────────────────────────────────────────────────────
# METRICS
# ─────────────────────────────────────────────────────────────

def metrics_cls(y, p, w):
    y, p, w = np.asarray(y, float), np.asarray(p, float), np.asarray(w, float)
    base = float((w * y).sum() / w.sum())
    r = {'n': len(y), 'pos': int(y.sum()), 'base_%': round(100 * base, 3)}
    if y.sum() == 0 or y.sum() == len(y):
        return r
    r['PR_AUC'] = round(average_precision_score(y, p, sample_weight=w), 4)
    r['ROC_AUC'] = round(roc_auc_score(y, p, sample_weight=w), 4)
    r['brier'] = round(brier_score_loss(y, p, sample_weight=w), 5)
    r['logloss'] = round(log_loss(y, np.clip(p, 1e-6, 1 - 1e-6), sample_weight=w), 5)
    r['mean_pred_%'] = round(100 * float((w * p).sum() / w.sum()), 3)
    return r


def top_rate(y, p, w, q):
    o = np.argsort(-p, kind='stable')
    y, w = y[o], w[o]
    cw = np.cumsum(w)
    k = int(np.searchsorted(cw, q * cw[-1])) + 1
    return float((w[:k] * y[:k]).sum() / w[:k].sum())


def lift_at_top20(y, p, w):
    base = float((w * y).sum() / w.sum())
    if base <= 0:
        return np.nan
    return round(top_rate(y, p, w, 0.20) / base, 2)


def metrics_reg(y_true, y_pred, w):
    """RMSE and R² on y3 (log scale)."""
    y_true = np.asarray(y_true, float)
    y_pred = np.asarray(y_pred, float)
    w = np.asarray(w, float)
    m = np.isfinite(y_true) & np.isfinite(y_pred)
    y_true, y_pred, w = y_true[m], y_pred[m], w[m]
    if len(y_true) == 0:
        return {'rmse': np.nan, 'r2': np.nan}
    wmse = np.sum(w * (y_true - y_pred) ** 2) / w.sum()
    wmean = np.sum(w * y_true) / w.sum()
    wvar = np.sum(w * (y_true - wmean) ** 2) / w.sum()
    r2 = 1 - wmse / wvar if wvar > 0 else np.nan
    return {'n': int(m.sum()), 'rmse': round(float(wmse ** 0.5), 5),
            'r2': round(float(r2), 4)}


def reliability(y, p, w):
    rows = []
    b = np.digitize(p, CAL_BINS[1:-1])
    tot = w.sum()
    for k in range(len(CAL_BINS) - 1):
        m = b == k
        if not m.any():
            continue
        rows.append({'pred_bin': f'{CAL_BINS[k]:.3f}-{CAL_BINS[k+1]:.3f}',
                     'weighted_share_%': round(100 * w[m].sum() / tot, 2),
                     'mean_pred_%': round(100 * (w[m] * p[m]).sum() / w[m].sum(), 2),
                     'actual_%': round(100 * (w[m] * y[m]).sum() / w[m].sum(), 2),
                     'rows': int(m.sum())})
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────
# REPORT CLASS
# ─────────────────────────────────────────────────────────────

class Report:
    def __init__(self):
        self.lines = []

    def add(self, s=''):
        self.lines.append(str(s))
        print(s)

    def h(self, t):
        self.add()
        self.add('=' * 78)
        self.add(t)
        self.add('=' * 78)

    def sub(self, t):
        self.add()
        self.add(f'--- {t} ---')

    def table(self, df, **kw):
        self.add(df.to_string(**kw))

    def save(self, p):
        Path(p).write_text('\n'.join(self.lines), encoding='utf-8')


# ─────────────────────────────────────────────────────────────
# MODEL FITTING
# ─────────────────────────────────────────────────────────────

def fit_lgbm_cls(tr, es, features, num_leaves=6, min_child_samples=75):
    params = dict(LGB_CLS_PARAMS, num_leaves=num_leaves,
                  min_child_samples=min_child_samples)
    m = lgb.LGBMClassifier(**params)
    m.fit(tr[features], tr['y'], sample_weight=tr['w'],
          eval_set=[(es[features], es['y'])],
          eval_sample_weight=[es['w']],
          eval_metric='binary_logloss',
          callbacks=[lgb.early_stopping(100, verbose=False)])
    return m, int(m.best_iteration_ or params['n_estimators'])


def fit_logistic(tr, features, C=0.1):
    """Standardized logistic regression, L2."""
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import Pipeline
    pipe = Pipeline([
        ('scaler', StandardScaler()),
        ('lr', LogisticRegression(C=C, penalty='l2', max_iter=1000,
                                  random_state=SEED, solver='lbfgs')),
    ])
    pipe.fit(tr[features], tr['y'], lr__sample_weight=tr['w'])
    return pipe


def fit_lgbm_quantile(tr, es, features, alpha):
    """LightGBM quantile regression for a single alpha."""
    params = dict(LGB_Q_PARAMS, objective='quantile', alpha=alpha,
                  metric='quantile')
    m = lgb.LGBMRegressor(**params)
    m.fit(tr[features], tr['y3'],
          sample_weight=tr['w'],
          eval_set=[(es[features], es['y3'])],
          eval_sample_weight=[es['w']],
          callbacks=[lgb.early_stopping(100, verbose=False)])
    return m, int(m.best_iteration_ or params['n_estimators'])


def fit_ols(tr, features):
    """OLS via numpy for regression benchmark."""
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import Pipeline
    pipe = Pipeline([
        ('scaler', StandardScaler()),
        ('ols', LinearRegression()),
    ])
    pipe.fit(tr[features], tr['y3'], ols__sample_weight=tr['w'])
    return pipe


# ─────────────────────────────────────────────────────────────
# VIABILITY CHECK
# ─────────────────────────────────────────────────────────────

def viability_check(n_trigger_rows, model_name, rep):
    """If fewer than ~300 trigger rows, recommend lookup table instead."""
    rep.add(f'  {model_name}: {n_trigger_rows} trigger rows in train+val.')
    if n_trigger_rows < 300:
        rep.add(f'  WARNING: fewer than 300 rows — consider a bucketed base-rate '
                f'lookup table instead of a fitted model.')
    return n_trigger_rows >= 300


# ─────────────────────────────────────────────────────────────
# HYPERPARAMETER SEARCH (C grid for logistic, num_leaves for LightGBM)
# ─────────────────────────────────────────────────────────────

def cv_select_model(tv, features, rep, model_name):
    """
    Run grouped CV to pick between:
      - Logistic regression (L2) with C in {0.01, 0.03, 0.1, 0.3, 1}
      - Shallow LightGBM with num_leaves in {4, 5, 6, 7}, min_child_samples in {50, 75, 100}
    Returns the best model type and hyperparams, plus OOF predictions dict.
    """
    rep.sub(f'{model_name}: grouped CV model selection')
    best_prauc = -1
    best_name = None
    best_params = {}
    oof_records = []

    # ── Try logistic regression ──────────────────────────────
    for C in [0.01, 0.03, 0.1, 0.3, 1.0]:
        fold_paucs = []
        oof_parts = []
        for k in range(N_FOLDS):
            val = tv[tv['cv_fold'] == k]
            es_fold = (k + 1) % N_FOLDS
            es = purge(tv[tv['cv_fold'] == es_fold], val)
            tr = tv[~tv['cv_fold'].isin([k, es_fold])]
            tr = purge(purge(tr, val), es)
            if val['y'].sum() == 0 or tr['y'].sum() == 0:
                continue
            try:
                m = fit_logistic(tr, features, C=C)
                pv = m.predict_proba(val[features])[:, 1]
                pauc = average_precision_score(val['y'], pv, sample_weight=val['w'])
                fold_paucs.append(pauc)
                part = val[['date', 'cv_fold', 'y', 'w'] + list(features)].copy()
                part['p_model'] = pv
                oof_parts.append(part)
            except Exception as e:
                rep.add(f'    logistic C={C} fold {k} failed: {e}')
        if fold_paucs:
            mean_pauc = float(np.mean(fold_paucs))
            rep.add(f'  Logistic C={C}: mean CV PR-AUC={mean_pauc:.4f} (folds: '
                    f'{", ".join(f"{x:.3f}" for x in fold_paucs)})')
            if mean_pauc > best_prauc:
                best_prauc = mean_pauc
                best_name = 'logistic'
                best_params = {'C': C}
                oof_records = oof_parts

    # ── Try shallow LightGBM ─────────────────────────────────
    for num_leaves in [4, 5, 6, 7]:
        for min_cs in [50, 75, 100]:
            fold_paucs = []
            oof_parts = []
            best_iters = []
            for k in range(N_FOLDS):
                val = tv[tv['cv_fold'] == k]
                es_fold = (k + 1) % N_FOLDS
                es = purge(tv[tv['cv_fold'] == es_fold], val)
                tr = tv[~tv['cv_fold'].isin([k, es_fold])]
                tr = purge(purge(tr, val), es)
                if val['y'].sum() == 0 or tr['y'].sum() == 0 or es['y'].sum() == 0:
                    continue
                try:
                    m, bi = fit_lgbm_cls(tr, es, features, num_leaves, min_cs)
                    pv = m.predict_proba(val[features])[:, 1]
                    pauc = average_precision_score(val['y'], pv, sample_weight=val['w'])
                    fold_paucs.append(pauc)
                    best_iters.append(bi)
                    part = val[['date', 'cv_fold', 'y', 'w'] + list(features)].copy()
                    part['p_model'] = pv
                    oof_parts.append(part)
                except Exception as e:
                    rep.add(f'    lgbm leaves={num_leaves} mcs={min_cs} fold {k} failed: {e}')
            if fold_paucs:
                mean_pauc = float(np.mean(fold_paucs))
                rep.add(f'  LightGBM leaves={num_leaves} min_cs={min_cs}: '
                        f'mean CV PR-AUC={mean_pauc:.4f}')
                # prefer simpler model on ties (logistic wins at <= 0.001 diff)
                if mean_pauc > best_prauc + 0.001:
                    best_prauc = mean_pauc
                    best_name = 'lgbm'
                    best_params = {'num_leaves': num_leaves,
                                   'min_child_samples': min_cs,
                                   'best_iters': best_iters}
                    oof_records = oof_parts

    rep.add(f'  >> Selected: {best_name} params={best_params} PR-AUC={best_prauc:.4f}')
    return best_name, best_params, oof_records


# ─────────────────────────────────────────────────────────────
# TRAIN CLASSIFICATION MODEL (Models 1 & 2)
# ─────────────────────────────────────────────────────────────

def train_classification_model(df, features, target_col, trigger_col,
                                row_ok_col, model_name, out_dir, rep,
                                score_test=False, baseline_rule_fn=None):
    """
    Full training pipeline for a binary classification model.
    Returns trained model + calibrator or None if not viable.
    """
    leakage_guard(features)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── prepare data ─────────────────────────────────────────
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    # filter: trigger rows only, row_ok, valid target
    mask = (df[row_ok_col].fillna(False).astype(bool) &
            df[trigger_col].fillna(False).astype(bool))
    df = df[mask].copy()
    # compute binary target
    if target_col == 'y1':
        df['y'] = (df['max_return_10d'] >= 0.30).astype(int)
        df['y'] = df['y'].where(df['max_return_10d'].notna())
    elif target_col == 'y2':
        df['y'] = (df['min_return_5d'] <= -0.20).astype(int)
        df['y'] = df['y'].where(df['min_return_5d'].notna())
    df = df[df['y'].notna()].copy()
    df['y'] = df['y'].astype(int)
    df['w'] = df['sample_weight'].astype(float)

    # only finite features
    df = df[np.isfinite(df[features].astype(float)).all(axis=1)]

    tv = df[df['split'].isin(['train', 'val'])].copy()
    test = df[df['split'] == 'test'].copy()

    base_rate = float((tv['w'] * tv['y']).sum() / tv['w'].sum()) if len(tv) > 0 else 0.0
    rep.add(f'\n{model_name}:')
    rep.add(f'  Train+val rows: {len(tv):,}  positives: {int(tv["y"].sum()):,}  '
            f'weighted base rate: {100 * base_rate:.3f}%')
    rep.add(f'  Test rows: {len(test):,}')

    # viability check
    viable = viability_check(len(tv), model_name, rep)
    if not viable:
        rep.add(f'  {model_name}: skipping model fit — too few rows. '
                'Returning base-rate lookup.')
        # save base rate as a minimal artifact
        info = {'model_type': 'base_rate_fallback', 'base_rate': base_rate,
                'n_train': len(tv), 'features': features, 'target': target_col,
                'embargo_days': EMBARGO_DAYS, 'min_price': MIN_PRICE,
                'min_dollar_vol': MIN_DOLLAR_VOL}
        (out_dir / 'features.json').write_text(json.dumps(info, indent=2))
        report_path = out_dir / 'report.txt'
        rep.save(report_path)
        return None, None

    # ── assign folds ─────────────────────────────────────────
    tv = assign_quarter_folds(tv, target_col='y')

    # ── CV model selection ───────────────────────────────────
    best_name, best_params, oof_parts = cv_select_model(tv, features, rep, model_name)

    # ── OOF predictions for calibration / reporting ──────────
    oof = pd.concat(oof_parts).reset_index(drop=True) if oof_parts else pd.DataFrame()

    # ── Platt scaling cross-fitted ───────────────────────────
    if len(oof) > 0:
        oof['p_platt'] = np.nan
        for k in oof['cv_fold'].unique():
            fit_m = oof['cv_fold'] != k
            app_m = oof['cv_fold'] == k
            if fit_m.sum() < 10 or app_m.sum() < 2:
                continue
            lr = fit_platt(oof.loc[fit_m, 'p_model'].values,
                           oof.loc[fit_m, 'y'].values,
                           oof.loc[fit_m, 'w'].values)
            oof.loc[app_m, 'p_platt'] = apply_platt(
                lr, oof.loc[app_m, 'p_model'].values)

        rep.h(f'{model_name}: OOF Metrics')
        y_oof, w_oof = oof['y'].values, oof['w'].values
        p_raw = oof['p_model'].values
        p_cal = np.where(oof['p_platt'].notna(), oof['p_platt'], p_raw)
        for label, p in (('raw', p_raw), ('platt', p_cal)):
            r = metrics_cls(y_oof, p, w_oof)
            r['lift@top20'] = lift_at_top20(y_oof, p, w_oof)
            rep.add(f'  {label}: {r}')
        rep.sub('Calibration table (platt)')
        rep.table(reliability(y_oof, p_cal, w_oof), index=False)

        # per-fold stability
        rep.sub('Per-fold stability (platt)')
        fold_rows = []
        for k, g in oof.groupby('cv_fold'):
            r = metrics_cls(g['y'].values,
                            g['p_platt'].fillna(g['p_model']).values,
                            g['w'].values)
            r['fold'] = k
            fold_rows.append(r)
        if fold_rows:
            rep.table(pd.DataFrame(fold_rows), index=False)

        # per-year stability
        rep.sub('Per-year stability (platt)')
        oof_yr = oof.copy()
        oof_yr['year'] = pd.to_datetime(oof_yr['date']).dt.year
        yr_rows = []
        for yr, g in oof_yr.groupby('year'):
            r = metrics_cls(g['y'].values,
                            g['p_platt'].fillna(g['p_model']).values,
                            g['w'].values)
            r['year'] = yr
            yr_rows.append(r)
        if yr_rows:
            rep.table(pd.DataFrame(yr_rows), index=False)

        # label-shuffle sanity check
        rep.sub('Sanity: label-shuffle ROC (should be ~0.5)')
        k0 = int(oof['cv_fold'].min())
        val_s = tv[tv['cv_fold'] == k0]
        es_s = purge(tv[tv['cv_fold'] == (k0 + 1) % N_FOLDS], val_s)
        tr_s = purge(purge(tv[~tv['cv_fold'].isin([k0, (k0 + 1) % N_FOLDS])], val_s), es_s)
        aucs_shuf = []
        for rep_i in range(3):
            rng = np.random.default_rng(SEED + rep_i)
            trs = tr_s.copy()
            trs['y'] = rng.permutation(trs['y'].values)
            ess = es_s.copy()
            ess['y'] = rng.permutation(ess['y'].values)
            if trs['y'].sum() == 0 or ess['y'].sum() == 0:
                continue
            try:
                if best_name == 'lgbm':
                    m_s, _ = fit_lgbm_cls(trs, ess, features,
                                          best_params.get('num_leaves', 6),
                                          best_params.get('min_child_samples', 75))
                    pv_s = m_s.predict_proba(val_s[features])[:, 1]
                else:
                    m_s = fit_logistic(trs, features, C=best_params.get('C', 0.1))
                    pv_s = m_s.predict_proba(val_s[features])[:, 1]
                if val_s['y'].sum() > 0:
                    aucs_shuf.append(roc_auc_score(val_s['y'], pv_s, sample_weight=val_s['w']))
            except Exception:
                pass
        if aucs_shuf:
            rep.add(f'  Shuffled ROC: mean={np.mean(aucs_shuf):.3f} '
                    f'range={min(aucs_shuf):.3f}-{max(aucs_shuf):.3f}')

    # ── baseline comparison ──────────────────────────────────
    rep.sub(f'{model_name}: Baseline comparison (OOF, weighted)')
    if len(oof) > 0 and baseline_rule_fn is not None:
        rule = baseline_rule_fn(tv)
        w_tv = tv['w'].values
        y_tv = tv['y'].values
        base_tv = float((w_tv * y_tv).sum() / w_tv.sum())
        if rule.sum() > 0:
            rp = float((w_tv[rule] * y_tv[rule]).sum() / w_tv[rule].sum())
            rep.add(f'  Rule baseline: {100 * rp:.2f}% precision '
                    f'(base rate: {100 * base_tv:.2f}%, '
                    f'lift: {rp / base_tv:.2f}x)')
        else:
            rep.add('  Rule baseline: fires on 0 rows in train+val.')

    # ── fit final model on all train+val ─────────────────────
    rep.sub(f'{model_name}: Final model fit (all train+val)')
    if best_name == 'lgbm':
        median_iters = int(np.median(best_params.get('best_iters', [200])) * 1.1)
        median_iters = max(median_iters, 50)
        params_final = dict(LGB_CLS_PARAMS,
                            num_leaves=best_params.get('num_leaves', 6),
                            min_child_samples=best_params.get('min_child_samples', 75),
                            n_estimators=median_iters)
        final_model = lgb.LGBMClassifier(**params_final)
        final_model.fit(tv[features], tv['y'], sample_weight=tv['w'])
        model_type = 'lgbm'
    else:
        final_model = fit_logistic(tv, features, C=best_params.get('C', 0.1))
        model_type = 'logistic'
    rep.add(f'  Final model type: {model_type}')

    # fit final calibrator on all OOF scores
    final_calibrator = None
    if len(oof) > 0:
        if model_type == 'lgbm':
            p_all_oof = final_model.predict_proba(oof[features])[:, 1] if len(oof) > 0 else np.array([])
        else:
            p_all_oof = final_model.predict_proba(oof[features])[:, 1]
        final_calibrator = fit_platt(p_all_oof, oof['y'].values, oof['w'].values)

    # ── save artifacts ───────────────────────────────────────
    joblib.dump(final_model, out_dir / 'model.pkl')
    if final_calibrator is not None:
        joblib.dump(final_calibrator, out_dir / 'calibrator.pkl')
    (out_dir / 'features.json').write_text(json.dumps({
        'model_type': model_type,
        'features': features,
        'target': target_col,
        'embargo_days': EMBARGO_DAYS,
        'min_price': MIN_PRICE,
        'min_dollar_vol': MIN_DOLLAR_VOL,
        'base_rate': base_rate,
        'n_train': len(tv),
        'best_params': {k: v for k, v in best_params.items() if k != 'best_iters'},
        'trigger_col': trigger_col,
        'output_schema': {
            'applicable': 'bool',
            'p_continue' if target_col == 'y1' else 'p_drop': 'float',
            'base_rate': 'float',
            'lift': 'float',
            'n_train': 'int',
        },
        'eligibility': {
            'min_price': MIN_PRICE,
            'min_dollar_vol': MIN_DOLLAR_VOL,
            'trigger_col': trigger_col,
        },
    }, indent=2))
    if len(oof) > 0:
        try:
            oof.to_csv(out_dir / 'oof_predictions.csv', index=False)
        except Exception:
            pass

    # ── test scoring (one-shot) ──────────────────────────────
    rep.h(f'{model_name}: Test Split')
    if not score_test or len(test) == 0:
        rep.add('  Not scored. Pass --score-test to evaluate once.')
    else:
        p_test = final_model.predict_proba(test[features])[:, 1]
        if final_calibrator is not None:
            p_test_cal = apply_platt(final_calibrator, p_test)
        else:
            p_test_cal = p_test
        rows = []
        for label, pp in (('raw', p_test), ('platt', p_test_cal)):
            r = metrics_cls(test['y'].values, pp, test['w'].values)
            r['model'] = label
            rows.append(r)
        rep.table(pd.DataFrame(rows), index=False)

    rep.save(out_dir / 'report.txt')
    print(f'  Saved {model_name} artifacts to {out_dir}')
    return final_model, final_calibrator


# ─────────────────────────────────────────────────────────────
# MODEL 3 TRAINING
# ─────────────────────────────────────────────────────────────

def train_volatility_model(df, features, out_dir, rep, score_test=False):
    """Train Model 3: Realized-Volatility Forecast using LightGBM quantile."""
    leakage_guard(features)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── prepare data ─────────────────────────────────────────
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    mask = (df['row_ok_m3'].fillna(False).astype(bool) &
            df['fwd_vol_10d'].notna())
    df = df[mask].copy()
    df['y3'] = np.log(np.maximum(df['fwd_vol_10d'].astype(float), 0.003))
    df['w'] = df['sample_weight'].astype(float)
    df = df[np.isfinite(df[features].astype(float)).all(axis=1) & np.isfinite(df['y3'])]

    tv = df[df['split'].isin(['train', 'val'])].copy()
    test = df[df['split'] == 'test'].copy()

    rep.add(f'\nModel 3 — Realized-Volatility Forecast:')
    rep.add(f'  Train+val rows: {len(tv):,}  Test: {len(test):,}')
    rep.add(f'  y3 (log fwd_vol_10d) range: [{tv["y3"].min():.3f}, {tv["y3"].max():.3f}]')

    tv = assign_quarter_folds_regression(tv)

    # ── Naive baseline (future vol = current vol_10d) ─────────
    rep.sub('Baseline: naive forecast (volatility_10d_norm * 0.5)')
    if 'volatility_10d_norm' in tv.columns:
        y3_naive_log = np.log(np.maximum(tv['volatility_10d_norm'].astype(float) * 0.5, 0.003))
        naive_m = metrics_reg(tv['y3'].values, y3_naive_log.values, tv['w'].values)
        rep.add(f'  Naive train+val: {naive_m}')

    # ── OLS benchmark ────────────────────────────────────────
    rep.sub('OLS benchmark')
    ols_features = ['volatility_10d_norm', 'atr_norm']
    ols_features = [f for f in ols_features if f in tv.columns]
    ols_oof_parts = []
    for k in range(N_FOLDS):
        val = tv[tv['cv_fold'] == k]
        tr = tv[~tv['cv_fold'].isin([k, (k+1) % N_FOLDS])]
        tr = purge(purge(tr, val),
                   tv[tv['cv_fold'] == (k+1) % N_FOLDS])
        if len(tr) < 10 or len(val) < 2:
            continue
        try:
            ols = fit_ols(tr, ols_features)
            pv = ols.predict(val[ols_features])
            part = val[['date', 'cv_fold', 'y3', 'w']].copy()
            part['p_ols'] = pv
            ols_oof_parts.append(part)
        except Exception as e:
            rep.add(f'    OLS fold {k} failed: {e}')
    if ols_oof_parts:
        ols_oof = pd.concat(ols_oof_parts)
        ols_m = metrics_reg(ols_oof['y3'].values, ols_oof['p_ols'].values, ols_oof['w'].values)
        rep.add(f'  OLS OOF: {ols_m}')

    # ── LightGBM quantile: OOF CV ────────────────────────────
    rep.sub('LightGBM quantile OOF CV (alpha = 0.1, 0.5, 0.9)')
    oof_parts = {a: [] for a in [0.1, 0.5, 0.9]}
    best_iters = {a: [] for a in [0.1, 0.5, 0.9]}
    for k in range(N_FOLDS):
        val = tv[tv['cv_fold'] == k]
        es_fold = (k + 1) % N_FOLDS
        es = purge(tv[tv['cv_fold'] == es_fold], val)
        tr = tv[~tv['cv_fold'].isin([k, es_fold])]
        tr = purge(purge(tr, val), es)
        if len(tr) < 20 or len(val) < 5 or len(es) < 5:
            rep.add(f'  fold {k}: skipped (too few rows)')
            continue
        part_base = val[['date', 'cv_fold', 'y3', 'w']].copy()
        for alpha in [0.1, 0.5, 0.9]:
            try:
                m, bi = fit_lgbm_quantile(tr, es, features, alpha)
                pv = m.predict(val[features])
                best_iters[alpha].append(bi)
                part = part_base.copy()
                part['p_q'] = pv
                oof_parts[alpha].append(part)
            except Exception as e:
                rep.add(f'    alpha={alpha} fold {k} failed: {e}')

    q_oof = {}
    for alpha in [0.1, 0.5, 0.9]:
        if oof_parts[alpha]:
            q_oof[alpha] = pd.concat(oof_parts[alpha])
            m = metrics_reg(q_oof[alpha]['y3'].values,
                            q_oof[alpha]['p_q'].values,
                            q_oof[alpha]['w'].values)
            rep.add(f'  alpha={alpha} OOF: {m}')

    # Band coverage (80% band: p10 to p90 should contain actual ~80% of the time)
    rep.sub('Band coverage (80% band: p10 to p90)')
    if 0.1 in q_oof and 0.9 in q_oof:
        common_idx = q_oof[0.1].index.intersection(q_oof[0.9].index)
        if len(common_idx) > 0:
            p10 = q_oof[0.1].loc[common_idx, 'p_q'].values
            p90 = q_oof[0.9].loc[common_idx, 'p_q'].values
            y_common = q_oof[0.1].loc[common_idx, 'y3'].values
            w_common = q_oof[0.1].loc[common_idx, 'w'].values
            in_band = (y_common >= p10) & (y_common <= p90)
            cov = float((w_common * in_band).sum() / w_common.sum())
            rep.add(f'  80% band coverage (p10-p90): {100 * cov:.1f}% (target ~80%)')

    # per-fold and per-year stability (median quantile)
    if 0.5 in q_oof and len(q_oof[0.5]) > 0:
        rep.sub('Per-fold stability (median quantile, p50)')
        oof50 = q_oof[0.5].copy()
        fold_rows = []
        for k, g in oof50.groupby('cv_fold'):
            r = metrics_reg(g['y3'].values, g['p_q'].values, g['w'].values)
            r['fold'] = k
            fold_rows.append(r)
        if fold_rows:
            rep.table(pd.DataFrame(fold_rows), index=False)

        rep.sub('Per-year stability (p50)')
        oof50['year'] = pd.to_datetime(oof50['date']).dt.year
        yr_rows = []
        for yr, g in oof50.groupby('year'):
            r = metrics_reg(g['y3'].values, g['p_q'].values, g['w'].values)
            r['year'] = yr
            yr_rows.append(r)
        if yr_rows:
            rep.table(pd.DataFrame(yr_rows), index=False)

    # Viability check: does it beat naive in most folds?
    rep.sub('Viability vs naive forecast')
    if 0.5 in q_oof and 'volatility_10d_norm' in tv.columns and len(q_oof[0.5]) > 0:
        oof50 = q_oof[0.5]
        model_wins = 0
        total_folds = 0
        for k, g in oof50.groupby('cv_fold'):
            if len(g) < 5:
                continue
            tv_fold = tv[tv['cv_fold'] == k]
            naive_preds = np.log(np.maximum(
                tv_fold['volatility_10d_norm'].astype(float) * 0.5, 0.003))
            naive_m = metrics_reg(tv_fold['y3'].values, naive_preds.values, tv_fold['w'].values)
            model_m = metrics_reg(g['y3'].values, g['p_q'].values, g['w'].values)
            wins = model_m['rmse'] < naive_m['rmse']
            model_wins += int(wins)
            total_folds += 1
        if total_folds > 0:
            rep.add(f'  Model beats naive in {model_wins}/{total_folds} folds (RMSE).')
            if model_wins < total_folds // 2 + 1:
                rep.add('  WARNING: model does NOT beat naive in majority of folds. '
                        'Consider using volatility_10d directly.')

    # ── fit final models on all train+val ────────────────────
    rep.sub('Final models (all train+val)')
    final_models = {}
    for alpha in [0.1, 0.5, 0.9]:
        iters = max(int(np.median(best_iters.get(alpha, [200])) * 1.1), 50)
        params = dict(LGB_Q_PARAMS, objective='quantile', alpha=alpha,
                      metric='quantile', n_estimators=iters)
        m = lgb.LGBMRegressor(**params)
        m.fit(tv[features], tv['y3'], sample_weight=tv['w'])
        final_models[alpha] = m
        rep.add(f'  alpha={alpha}: {iters} rounds')

    # ── save artifacts ───────────────────────────────────────
    joblib.dump(final_models, out_dir / 'model.pkl')
    (out_dir / 'features.json').write_text(json.dumps({
        'model_type': 'lgbm_quantile',
        'features': features,
        'target': 'y3 = log(max(fwd_vol_10d, 0.003))',
        'output_schema': {
            'daily_vol_p10': 'float (exp of p10 prediction)',
            'daily_vol_p50': 'float (exp of p50 prediction)',
            'daily_vol_p90': 'float (exp of p90 prediction)',
            'expected_10d_move_1sigma': 'float (daily_vol_p50 * sqrt(10))',
        },
        'embargo_days': EMBARGO_DAYS,
        'min_price': MIN_PRICE,
        'min_dollar_vol': MIN_DOLLAR_VOL,
        'n_train': len(tv),
    }, indent=2))
    # save OOF for median quantile
    if 0.5 in q_oof:
        try:
            q_oof[0.5].to_csv(out_dir / 'oof_predictions.csv', index=False)
        except Exception:
            pass
    # no calibrator for regression
    joblib.dump(None, out_dir / 'calibrator.pkl')

    # ── test scoring ─────────────────────────────────────────
    rep.h('Model 3: Test Split')
    if not score_test or len(test) == 0:
        rep.add('  Not scored. Pass --score-test to evaluate once.')
    else:
        rows = []
        for alpha in [0.1, 0.5, 0.9]:
            p_test = final_models[alpha].predict(test[features])
            r = metrics_reg(test['y3'].values, p_test, test['w'].values)
            r['alpha'] = alpha
            rows.append(r)
        rep.table(pd.DataFrame(rows), index=False)
        # band coverage on test
        if 0.1 in final_models and 0.9 in final_models:
            p10_t = final_models[0.1].predict(test[features])
            p90_t = final_models[0.9].predict(test[features])
            in_band = (test['y3'].values >= p10_t) & (test['y3'].values <= p90_t)
            cov = float((test['w'].values * in_band).sum() / test['w'].values.sum())
            rep.add(f'  Test band coverage: {100 * cov:.1f}%')

    rep.save(out_dir / 'report.txt')
    print(f'  Saved Model 3 artifacts to {out_dir}')
    return final_models


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main(score_test=False):
    print('\n' + '=' * 60)
    print('  SqueezeRadar - Three-Model Trainer v1.2')
    print('=' * 60)

    df = load_data()
    df['date'] = pd.to_datetime(df['date'])
    print(f'  Dataset: {len(df):,} rows, {df["symbol"].nunique()} symbols, '
          f'{df["date"].min().date()} -> {df["date"].max().date()}')

    # ── Model 1 — Ignition Continuation ─────────────────────
    rep1 = Report()
    rep1.h('SQUEEZERADAR v1.2 — Model 1: Ignition Continuation')
    rep1.add(f'Generated: {datetime.now().isoformat(timespec="seconds")}')
    rep1.add(f'Features: {M1_FEATURES}')
    rep1.add(f'Target y1: 1 if max_return_10d >= 30% | ign_trigger rows')
    rep1.add(f'Eligibility: MIN_PRICE={MIN_PRICE}, MIN_DOLLAR_VOL={MIN_DOLLAR_VOL}')

    def m1_baseline_rule(df_):
        return ((df_['volume_ratio_30d_norm'] >= 0.25) &
                (df_['price_change_5d_norm'] >= 0.20)).values

    out1 = MODEL_DIR / 'model1_ignition'
    m1_model, m1_cal = train_classification_model(
        df, M1_FEATURES, 'y1', 'ign_trigger', 'row_ok_m1',
        'Model 1 — Ignition Continuation', out1, rep1,
        score_test=score_test, baseline_rule_fn=m1_baseline_rule,
    )
    print()

    # ── Model 2 — Exhaustion / Fade Risk ─────────────────────
    rep2 = Report()
    rep2.h('SQUEEZERADAR v1.2 — Model 2: Exhaustion / Fade Risk')
    rep2.add(f'Generated: {datetime.now().isoformat(timespec="seconds")}')
    rep2.add(f'Features: {M2_FEATURES}')
    rep2.add(f'Target y2: 1 if min_return_5d <= -20% | exh_trigger rows')

    def m2_baseline_rule(df_):
        return (df_['rsi_14_norm'] >= 0.80).values

    out2 = MODEL_DIR / 'model2_exhaustion'
    m2_model, m2_cal = train_classification_model(
        df, M2_FEATURES, 'y2', 'exh_trigger', 'row_ok_m2',
        'Model 2 — Exhaustion / Fade Risk', out2, rep2,
        score_test=score_test, baseline_rule_fn=m2_baseline_rule,
    )
    print()

    # ── Model 3 — Realized-Volatility Forecast ───────────────
    rep3 = Report()
    rep3.h('SQUEEZERADAR v1.2 — Model 3: Realized-Volatility Forecast')
    rep3.add(f'Generated: {datetime.now().isoformat(timespec="seconds")}')
    rep3.add(f'Features: {M3_FEATURES}')
    rep3.add(f'Target y3: log(max(fwd_vol_10d, 0.003))')

    out3 = MODEL_DIR / 'model3_volatility'
    m3_models = train_volatility_model(
        df, M3_FEATURES, out3, rep3, score_test=score_test,
    )
    print()

    print('\n' + '=' * 60)
    print('  All three models trained.')
    print(f'  model1_ignition  -> {out1}')
    print(f'  model2_exhaustion -> {out2}')
    print(f'  model3_volatility -> {out3}')
    print('=' * 60)


if __name__ == '__main__':
    ap = argparse.ArgumentParser(
        description='Train SqueezeRadar three-model suite (v1.2)')
    ap.add_argument('--score-test', action='store_true',
                    help='Score the held-out 2022+ test split (do this once).')
    a = ap.parse_args()
    main(score_test=a.score_test)
