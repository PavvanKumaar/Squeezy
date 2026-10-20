# SqueezeRadar v1.2 — Three-Model Architecture and Dataset Specification

> **Status:** Design only. Nothing has been trained except where marked `MEASURED`.
> Thresholds are proposals, not tuned values.
> *Written by Claude on 2026-10-19. Handoff spec for a coding assistant.*

---

## 1. Context and Existing Code

SqueezeRadar scores US stocks for squeeze-like behavior. At runtime a feature pipeline produces a **28-feature vector** per ticker per day. Training data comes only from `yfinance` daily OHLCV, so only OHLCV-derived features can be trained on.

**Extend existing code — do not rewrite:**

| File | Key Functions |
|------|--------------|
| `notebooks/01_build_training_data_v1_1.py` | `compute_features`, `derive_episode`, `process_symbol`, `assign_split_groups_folds`, `build_report` |
| `notebooks/02_train_models.py` | `assign_quarter_folds`, `purge`, Platt scaling, text report |

**Output:** `data/training/v1_1_raw.parquet` (falls back to `.pkl` — pyarrow broken on user's Windows machine)

### MEASURED Results So Far

- A model predicting any 30% forward move from 7 OHLCV features mostly ranks volatility.
- Importances led by `distance_from_52w_high`, price moves, and RSI.
- With `atr_pct` and `volatility_10d` removed: no better than ATR alone, slightly worse than the rule (`volume_ratio >= 5` AND `5-day move >= 20%`).

> **Conclusion:** Do not expect strong skill. Every model below must beat its named baselines in grouped CV or be replaced by a simpler lookup table.

---

## 2. Runtime Architecture

1. Feature engineering produces the 28-feature vector (values in `_norm` columns).
2. A per-model selector takes only the columns listed in section 4.
3. **Gates:** Model 1 and Model 2 run only when their trigger fires (section 3). Otherwise they return `applicable: false`. Model 3 runs for every eligible stock every day.
4. Each model returns a calibrated output plus its reference `base_rate` and training sample size.
5. The three outputs feed the squeeze score as separate components next to the fuel, crowd, options and catalyst layers — those stay rule-based until live data has history. The lifecycle phase stays a rule-based tracker, not a trained model.
6. **Log daily (never overwritten):** full 28-feature vector, all model outputs, next 5 and 10 day outcomes — so later versions can train on real fuel, crowd and options data.

### Eligibility (must match training)

| Constant | Value |
|----------|-------|
| `MIN_PRICE` | `0.10` |
| `MIN_DOLLAR_VOL` | `50000` |
| Min history rows | `60` |

- Trailing 20-day average dollar volume (prior days) >= `MIN_DOLLAR_VOL`
- Yahoo data below `0.10` is noisy — **report how many rows fall between 0.10 and 0.50**

---

## 3. Exact Feature Definitions and Triggers

> All features use only data up to and including day `t`. Training must use the same `_norm` columns as runtime.

| Feature | Definition |
|---------|-----------|
| `volume_ratio_30d_norm` | `clip(volume_t / mean(volume over prior 30 days, min 20), 0, 20) / 20` |
| `price_change_1d_norm` | `clip(close_t / close_(t-1) - 1, -1, 1)` |
| `price_change_5d_norm` | `clip(close_t / close_(t-5) - 1, -1, 1)` |
| `rsi_14_norm` | `WilderRSI(14) / 100` |
| `volatility_10d_norm` | `clip(std of daily returns over last 10 days, 0, 0.5) / 0.5` |
| `atr_norm` | `clip(WilderATR(14) / close, 0, 1)` |
| `abs_price_change_1d_norm` | `abs(price_change_1d_norm)` (derived at feature time) |
| `abs_price_change_5d_norm` | `abs(price_change_5d_norm)` (derived at feature time) |

> **Note:** Norm columns clip extremes (volume ratio at 20x, price moves at ±100%). Ignition days are where clipping happens. First version accepts this for runtime parity. Report the share of clipped rows.

### Triggers (norm space, identical to raw thresholds)

```python
ign_trigger = (
    volume_ratio_30d_norm >= 0.15   # 3x
    AND (price_change_1d_norm >= 0.10 OR price_change_5d_norm >= 0.25)
)

exh_trigger = (
    price_change_5d_norm >= 0.25 OR rsi_14_norm >= 0.80
)
```

---

## 4. The Three Models

### Model 1 — Ignition Continuation

**Question:** A stock just spiked; will it keep running?

| Property | Value |
|----------|-------|
| Rows used | `ign_trigger == True` AND eligible AND row features complete AND label valid |
| Input features | `volume_ratio_30d_norm`, `price_change_1d_norm`, `price_change_5d_norm`, `rsi_14_norm` |
| Optional later | Add `distance_from_52w_high_norm` |
| Target `y1` | `1` if `max_return_10d >= 0.30`; `NaN` if 10-day window incomplete or spans data gap (> 18 calendar days) |

**Algorithms:**
- (a) Standardized logistic regression, L2, C ∈ {0.01, 0.03, 0.1, 0.3, 1}
- (b) Shallow LightGBM: `num_leaves` 4–7, `min_child_samples` 50–100, `learning_rate` 0.03, `reg_lambda` 10, early stopping on logloss
- Keep whichever wins grouped CV; prefer the simpler model on ties.

**Calibration:** Platt scaling on out-of-fold scores if model is LightGBM. Check the reliability table.

**Output:** `p_continue` — calibrated probability of a further 30% run within 10 days.

**Interpretation:**
- Always show `base_rate` (share of ALL training triggers that continued) and `lift = p_continue / base_rate`, plus training sample size.
- Example: 34% vs 20% base rate = 1.7x lift.
- Value near base rate → features add nothing for this stock.
- `applicable: false` → no trigger fired today (not the same as low probability).

**Baselines it must beat:**
1. The base rate
2. Rule: `volume_ratio_30d >= 5` AND `price_change_5d >= 0.20`

**Metrics:** weighted PR-AUC, Brier score, lift in top 20% of scores, calibration table, per-fold and per-year stability.

**Viability check:** Print positive trigger rows in train+val. If fewer than ~300, ship a bucketed base-rate lookup (e.g., volume ratio tercile × 5-day move tercile) instead of a fitted model.

---

### Model 2 — Exhaustion / Fade Risk

**Question:** After a big run, is a sharp drop coming?

| Property | Value |
|----------|-------|
| Rows used | `exh_trigger == True` AND eligible |
| Input features | `volume_ratio_30d_norm`, `price_change_1d_norm`, `price_change_5d_norm`, `rsi_14_norm` |
| Target `y2` | `1` if `min_return_5d <= -0.20` (new column); fallback: `return_5d <= -0.20` |

**Algorithms, calibration, baselines:** Same as Model 1.

**Baselines it must beat:**
1. Base rate among exhaustion triggers
2. Rule: `rsi_14_norm >= 0.80`

**Output:** `p_drop` — calibrated probability of a 20%+ drop within 5 days.

**Interpretation:**
- A risk flag, not the mirror of Model 1. The two probabilities **do not sum to 1** and both can be high on a volatile stock.
- Display side by side, e.g., `run: 22%` and `drop: 18%`, each with its own base rate and lift.
- High `p_drop` + high `p_continue` = violent two-way move expected, not a contradiction.

---

### Model 3 — Realized-Volatility Forecast

**Question:** How big will the next 10 days of daily moves be?

| Property | Value |
|----------|-------|
| Rows used | All eligible rows with complete features and valid target; use `sample_weight` (non-trigger rows subsampled) |
| Input features (8) | `volume_ratio_30d_norm`, `price_change_1d_norm`, `price_change_5d_norm`, `rsi_14_norm`, `volatility_10d_norm`, `atr_norm`, `abs_price_change_1d_norm`, `abs_price_change_5d_norm` |
| Target `y3` | `log(max(fwd_vol_10d, 0.003))` where `fwd_vol_10d = std(ddof=1)` of 10 daily returns over t+1..t+10 |

**Algorithms:**
- OLS on `y3` with `log(volatility_10d_norm * 0.5)` and `atr_norm` as benchmark
- LightGBM quantile at alpha 0.1, 0.5, 0.9 (three models): `num_leaves` 15, `min_child_samples` 200, `learning_rate` 0.03, early stopping

**Output:** `daily_vol_p10`, `daily_vol_p50`, `daily_vol_p90` (exp of predictions), and:
```
expected_10d_move_1sigma = daily_vol_p50 * sqrt(10)
```

**Interpretation:** Typical daily swing with an 80% band. A 30% move is `0.3 / expected_10d_move_1sigma` sigmas away. Can normalize Models 1 & 2 and size positions.

**Baseline it must beat:** Naive forecast that future volatility = `volatility_10d`.

**Metrics:** RMSE and R² on `y3`, band coverage (should be near 80%), per-fold and per-year stability. If it does not beat the naive forecast in most folds, drop it and use `volatility_10d` directly.

---

## 5. Dataset Modifications (Builder v1.2)

**Current dataset:** `v1_1_raw` — one row per eligible ticker-day.
- Event-window rows: weight 1
- Other rows: kept at 20% (weight 5)

**Existing columns:** `date`, `symbol`, `split`, `group_id`, `cv_fold`, `row_ok`, `sample_weight`, `event_id`, `event_source`, `event_status`, `event_use`, `event_ignition`, `lifecycle_phase`, `days_from_ignition`, `close`, `volume`, `history_days`, `dollar_vol_20d`, the 7 raw features (`volume_ratio_30d`, `price_change_1d`, `price_change_5d`, `rsi_14`, `distance_from_52w_high`, `atr_pct`, `volatility_10d`), their 7 `_norm` versions, `return_5d`, `return_10d`, `max_return_5d`, `max_return_10d`, `squeezed_5d`, `squeezed_10d`.

### Required Changes

All computed per symbol on the **full price series** before eligibility filtering and subsampling.

#### 1. New forward columns in `compute_features`

```python
# fwd_vol_10d
ret = close.pct_change()
fwd_vol_10d = ret.shift(-10).rolling(10, min_periods=10).std()
# Set NaN when (date[t+10] - date[t]) > 18 calendar days

# min_return_5d and min_return_10d
# min(close[t+1..t+n]) / close[t] - 1, built like existing future_max with sliding windows
# Same NaN rule: > 10 calendar days for 5d

# Optional
baseline_vol_60d_lag10 = std(daily returns over t-69..t-10)  # kept for later volatility-adjusted targets
```

#### 2. Derived columns

```python
abs_price_change_1d_norm = abs(price_change_1d_norm)
abs_price_change_5d_norm = abs(price_change_5d_norm)
# Boolean flags from _norm columns (so training and runtime share one code path):
ign_trigger = (volume_ratio_30d_norm >= 0.15) & ((price_change_1d_norm >= 0.10) | (price_change_5d_norm >= 0.25))
exh_trigger = (price_change_5d_norm >= 0.25) | (rsi_14_norm >= 0.80)
```

#### 3. Sampling change in `process_symbol`

| Row type | Keep fraction | Weight |
|----------|--------------|--------|
| Event-window rows | 100% | 1 |
| `ign_trigger == True` OR `exh_trigger == True` | 100% | 1 |
| All other rows | 20% | 5 |

- Models 1 and 2 train only on trigger rows (weight 1).
- Model 3 trains on everything with `sample_weight`.

#### 4. Eligibility constants

```python
MIN_PRICE = 0.10       # lowered from previous
MIN_DOLLAR_VOL = 50000  # lowered from previous
```

#### 5. Split-artifact guard

Flag a forward window as suspect when:
- A daily close ratio inside it is within 2% of a common split ratio (1/2, 1/3, 1/4, 1/5, 1/10, 2, 3, 4, 5, 10)
- **AND** that day's volume ratio is below 1.5

→ Set that row's forward targets to `NaN` and count them in the report.

#### 6. Splits and folds (unchanged)

| Split | Date range |
|-------|-----------|
| Train | before 2021-07-01 |
| Validation | 2021-07 to 2021-12 |
| Test | from 2022-01-01 |

- 21-day embargo before each boundary.
- Trainer rebuilds quarter folds from each row's own date (`assign_quarter_folds`) and purges 21 days at quarter edges.
- The 2022+ test split has been scored once before — **decide all thresholds from grouped CV**, treat any further test number as optimistic. Prefer a forward holdout from live logging.

#### 7. Report additions in `build_report`

- Trigger-row counts per split and per year
- Positives and base rate for `y1` and `y2` per split
- Counts and base rate for Model 3's target
- Share of norm values clipped
- Number of rows between 0.10 and 0.50 in price
- Number of rows invalidated by the split guard

**Save output as:** `v1_2_raw` (parquet with pickle fallback) — keep `v1_1` reproducible.

---

## 6. Training Protocol

**New script:** `notebooks/03_train_three_models.py` — reuse helpers from `02_train_models.py`

### Data loading

```python
# Load v1_2_raw, keep row_ok rows with non-null target for the model being trained
```

### Grouped CV

- Five folds by calendar quarter of the row date
- Balanced by positives
- 21-day embargo (reuse `assign_quarter_folds` and `purge`)
- Nested early stopping as in `02_train_models.py`

### Per-model steps

| Model | Steps |
|-------|-------|
| Model 1 & 2 | Compare logistic regression and shallow LightGBM; OOF predictions; Platt scaling cross-fitted for reporting; final calibrator fit on all OOF scores |
| Model 3 | Compare OLS, naive, and LightGBM quantile models; report band coverage |

### Output artifacts (save to `models/<model_name>/`)

- `model.pkl` — trained model file
- `calibrator.pkl` — Platt scaler
- `features.json` — exact feature list, thresholds, eligibility constants
- `oof_predictions.csv` — out-of-fold predictions
- `report.txt` — metrics, base rates, sample sizes, calibration table

### Leakage guard — NEVER use as features

Columns starting with: `return_`, `max_return_`, `min_return_`, `fwd_`, `squeezed_`, `event_`, `lifecycle`, `days_from`, `cv_`, `group`, `split`, `sample_weight`, `row_ok`, `symbol`, `date`

### Sanity check

Run label-shuffle check on each model — shuffled-label ROC should sit near 0.5.

---

## 7. Acceptance Checklist

- [ ] Each fitted model beats its baselines in a majority of folds and in most years, or is replaced by a lookup table / naive forecast.
- [ ] Calibration table is roughly monotone and mean prediction is near the base rate.
- [ ] Reports state positives, base rates and sample sizes for every model.
- [ ] Live code computes features, eligibility and triggers with the **same functions** as training.
- [ ] Runtime output schema per model:

```
# Example
ignition:
  applicable: true
  p_continue: 0.31
  base_rate: 0.19
  lift: 1.6
  n_train: 4200
```

---

## 8. Known Risks

| Risk | Detail |
|------|--------|
| **Survivorship bias** | Universe is current listings — delisted names are missing. Results are an upper bound. Test a vendor with delisted history (Polygon, Tiingo, EODHD, Norgate; none verified) before relying on older years. |
| **Mined ignitions** | Price-defined momentum events, not proof of short squeezes. |
| **Missing data** | Short interest, borrow, options, and social data are absent — none of these models measure squeeze fuel. |
| **Small trigger samples** | Report counts first; prefer simple models. |
| **Disclaimer** | Model outputs are probabilities of price moves in history, not advice or guarantees. |

---

## Quick Reference: File Map

```
squeezeradar/
├── notebooks/
│   ├── 01_build_training_data_v1_1.py   # extend → produces v1_2_raw
│   ├── 02_train_models.py               # reuse helpers
│   └── 03_train_three_models.py         # NEW — trains all three models
├── data/
│   └── training/
│       ├── cache/                        # per-symbol pickle cache
│       ├── v1_1_raw.parquet / .pkl       # do not overwrite
│       └── v1_2_raw.parquet / .pkl       # new output
└── models/
    ├── model1_ignition/
    │   ├── model.pkl
    │   ├── calibrator.pkl
    │   ├── features.json
    │   ├── oof_predictions.csv
    │   └── report.txt
    ├── model2_exhaustion/
    │   └── (same structure)
    └── model3_volatility/
        └── (same structure)
```
