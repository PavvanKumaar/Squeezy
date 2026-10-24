# pipeline/scoring/config.py
#
# All scoring configuration in one place.
# IMPORTANT: every threshold below is an UNVALIDATED heuristic unless noted.
# They are display bands and trigger gates, not tuned or backtested values.

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MODELS_DIR = Path(os.getenv('SQUEEZE_MODELS_DIR', REPO_ROOT / 'models'))
MODEL_SUBDIRS = {
    'ignition':   'model1_ignition',
    'exhaustion': 'model2_exhaustion',
    'volatility': 'model3_volatility',
}
MODEL_VERSION = 'mvp-0.1'

# ── Eligibility (defaults; overridden by each model's features.json) ──
DEFAULT_MIN_PRICE = 0.10
DEFAULT_MIN_DOLLAR_VOL = 50_000
MIN_BARS_FOR_CONTEXT = 15     # daily bars needed to compute 20d dollar volume
MAX_BAR_AGE_DAYS = 5          # newest daily bar older than this => collectors are not running

# ── Model trigger gates, in the _norm space the models were trained on ──
# ign_trigger : volume >= 3x (3/20 = 0.15) AND (1d >= +10% OR 5d >= +25%)
# exh_trigger : 5d >= +25% OR RSI >= 80
IGN_VOLRATIO_NORM = 0.15
IGN_1D = 0.10
IGN_5D = 0.25
EXH_5D = 0.25
EXH_RSI_NORM = 0.80

# Use the Platt calibrator for the displayed probability?
# Run `python -m pipeline.scoring.refit_calibrators` first: the calibrators saved
# by the trainer were fitted on in-sample scores.
USE_CALIBRATOR = {'ignition': True, 'exhaustion': True}

# ── Rule-based lifecycle tracker (raw units) ──
PHASE = dict(
    min_bars=25,
    ign_lookback=30,        # trading days to look back for an ignition
    ign_volratio=3.0,
    ign_1d=0.10,
    ign_5d=0.25,
    cluster_gap=3,          # ignition days within this gap form one cluster
    cooldown_drawdown=-0.25,
    danger_drawdown=-0.10,
    danger_peak_days=5,
    cooldown_peak_days=15,
    below_ignition_close=0.90,
)

# ── Interpretation text ──
LIFT_BANDS = [(0.8, 'below typical'), (1.25, 'about typical'),
              (2.0, 'above typical')]          # beyond the last: 'well above typical'
BASE_RATE_CAVEAT = ('Typical rate comes from a broad universe of US stocks '
                    '(2018-2026), not only your screened squeeze universe.')

# ── Group scoring (equal weights) ──
# Each feature earns points by status; group score = 100 x mean points over the
# group's AVAILABLE scoring features. squeeze_score = plain average of the
# available group scores (equal weights, reweighted when a group has no data).
# Heuristic, not calibrated, not a probability.
STATUS_POINTS = {'normal': 0.0, 'elevated': 0.5, 'high': 1.0}
SCORED_GROUPS = ['momentum', 'fuel', 'short_stress', 'attention', 'catalyst', 'gamma']
GROUP_HOT = 60.0      # group score >= 60 -> 'hot'
GROUP_WARM = 25.0     # group score >= 25 -> 'warm', else 'quiet'

# Redis TTL (seconds) re-applied after save_score so cached scores survive between
# scoring runs without editing stock_state_manager.py (its default TTL is 300s).
REDIS_TTL_S = 1800