# pipeline/scoring/refit_calibrators.py
#
# The Platt calibrators saved by 03_train_three_models.py were fitted on the final
# model's IN-SAMPLE scores. This refits them on the saved OUT-OF-FOLD scores
# (models/<m>/oof_predictions.csv, columns y, w, p_model), which is the honest way.
#
#   python -m pipeline.scoring.refit_calibrators

import json
import math

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

from pipeline.scoring import config as C


def _logit(p):
    p = np.clip(np.asarray(p, float), 1e-6, 1 - 1e-6)
    return np.log(p / (1 - p))


def refit(key):
    d = C.MODELS_DIR / C.MODEL_SUBDIRS[key]
    f = d / 'oof_predictions.csv'
    if not f.exists():
        print(f'  {key}: {f} not found - skipped')
        return
    df = pd.read_csv(f).dropna(subset=['y', 'w', 'p_model'])
    lr = LogisticRegression(C=1e6, max_iter=1000)
    lr.fit(_logit(df['p_model']).reshape(-1, 1), df['y'], sample_weight=df['w'])
    base = float((df['w'] * df['y']).sum() / df['w'].sum())
    p_cal = lr.predict_proba(_logit(df['p_model']).reshape(-1, 1))[:, 1]
    print(f'  {key}: rows={len(df)} base={base:.4f} '
          f'mean raw={float((df["w"] * df["p_model"]).sum() / df["w"].sum()):.4f} '
          f'mean calibrated={float((df["w"] * p_cal).sum() / df["w"].sum()):.4f} '
          f'slope={lr.coef_[0][0]:.3f} intercept={lr.intercept_[0]:.3f}')
    joblib.dump(lr, d / 'calibrator.pkl')


if __name__ == '__main__':
    for k in ('ignition', 'exhaustion'):
        refit(k)
    print('done - calibrator.pkl files refitted on out-of-fold scores')
