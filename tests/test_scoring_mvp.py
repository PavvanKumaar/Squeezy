# tests/test_scoring_mvp.py  -  run: python tests/test_scoring_mvp.py   (or pytest)
# Uses FAKE model artifacts with the same structure as models/model*/ so no real
# models, database or Redis are needed.

import json
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.scoring import config as C                      # noqa: E402
from pipeline.scoring.feature_groups import (FEATURES, build_group_panels,   # noqa: E402
                                             exhaustion_flags)
from pipeline.scoring.lifecycle_tracker import classify_phase  # noqa: E402
from pipeline.scoring.market_context import MarketContextReader  # noqa: E402
from pipeline.scoring.model_router import ModelRouter, compute_triggers, build_model_inputs  # noqa: E402
from pipeline.scoring.scorer import SqueezeScorer, format_text  # noqa: E402

FEATURE_ORDER = [
    'short_interest_pct_norm', 'days_to_cover_norm', 'borrow_rate_norm', 'si_staleness_penalty',
    'borrow_rate_delta_norm', 'short_volume_ratio', 'volume_ratio_30d_norm', 'float_rotation_norm',
    'price_change_1d', 'price_change_5d', 'rsi_14_norm', 'distance_from_52w_high_norm', 'atr_norm',
    'volatility_10d_norm', 'call_put_ratio_norm', 'iv_rank_norm', 'max_pain_distance_norm',
    'options_available', 'unusual_sweep_flag', 'stocktwits_bull_ratio', 'stocktwits_message_count_norm',
    'google_trend_score_norm', 'trend_spike_flag', 'news_catalyst_flag', 'has_activist_filing',
    'news_sentiment_score', 'news_squeeze_keyword_score', 'news_catalyst_composite']

M1F = ['volume_ratio_30d_norm', 'price_change_1d_norm', 'price_change_5d_norm', 'rsi_14_norm']
M3F = M1F + ['volatility_10d_norm', 'atr_norm', 'abs_price_change_1d_norm', 'abs_price_change_5d_norm']


def make_models(root: Path):
    import pandas as pd
    rng = np.random.default_rng(0)
    X = pd.DataFrame(rng.random((600, 4)), columns=M1F)
    y = (rng.random(600) < 0.2).astype(int)
    meta1 = {'model_type': 'logistic', 'features': M1F, 'base_rate': 0.1795, 'n_train': 3700,
             'min_price': 0.1, 'min_dollar_vol': 50000}
    meta2 = dict(meta1, model_type='lgbm', base_rate=0.1184, n_train=11936)
    meta3 = {'model_type': 'lgbm_quantile', 'features': M3F, 'n_train': 157001,
             'min_price': 0.1, 'min_dollar_vol': 50000}
    for sub, meta in (('model1_ignition', meta1), ('model2_exhaustion', meta2), ('model3_volatility', meta3)):
        (root / sub).mkdir(parents=True)
        (root / sub / 'features.json').write_text(json.dumps(meta))
    m1 = Pipeline([('s', StandardScaler()), ('lr', LogisticRegression())]).fit(X, y)
    joblib.dump(m1, root / 'model1_ignition' / 'model.pkl')
    m2 = lgb.LGBMClassifier(n_estimators=20, num_leaves=5, verbose=-1).fit(X, y)
    joblib.dump(m2, root / 'model2_exhaustion' / 'model.pkl')
    for sub, m in (('model1_ignition', m1), ('model2_exhaustion', m2)):
        p = m.predict_proba(X)[:, 1]
        z = np.log(np.clip(p, 1e-6, 1 - 1e-6) / (1 - np.clip(p, 1e-6, 1 - 1e-6)))
        joblib.dump(LogisticRegression().fit(z.reshape(-1, 1), y), root / sub / 'calibrator.pkl')
    X3 = pd.DataFrame(rng.random((600, 8)), columns=M3F)
    y3 = rng.normal(-3.5, 0.5, 600)
    regs = {a: lgb.LGBMRegressor(objective='quantile', alpha=a, n_estimators=20, verbose=-1).fit(X3, y3)
            for a in (0.1, 0.5, 0.9)}
    joblib.dump(regs, root / 'model3_volatility' / 'model.pkl')
    joblib.dump(None, root / 'model3_volatility' / 'calibrator.pkl')


def hot_assembled():
    raw = {'short_interest_pct': 32, 'days_to_cover': 6, 'borrow_rate': 45, 'si_staleness_penalty': 0.2,
           'borrow_rate_delta': 12, 'short_volume_ratio': 0.62, 'volume_ratio_30d': 5.0,
           'float_rotation': 1.4, 'price_change_1d': 0.14, 'price_change_5d': 0.31, 'rsi_14': 83,
           'distance_from_52w_high': 0.04, 'atr_norm': 0.11, 'volatility_10d': 0.09,
           'call_put_ratio': 3.1, 'iv_rank': 92, 'max_pain_distance': 0.1, 'options_available': 1.0,
           'unusual_sweep_flag': 1.0, 'stocktwits_bull_ratio': 0.92, 'stocktwits_message_count': 140,
           'google_trend_score': 95, 'trend_spike_flag': 1, 'news_catalyst_flag': 1,
           'has_activist_filing': 0, 'news_sentiment_score': 0.5, 'news_squeeze_keyword_score': 0.7,
           'news_catalyst_composite': 0.65}
    norm = {'short_interest_pct_norm': .32, 'days_to_cover_norm': .3, 'borrow_rate_norm': .15,
            'si_staleness_penalty': .2, 'borrow_rate_delta_norm': .12, 'short_volume_ratio': .62,
            'volume_ratio_30d_norm': .25, 'float_rotation_norm': .28, 'price_change_1d': .14,
            'price_change_5d': .31, 'rsi_14_norm': .83, 'distance_from_52w_high_norm': .04,
            'atr_norm': .11, 'volatility_10d_norm': .18, 'call_put_ratio_norm': .31, 'iv_rank_norm': .92,
            'max_pain_distance_norm': .1, 'options_available': 1.0, 'unusual_sweep_flag': 1.0,
            'stocktwits_bull_ratio': .92, 'stocktwits_message_count_norm': .28, 'google_trend_score_norm': .95,
            'trend_spike_flag': 1.0, 'news_catalyst_flag': 1.0, 'has_activist_filing': 0.0,
            'news_sentiment_score': .5, 'news_squeeze_keyword_score': .7, 'news_catalyst_composite': .65}
    return {'raw': raw, 'normalized': norm, 'missing_features': [], 'news_nlp_available': True,
            'quality_score': 96.0, 'news_article_count': 12, 'si_proxy_direction': 'rising'}


def bars(n=60, ign_ago=None, spike=3.0, after=None):
    """Flat-ish bars; optional ignition `ign_ago` days before the end then a path `after`."""
    closes = [10.0 + 0.01 * (i % 3) for i in range(n)]
    vols = [1e6] * n
    if ign_ago is not None:
        i = n - 1 - ign_ago
        closes[i] = closes[i - 1] * 1.30
        vols[i] = 1e6 * spike * 2
        for k, c in enumerate(after or [], start=1):
            if i + k < n:
                closes[i + k] = closes[i] * c
    d0 = date(2026, 1, 1)
    return [{'date': d0 + timedelta(days=i), 'close': closes[i], 'volume': vols[i]} for i in range(n)]


# ───────────────────────── tests ─────────────────────────
def test_all_28_features_mapped():
    names = [f[0] for f in FEATURES]
    assert len(names) == 28 and set(names) == set(FEATURE_ORDER)


def test_equal_weight_score():
    a = hot_assembled()
    p = build_group_panels(a['raw'], a['normalized'], [], {}, True, 96.0)
    scored = [s for g, s in p['group_scores'].items() if g in C.SCORED_GROUPS and s is not None]
    assert len(scored) == 6
    assert abs(p['squeeze_score'] - round(sum(scored) / 6, 1)) < 0.11
    assert p['squeeze_score'] > 80 and p['confidence']['level'] == 'high'


def test_missing_group_is_excluded_not_zeroed():
    a = hot_assembled()
    a['raw']['options_available'] = 0.0
    p = build_group_panels(a['raw'], a['normalized'], [], {}, True, 80.0)
    assert p['group_scores']['gamma'] is None and p['coverage'] < 1
    scored = [s for g, s in p['group_scores'].items() if g in C.SCORED_GROUPS and s is not None]
    assert len(scored) == 5 and abs(p['squeeze_score'] - round(sum(scored) / 5, 1)) < 0.11


def test_never_collected_source_is_unavailable():
    a = hot_assembled()
    fresh = {'iborrowdesk': {'last_collected': None, 'stale': True, 'very_stale': True}}
    p = build_group_panels(a['raw'], a['normalized'], [], fresh, True, 90.0)
    ss = [g for g in p['groups'] if g['id'] == 'short_stress'][0]
    assert any(f['name'] == 'borrow_rate_delta_norm' and not f['available'] for f in ss['features'])


def test_exhaustion_flags():
    a = hot_assembled()
    p = build_group_panels(a['raw'], a['normalized'], [], {}, True, 90.0)
    fl = exhaustion_flags(a['raw'], p['availability'])
    names = {f['flag'] for f in fl['fired']}
    assert {'euphoric_crowd', 'options_frenzy', 'trend_peak', 'overbought'} <= names
    assert 'parabolic' not in names          # 31% 5d gain is below the 50% bar


def test_triggers_and_inputs():
    a = hot_assembled()
    x = build_model_inputs(a['normalized'])
    t = compute_triggers(x)
    assert t == {'ign_trigger': True, 'exh_trigger': True}
    quiet = dict(a['normalized'], volume_ratio_30d_norm=0.05, price_change_1d=0.01,
                 price_change_5d=0.02, rsi_14_norm=0.5)
    assert compute_triggers(build_model_inputs(quiet)) == {'ign_trigger': False, 'exh_trigger': False}
    assert x['abs_price_change_1d_norm'] == abs(x['price_change_1d_norm'])


def test_router_gating_and_outputs():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        make_models(root)
        r = ModelRouter(models_dir=root)
        a = hot_assembled()
        ctx = {'price': 5.0, 'dollar_vol_20d': 2e6, 'n_bars': 60}
        out = r.route(a['normalized'], ctx)
        assert out['eligible'] and out['ignition']['applicable'] and out['exhaustion']['applicable']
        ig = out['ignition']
        assert 0 <= ig['p_continue'] <= 1 and ig['base_rate'] == 0.1795 and ig['lift'] is not None
        v = out['volatility']
        assert v['daily_vol_p10'] <= v['daily_vol_p50'] <= v['daily_vol_p90']
        # not eligible: penny + illiquid
        out2 = r.route(a['normalized'], {'price': 0.05, 'dollar_vol_20d': 1e6, 'n_bars': 60})
        assert not out2['eligible'] and not out2['ignition']['applicable']
        out3 = r.route(a['normalized'], {'price': 5.0, 'dollar_vol_20d': 1e4, 'n_bars': 60})
        assert not out3['eligible']
        out4 = r.route(a['normalized'], {'price': 5.0, 'dollar_vol_20d': None, 'n_bars': 5})
        assert not out4['eligible'] and 'bars' in out4['eligibility_reason']
        # quiet stock: volatility runs, classifiers gated off
        quiet = dict(a['normalized'], volume_ratio_30d_norm=0.05, price_change_1d=0.01,
                     price_change_5d=0.02, rsi_14_norm=0.5)
        out5 = r.route(quiet, ctx)
        assert out5['volatility']['applicable'] and not out5['ignition']['applicable'] \
            and not out5['exhaustion']['applicable']


def test_stale_bars_block_models():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        make_models(root)
        r = ModelRouter(models_dir=root)
        a = hot_assembled()
        out = r.route(a['normalized'], {'price': 5.0, 'dollar_vol_20d': 2e6, 'n_bars': 60,
                                        'bar_age_days': 12})
        assert not out['eligible'] and '12 days old' in out['eligibility_reason']
        ok = r.route(a['normalized'], {'price': 5.0, 'dollar_vol_20d': 2e6, 'n_bars': 60,
                                       'bar_age_days': 3})
        assert ok['eligible']


def test_session_helpers():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from pipeline.scoring.session import last_completed_session_date as lcs, market_session_state as st
    et = ZoneInfo('America/New_York')
    assert lcs(datetime(2026, 10, 23, 9, 31, tzinfo=et)) == date(2026, 10, 22)   # Fri pre-close -> Thu
    assert lcs(datetime(2026, 10, 23, 16, 10, tzinfo=et)) == date(2026, 10, 23)  # Fri after close
    assert lcs(datetime(2026, 10, 24, 12, 0, tzinfo=et)) == date(2026, 10, 23)   # Sat -> Fri
    assert lcs(datetime(2026, 10, 26, 9, 31, tzinfo=et)) == date(2026, 10, 23)   # Mon open -> Fri
    assert st(datetime(2026, 10, 23, 9, 31, tzinfo=et)) == 'open'
    assert st(datetime(2026, 10, 24, 12, 0, tzinfo=et)) == 'weekend'


def test_lifecycle_phases():
    quiet = {'momentum': 'quiet'}
    assert classify_phase(bars(10), quiet, {'count': 0})['phase'] == 'Early Setup'
    assert classify_phase(bars(60), quiet, {'count': 0})['phase'] == 'Early Setup'
    assert classify_phase(bars(60), {'momentum': 'warm'}, {'count': 0})['phase'] == 'Heating Up'
    assert classify_phase(bars(60), {'fuel': 'hot', 'attention': 'hot'}, {'count': 0})['phase'] == 'Heating Up'
    run = [1.05, 1.10, 1.15, 1.20, 1.22, 1.25, 1.26, 1.27]
    assert classify_phase(bars(60, ign_ago=8, after=run), quiet, {'count': 0})['phase'] == 'Squeeze Zone'
    pull = [1.2, 1.4, 1.5, 1.45, 1.35, 1.30]          # peak then -13%
    assert classify_phase(bars(60, ign_ago=6, after=pull), quiet, {'count': 0})['phase'] == 'Danger Zone'
    assert classify_phase(bars(60, ign_ago=8, after=run), quiet, {'count': 2})['phase'] == 'Danger Zone'
    crash = [1.4, 1.5, 1.3, 1.1, 1.0, 0.95, 0.9, 0.85]
    assert classify_phase(bars(60, ign_ago=8, after=crash), quiet, {'count': 0})['phase'] == 'Cooldown'


def test_context_dollar_volume_uses_prior_bars():
    b = bars(40)
    b[-1]['volume'] = 1e9                       # today's spike must not inflate it
    ctx = MarketContextReader.build_context(b)
    assert ctx['dollar_vol_20d'] < 1e8 and ctx['price'] == b[-1]['close']


class FakeRedis:
    def __init__(self): self.calls = []
    def expire(self, k, t): self.calls.append(('expire', k, t))
    def publish(self, ch, m): self.calls.append(('publish', ch))


class FakeState:
    def __init__(self): self.order, self.saved, self.redis, self.pg = [], {}, FakeRedis(), None
    def get_data_freshness(self, t): return {}
    def detect_phase_transition(self, t, ph): self.order.append('detect'); return True
    def save_score(self, t, s):
        self.order.append('save'); assert 'squeeze_score' in s; self.saved[t] = s; return True


class FakeAssembler:
    def assemble(self, t): return hot_assembled()


class FakeCtx:
    def __init__(self, b): self.b = b
    def load_daily_bars(self, t, n=80): return self.b
    def build_context(self, bars):
        return MarketContextReader.build_context(bars, today=bars[-1]['date'] + timedelta(days=1))


class FakeStore:
    def __init__(self): self.rows = []
    def save(self, t, p): self.rows.append((t, p['squeeze_score'])); return True


def test_scorer_end_to_end():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        make_models(root)
        st = FakeState()
        b = bars(60, ign_ago=8, after=[1.05, 1.1, 1.15, 1.2, 1.22, 1.25, 1.26, 1.27])
        sc = SqueezeScorer(assembler=FakeAssembler(), state=st, router=ModelRouter(models_dir=root),
                           ctx=FakeCtx(b), store=FakeStore())
        s = sc.score_and_store('gme')
        assert st.order == ['detect', 'save'], 'phase transition must be detected BEFORE save_score'
        assert s['ticker'] == 'GME' and 0 <= s['squeeze_score'] <= 100
        assert set(s['models_run']) == {'ignition', 'exhaustion', 'volatility'}
        saved = st.saved['GME']
        json.dumps(saved)                                   # JSON-safe
        assert ('expire', 'score:GME', C.REDIS_TTL_S) in st.redis.calls
        text = format_text(saved)
        assert 'Fuel' in text and 'Options pressure' in text and 'ML models' in text
        print(text)


if __name__ == '__main__':
    fns = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    for f in fns:
        f()
        print('PASS', f.__name__)
    print(f'\n{len(fns)} tests passed')