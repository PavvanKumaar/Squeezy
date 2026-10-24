# pipeline/scoring/scorer.py
#
# Orchestrates one scoring cycle for one ticker. Uses your existing classes
# unchanged: FeatureAssembler.assemble() and StockStateManager.
#
#   assemble 28 features
#     -> grouped panels + group scores + equal-weight squeeze_score
#     -> exhaustion flags
#     -> market context (price, 20d $ volume) from stock_timeseries
#     -> model router (trigger gates -> models 1/2/3)
#     -> rule-based lifecycle phase
#     -> detect_phase_transition  (BEFORE save_score, see note below)
#     -> save_score (Redis + stock_scores), extend Redis TTL, store full payload
#
# Order matters: save_score overwrites the cached phase, so detect_phase_transition
# must run first or a transition is never seen.

import json
import math
from datetime import datetime

import numpy as np

from pipeline.scoring import config as C
from pipeline.scoring.feature_groups import build_group_panels, exhaustion_flags
from pipeline.scoring.lifecycle_tracker import classify_phase


def _clean(o):
    """Make a payload JSON-safe (numpy types, NaN/inf -> None)."""
    if isinstance(o, dict):
        return {str(k): _clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_clean(v) for v in o]
    if isinstance(o, (np.floating, float)):
        f = float(o)
        return None if (math.isnan(f) or math.isinf(f)) else f
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    if isinstance(o, datetime):
        return o.isoformat()
    return o


def is_market_open_et(now=None):
    """True/False during/outside regular US hours (ignores holidays);
    None if timezone data is unavailable (pip install tzdata on Windows)."""
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo('America/New_York')
        t = (now.astimezone(et) if now else datetime.now(et))
    except Exception:
        return None
    if t.weekday() >= 5:
        return False
    mins = t.hour * 60 + t.minute
    return 9 * 60 + 30 <= mins < 16 * 60


def summarize(p):
    parts = [f"Phase: {p['phase_label']} ({'; '.join(p['lifecycle']['reasons']) or 'default'})."]
    gs = p['group_scores']
    hot = [g for g, s in gs.items() if s is not None and s >= C.GROUP_HOT]
    parts.append(f"Score {p['squeeze_score']:.0f}/100 = equal-weight average of "
                 f"{p['confidence']['groups_with_data']} of {p['confidence']['groups_total']} groups"
                 + (f"; hot: {', '.join(hot)}." if hot else '; none hot.'))
    ig = p['models'].get('ignition', {})
    if ig.get('applicable') and 'p_continue' in ig:
        parts.append(f"Ignition model: {ig['p_continue']:.0%} chance of a further 30% run in 10 days "
                     f"vs {ig['base_rate']:.0%} typical ({ig['band']}).")
    ex = p['models'].get('exhaustion', {})
    if ex.get('applicable') and 'p_drop' in ex:
        parts.append(f"Fade model: {ex['p_drop']:.0%} chance of a 20% drop in 5 days "
                     f"vs {ex['base_rate']:.0%} typical ({ex['band']}).")
    if p['exhaustion_flags']['count']:
        parts.append('Exhaustion flags: ' + ', '.join(f['flag'] for f in p['exhaustion_flags']['fired']) + '.')
    if p.get('provisional'):
        parts.append('Provisional: US market is open, models were trained on daily closes.')
    return ' '.join(parts)


class SqueezeScorer:
    def __init__(self, assembler=None, state=None, router=None, ctx=None, store=None):
        self._assembler, self._state = assembler, state
        self._router, self._ctx, self._store = router, ctx, store

    # lazy construction so importing this module needs no database
    @property
    def assembler(self):
        if self._assembler is None:
            from pipeline.engineering.feature_assembler import FeatureAssembler
            self._assembler = FeatureAssembler()
        return self._assembler

    @property
    def state(self):
        if self._state is None:
            from pipeline.state.stock_state_manager import StockStateManager
            self._state = StockStateManager()
        return self._state

    @property
    def router(self):
        if self._router is None:
            from pipeline.scoring.model_router import ModelRouter
            self._router = ModelRouter()
        return self._router

    @property
    def ctx(self):
        if self._ctx is None:
            from pipeline.scoring.market_context import MarketContextReader
            self._ctx = MarketContextReader(pg=self.state.pg)
        return self._ctx

    @property
    def store(self):
        if self._store is None:
            from pipeline.scoring.store import PayloadStore
            self._store = PayloadStore(self.state.pg)
        return self._store

    # ── one scoring cycle (no writes) ────────────────────────
    def score(self, ticker: str) -> dict:
        t = ticker.upper().strip()
        a = self.assembler.assemble(t)
        raw, norm = a['raw'], a['normalized']

        try:
            fresh = self.state.get_data_freshness(t)
        except Exception:
            fresh = {}

        panels = build_group_panels(
            raw, norm, a.get('missing_features', []), fresh,
            a.get('news_nlp_available', True), a.get('quality_score'))
        flags = exhaustion_flags(raw, panels['availability'])

        bars = self.ctx.load_daily_bars(t)
        context = self.ctx.build_context(bars)
        models = self.router.route(norm, context)
        status = {g['id']: g['status'] for g in panels['groups']}
        life = classify_phase(bars, status, flags)

        payload = {
            'ticker': t,
            'scored_at': datetime.utcnow().isoformat(),
            'provisional': is_market_open_et(),
            'squeeze_score': panels['squeeze_score'],
            'score_kind': 'equal_weight_group_average (heuristic, not a probability)',
            'group_scores': panels['group_scores'],
            'coverage': panels['coverage'],
            'confidence': panels['confidence'],
            'groups': panels['groups'],
            'top_drivers': panels['top_drivers'],
            'exhaustion_flags': flags,
            'models': models,
            'lifecycle': life,
            'phase_label': life['phase'],
            'market_context': context,
            'quality_score': a.get('quality_score'),
            'missing_features': a.get('missing_features', []),
            'news_nlp_available': a.get('news_nlp_available'),
            'news_article_count': a.get('news_article_count'),
            'si_proxy_direction': a.get('si_proxy_direction'),
            'model_version': C.MODEL_VERSION,
            # keys the existing save_score / stock_scores insert reads
            'shap_top_drivers': panels['top_drivers'],
            'phase_probabilities': {},
            'hype_acceleration': None, 'mention_velocity': None,
            'sentiment_score': raw.get('news_sentiment_score'),
            'volume_ratio': raw.get('volume_ratio_30d'),
            'borrow_rate': raw.get('borrow_rate'),
            'call_put_ratio': raw.get('call_put_ratio'),
        }
        payload['explanation_text'] = summarize(payload)
        return _clean(payload)

    # ── score + persist ──────────────────────────────────────
    def score_and_store(self, ticker: str) -> dict:
        p = self.score(ticker)
        t = p['ticker']
        # A stock with no stored phase has no baseline: record it, do not alert.
        # (The state manager would otherwise treat 'no phase' as 'Early Setup'.)
        try:
            has_baseline = (self.state.redis.get(f'phase:{t}') is not None
                            or self.state.get_last_score(t) is not None)
        except Exception:
            has_baseline = True
        if has_baseline:
            changed = self.state.detect_phase_transition(t, p['phase_label'])   # FIRST
        else:
            changed = False
            self.state.redis.set(f'phase:{t}', p['phase_label'])
        self.state.save_score(t, p)                                         # THEN save
        try:                       # keep scores alive between runs (default TTL is 300s)
            r = self.state.redis
            r.expire(f'score:{t}', C.REDIS_TTL_S)
            r.expire(f'phase:{t}', C.REDIS_TTL_S)
            if changed:
                r.publish('alerts:phase_transitions', json.dumps(
                    {'ticker': t, 'phase': p['phase_label'], 'at': p['scored_at']}))
        except Exception:
            pass
        self.store.save(t, p)
        ran = [k for k in ('ignition', 'exhaustion', 'volatility')
               if p['models'].get(k, {}).get('applicable')]
        return {'ticker': t, 'squeeze_score': p['squeeze_score'],
                'phase': p['phase_label'], 'phase_changed': bool(changed),
                'models_run': ran, 'eligible': p['models'].get('eligible'),
                'bar_age_days': p['market_context'].get('bar_age_days'),
                'confidence': p['confidence']['level']}


# ── terminal view (until the dashboard exists) ───────────────
def format_text(p: dict) -> str:
    L = [f"{'=' * 70}", f"{p['ticker']}  squeeze score {p['squeeze_score']:.0f}/100  "
         f"phase: {p['phase_label']}  confidence: {p['confidence']['level']}"
         + ('  [PROVISIONAL]' if p.get('provisional') else ''), '=' * 70,
         p['explanation_text'], '']
    for g in p['groups']:
        sc = 'n/a' if g['score'] is None else f"{g['score']:.0f}"
        L.append(f"[{g['title']}]  score {sc}  ({g['status']})  - {g['question']}")
        for f in g['features']:
            extra = f"  <{f['note']}>" if f['note'] else ''
            L.append(f"    {f['label']:<46} {f['display']:>12}   {f['status']}{extra}")
        L.append('')
    fl = p['exhaustion_flags']
    L.append(f"[Exhaustion flags] {fl['count']} active")
    for f in fl['fired']:
        L.append(f"    ! {f['text']}")
    L.append('')
    m = p['models']
    L.append(f"[ML models]  eligible: {m['eligible']} ({m['eligibility_reason']})")
    for k in ('ignition', 'exhaustion', 'volatility'):
        v = m.get(k, {})
        if not v.get('applicable'):
            L.append(f"    {k:<11} not run: {v.get('reason')}")
        elif 'error' in v:
            L.append(f"    {k:<11} ERROR {v['error']}")
        elif k == 'volatility':
            L.append(f"    {k:<11} daily vol p10/p50/p90 = {v['daily_vol_p10']:.1%} / "
                     f"{v['daily_vol_p50']:.1%} / {v['daily_vol_p90']:.1%}; "
                     f"10-day 1-sigma move {v['expected_10d_move_1sigma']:.0%}; "
                     f"30% = {v['sigmas_to_30pct']} sigma")
        else:
            prob = v.get('p_continue', v.get('p_drop'))
            L.append(f"    {k:<11} {prob:.1%} vs typical {v['base_rate']:.1%} "
                     f"(lift {v['lift']}x, {v['band']}); n_train={v['n_train']}")
    return '\n'.join(L)