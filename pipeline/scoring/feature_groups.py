# pipeline/scoring/feature_groups.py
#
# Turns the assembled 28-feature output into grouped, human-readable panels,
# per-group scores, and ONE equal-weight squeeze score.
#
# Inputs come straight from FeatureAssembler.assemble():
#   raw, normalized, missing_features, news_nlp_available, quality_score
# plus (optionally) StockStateManager.get_data_freshness(ticker).

from pipeline.scoring.config import (STATUS_POINTS, SCORED_GROUPS,
                                     GROUP_HOT, GROUP_WARM)

GROUPS = [
    ('momentum',     'Momentum & volatility', 'Is price/volume action already unusual?'),
    ('fuel',         'Fuel',                  'How much squeeze potential exists?'),
    ('short_stress', 'Shorts under stress',   'Are shorts starting to hurt?'),
    ('attention',    'Attention',             'Is the crowd arriving?'),
    ('catalyst',     'Catalyst',              'Is there a news or filing trigger?'),
    ('gamma',        'Options pressure',      'Is options flow amplifying moves?'),
    ('confidence',   'Data confidence',       'How much to trust the readings'),
]

# (feature, group, label, raw_key, unit, kind, elevated, high, source)
# kind: hi = higher is hotter | lo = lower is hotter | flag = 0/1 |
#       signedpos = only positive values count | info = shown, never scored
# Thresholds are heuristic display bands (raw units), NOT validated.
FEATURES = [
    ('volume_ratio_30d_norm', 'momentum', 'Volume vs 30-day average', 'volume_ratio_30d', 'x', 'hi', 2.0, 3.0, 'yfinance_ohlcv_1d'),
    ('price_change_1d', 'momentum', '1-day price change', 'price_change_1d', 'frac', 'hi', 0.05, 0.10, 'yfinance_ohlcv_1d'),
    ('price_change_5d', 'momentum', '5-day price change', 'price_change_5d', 'frac', 'hi', 0.10, 0.25, 'yfinance_ohlcv_1d'),
    ('rsi_14_norm', 'momentum', 'RSI (14)', 'rsi_14', 'num1', 'hi', 70, 80, 'yfinance_ohlcv_1d'),
    ('distance_from_52w_high_norm', 'momentum', 'Distance below 52-week high', 'distance_from_52w_high', 'frac', 'lo', 0.20, 0.05, 'yfinance_ohlcv_1d'),
    ('atr_norm', 'momentum', 'ATR as % of price', 'atr_norm', 'frac', 'hi', 0.06, 0.10, 'yfinance_ohlcv_1d'),
    ('volatility_10d_norm', 'momentum', '10-day volatility (daily std)', 'volatility_10d', 'frac', 'hi', 0.04, 0.08, 'yfinance_ohlcv_1d'),

    ('short_interest_pct_norm', 'fuel', 'Short interest (% of float)', 'short_interest_pct', 'pct', 'hi', 15, 30, 'yfinance_info'),
    ('days_to_cover_norm', 'fuel', 'Days to cover', 'days_to_cover', 'days', 'hi', 3, 5, 'yfinance_info'),
    ('borrow_rate_norm', 'fuel', 'Borrow rate (annualized)', 'borrow_rate', 'pct', 'hi', 10, 25, 'iborrowdesk'),
    ('float_rotation_norm', 'fuel', 'Float rotation (volume / float)', 'float_rotation', 'x', 'hi', 0.3, 1.0, 'yfinance_info'),

    ('borrow_rate_delta_norm', 'short_stress', 'Borrow rate change', 'borrow_rate_delta', 'pts', 'hi', 5, 15, 'iborrowdesk'),
    ('short_volume_ratio', 'short_stress', 'Short volume ratio', 'short_volume_ratio', 'frac', 'hi', 0.5, 0.6, 'finra'),

    ('stocktwits_message_count_norm', 'attention', 'StockTwits message count', 'stocktwits_message_count', 'int', 'hi', 50, 100, 'stocktwits'),
    ('stocktwits_bull_ratio', 'attention', 'StockTwits bullish share', 'stocktwits_bull_ratio', 'frac', 'hi', 0.60, 0.75, 'stocktwits'),
    ('google_trend_score_norm', 'attention', 'Google Trends score', 'google_trend_score', 'num0', 'hi', 40, 70, 'pytrends'),
    ('trend_spike_flag', 'attention', 'Google Trends spike', 'trend_spike_flag', 'flag', 'flag', None, None, 'pytrends'),

    ('news_catalyst_flag', 'catalyst', 'Catalyst event (last 48h)', 'news_catalyst_flag', 'flag', 'flag', None, None, 'news'),
    ('has_activist_filing', 'catalyst', 'Activist filing (last 30d)', 'has_activist_filing', 'flag', 'flag', None, None, 'sec_13d_13g'),
    ('news_sentiment_score', 'catalyst', 'News sentiment (-1 to 1)', 'news_sentiment_score', 'signed', 'signedpos', 0.3, 0.6, 'news'),
    ('news_squeeze_keyword_score', 'catalyst', 'Squeeze language in news', 'news_squeeze_keyword_score', 'score', 'hi', 0.3, 0.6, 'news'),
    ('news_catalyst_composite', 'catalyst', 'Catalyst composite', 'news_catalyst_composite', 'score', 'hi', 0.3, 0.6, 'news'),

    ('call_put_ratio_norm', 'gamma', 'Call/put ratio', 'call_put_ratio', 'num2', 'hi', 1.5, 2.5, 'options_yfinance'),
    ('iv_rank_norm', 'gamma', 'IV rank', 'iv_rank', 'num0', 'hi', 50, 80, 'options_yfinance'),
    ('unusual_sweep_flag', 'gamma', 'Unusual options sweep', 'unusual_sweep_flag', 'flag', 'flag', None, None, 'options_yfinance'),
    ('max_pain_distance_norm', 'gamma', 'Distance to max pain', 'max_pain_distance', 'signed', 'info', None, None, 'options_yfinance'),

    ('si_staleness_penalty', 'confidence', 'Short-interest staleness (0 fresh, 1 stale)', 'si_staleness_penalty', 'score', 'info', None, None, 'yfinance_info'),
    ('options_available', 'confidence', 'Options data available', 'options_available', 'flag', 'info', None, None, 'options_yfinance'),
]

# A normalized value of exactly 0.0 is legitimate for these (not "missing").
ZERO_VALID = {'si_staleness_penalty', 'price_change_1d', 'price_change_5d',
              'borrow_rate_delta_norm', 'max_pain_distance_norm'}
NEWS_NLP_FEATURES = {'news_sentiment_score', 'news_squeeze_keyword_score',
                     'news_catalyst_composite'}


def fmt(value, unit):
    if value is None:
        return 'n/a'
    v = float(value)
    return {
        'pct': f'{v:.1f}%', 'frac': f'{v * 100:.1f}%', 'x': f'{v:.2f}x',
        'days': f'{v:.1f} days', 'pts': f'{v:+.1f} pts', 'int': f'{int(v)}',
        'num0': f'{v:.0f}', 'num1': f'{v:.1f}', 'num2': f'{v:.2f}',
        'score': f'{v:.2f}', 'signed': f'{v:+.2f}',
        'flag': 'yes' if v >= 0.5 else 'no',
    }.get(unit, f'{v:.3f}')


def _status(kind, v, elevated, high):
    if kind == 'info':
        return 'info'
    if kind == 'flag':
        return 'high' if v >= 0.5 else 'normal'
    if kind == 'lo':
        return 'high' if v <= high else 'elevated' if v <= elevated else 'normal'
    if kind == 'signedpos':
        v = max(v, 0.0)
    return 'high' if v >= high else 'elevated' if v >= elevated else 'normal'


def _freshness_label(fr):
    if not fr:
        return 'unknown'
    if fr.get('last_collected') is None:
        return 'never'
    if fr.get('very_stale'):
        return 'stale'
    if fr.get('stale'):
        return 'aging'
    return 'fresh'


def build_group_panels(raw, normalized, missing_features=(), freshness=None,
                       news_nlp_available=True, quality_score=None):
    """Returns dict: groups, group_scores, squeeze_score, coverage, availability,
    top_drivers, confidence."""
    raw, normalized = raw or {}, normalized or {}
    missing = set(missing_features or ())
    freshness = freshness or {}
    options_ok = float(raw.get('options_available', 0) or 0) >= 0.5

    by_group = {g[0]: [] for g in GROUPS}
    availability = {}
    for (name, grp, label, rkey, unit, kind, elev, high, source) in FEATURES:
        rv = raw.get(rkey)
        rv = None if rv is None else float(rv)
        nv = normalized.get(name)
        fr = _freshness_label(freshness.get(source))

        available = rv is not None
        if name in missing and name not in ZERO_VALID:
            available = False
        if grp == 'gamma' and kind != 'info' and not options_ok:
            available = False
        if name in NEWS_NLP_FEATURES and not news_nlp_available:
            available = False
        if fr == 'never' and grp != 'confidence':
            available = False
        availability[name] = available

        status = 'unavailable'
        if available:
            status = _status(kind, rv, elev, high)
        note = {'aging': 'data is over a day old', 'stale': 'data is over a week old',
                'never': 'source has never been collected'}.get(fr, '')
        by_group[grp].append({
            'name': name, 'label': label, 'group': grp, 'source': source,
            'raw': rv, 'normalized': None if nv is None else float(nv),
            'display': fmt(rv, unit) if available else 'n/a',
            'status': status, 'available': available, 'freshness': fr,
            'scored': kind != 'info', 'note': note,
            'points': STATUS_POINTS.get(status, 0.0) if (available and kind != 'info') else None,
        })

    groups, group_scores = [], {}
    for gid, title, question in GROUPS:
        feats = by_group[gid]
        pts = [f['points'] for f in feats if f['points'] is not None]
        score = round(100.0 * sum(pts) / len(pts), 1) if pts and gid in SCORED_GROUPS else None
        if gid == 'confidence':
            gstatus = 'info'
        elif score is None:
            gstatus = 'no_data'
        else:
            gstatus = 'hot' if score >= GROUP_HOT else 'warm' if score >= GROUP_WARM else 'quiet'
        group_scores[gid] = score
        groups.append({'id': gid, 'title': title, 'question': question,
                       'score': score, 'status': gstatus,
                       'features_available': len(pts), 'features': feats})

    avail_scores = [s for g, s in group_scores.items() if g in SCORED_GROUPS and s is not None]
    squeeze_score = round(sum(avail_scores) / len(avail_scores), 1) if avail_scores else 0.0
    coverage = len(avail_scores) / len(SCORED_GROUPS)

    # confidence label (heuristic)
    q = (quality_score or 0.0) / 100.0
    level = ('high' if coverage >= 5 / 6 and q >= 0.7 else
             'medium' if coverage >= 3 / 6 and q >= 0.4 else 'low')

    drivers = sorted([f for g in groups for f in g['features']
                      if f['points']], key=lambda f: -f['points'])
    top = [{'feature': f['name'], 'label': f['label'], 'display': f['display'],
            'group': f['group'], 'status': f['status']} for f in drivers[:5]]

    return {
        'groups': groups, 'group_scores': group_scores,
        'squeeze_score': squeeze_score, 'coverage': round(coverage, 3),
        'confidence': {'level': level, 'coverage': round(coverage, 3),
                       'quality_score': quality_score,
                       'groups_with_data': len(avail_scores),
                       'groups_total': len(SCORED_GROUPS)},
        'availability': availability, 'top_drivers': top,
    }


def exhaustion_flags(raw, availability):
    """Rule-based late-stage warnings. Shown, never added to the score."""
    raw = raw or {}
    av = availability or {}

    def g(key):
        v = raw.get(key)
        return None if v is None else float(v)

    rules = [
        ('euphoric_crowd', 'StockTwits bullish share is extreme (>= 90%)',
         'stocktwits_bull_ratio', lambda: g('stocktwits_bull_ratio') >= 0.90),
        ('options_frenzy', 'IV rank is near its ceiling (>= 90)',
         'iv_rank_norm', lambda: g('iv_rank') >= 90),
        ('trend_peak', 'Google Trends at a spike peak (score >= 90)',
         'google_trend_score_norm', lambda: g('google_trend_score') >= 90 and
         float(raw.get('trend_spike_flag', 0) or 0) >= 0.5),
        ('borrow_easing', 'Borrow cost falling while price is up 25%+ in 5 days',
         'borrow_rate_delta_norm', lambda: g('borrow_rate_delta') <= -10 and
         (g('price_change_5d') or 0) >= 0.25),
        ('overbought', 'RSI is 80 or higher', 'rsi_14_norm',
         lambda: g('rsi_14') >= 80),
        ('parabolic', 'Price is up 50%+ in 5 days', 'price_change_5d',
         lambda: g('price_change_5d') >= 0.50),
        ('news_after_move', 'Catalyst news arrived after a 25%+ 5-day move',
         'news_catalyst_flag', lambda: float(raw.get('news_catalyst_flag', 0) or 0) >= 0.5
         and (g('price_change_5d') or 0) >= 0.25),
    ]
    fired = []
    for name, text, dep, test in rules:
        if not av.get(dep, True):
            continue
        try:
            if test():
                fired.append({'flag': name, 'text': text})
        except TypeError:       # a needed raw value was None
            continue
    return {'fired': fired, 'count': len(fired)}
