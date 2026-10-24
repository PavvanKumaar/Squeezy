# pipeline/scoring/lifecycle_tracker.py
#
# Rule-based lifecycle phase (replaces the trained phase classifier).
# Emits the same five labels the state manager's polling tables use:
#   Early Setup | Heating Up | Squeeze Zone | Danger Zone | Cooldown
#
# Stateless: recomputed from the last daily bars every run.
# Heuristic rules, not validated. Before an ignition the phase is a WATCHLIST
# state from group scores; after an ignition it follows the price path.

import numpy as np

from pipeline.scoring.config import PHASE as P


def _series(closes, vols):
    n = len(closes)
    vr = np.full(n, np.nan)
    p1 = np.full(n, np.nan)
    p5 = np.full(n, np.nan)
    for i in range(n):
        if i >= 20:
            prior = vols[max(0, i - 30):i]
            m = prior.mean()
            if m > 0:
                vr[i] = vols[i] / m
        if i >= 1:
            p1[i] = closes[i] / closes[i - 1] - 1
        if i >= 5:
            p5[i] = closes[i] / closes[i - 5] - 1
    return vr, p1, p5


def classify_phase(bars: list, group_status: dict, exhaustion: dict) -> dict:
    res = {'phase': 'Early Setup', 'reasons': [], 'ignition_date': None,
           'days_since_ignition': None, 'gain_since_ignition': None,
           'drawdown_from_high': None, 'days_since_peak': None}
    n = len(bars)
    if n < P['min_bars']:
        res['reasons'].append(f'only {n} daily bars (need {P["min_bars"]}) - cannot detect an ignition')
        return res

    closes = np.array([b['close'] for b in bars], dtype=float)
    vols = np.array([b['volume'] for b in bars], dtype=float)
    vr, p1, p5 = _series(closes, vols)

    def is_ign(i):
        return (vr[i] >= P['ign_volratio'] and
                (p1[i] >= P['ign_1d'] or p5[i] >= P['ign_5d']))

    first = max(20, n - P['ign_lookback'])
    ign = next((i for i in range(n - 1, first - 1, -1) if is_ign(i)), None)

    if ign is None:
        hot = [g for g in ('fuel', 'short_stress', 'attention', 'gamma')
               if group_status.get(g) == 'hot']
        mom = group_status.get('momentum')
        if mom in ('warm', 'hot') or len(hot) >= 2:
            res['phase'] = 'Heating Up'
            if mom in ('warm', 'hot'):
                res['reasons'].append('momentum group is building (no ignition yet)')
            if len(hot) >= 2:
                res['reasons'].append('two or more of fuel/shorts/attention/options are hot: ' + ', '.join(hot))
        else:
            res['reasons'].append('no ignition in the last %d days and no building pressure' % P['ign_lookback'])
        return res

    # extend back to the start of the ignition cluster
    changed = True
    while changed:
        changed = False
        for j in range(max(first, ign - P['cluster_gap']), ign):
            if is_ign(j):
                ign, changed = j, True
                break

    seg = closes[ign:]
    peak = ign + int(np.argmax(seg))
    high = float(seg.max())
    dd = closes[-1] / high - 1
    gain = closes[-1] / closes[ign] - 1
    since_peak = n - 1 - peak
    res.update({'ignition_date': str(bars[ign]['date']), 'days_since_ignition': n - 1 - ign,
                'gain_since_ignition': round(float(gain), 4),
                'drawdown_from_high': round(float(dd), 4), 'days_since_peak': since_peak})
    flags = exhaustion.get('count', 0)

    if (dd <= P['cooldown_drawdown'] or since_peak > P['cooldown_peak_days'] or
            closes[-1] < closes[ign] * P['below_ignition_close']):
        res['phase'] = 'Cooldown'
        res['reasons'].append(f'{dd:.0%} below the post-ignition high, {since_peak} days since the high')
    elif ((since_peak <= P['danger_peak_days'] and dd <= P['danger_drawdown']) or
          (flags >= 2 and dd >= P['danger_drawdown'])):
        res['phase'] = 'Danger Zone'
        if dd <= P['danger_drawdown']:
            res['reasons'].append(f'pulling back {dd:.0%} from the high within {since_peak} days')
        if flags >= 2:
            res['reasons'].append(f'{flags} exhaustion flags active')
    else:
        res['phase'] = 'Squeeze Zone'
        res['reasons'].append(f'ignited {n - 1 - ign} days ago, {gain:+.0%} since, within {abs(dd):.0%} of the high')
    return res
