# pipeline/orchestration/run_pipeline.py
#
# One command that runs the whole pipeline, now or on a schedule:
#
#   screen   : universe screener (UNTRACKED -> CANDIDATE, failed -> COOLING)
#   collect  : every collector, for CANDIDATE + ACTIVE stocks
#   evaluate : CandidateEvaluator gates (CANDIDATE -> ACTIVE)
#   score    : grouped panels + ML models + phase, saved for each ACTIVE stock
#
# No Celery, no Kafka: plain function calls, failures isolated per collector/ticker.
#
#   python -m pipeline.orchestration.run_pipeline --list        # what is bound to what
#   python -m pipeline.orchestration.run_pipeline --check       # instantiate everything
#   python -m pipeline.orchestration.run_pipeline --now         # run all stages once
#   python -m pipeline.orchestration.run_pipeline --now --skip-screen
#   python -m pipeline.orchestration.run_pipeline --now --stages collect,score --tickers GME AMC
#   python -m pipeline.orchestration.run_pipeline --seed GME AMC --activate GME   # test setup
#   python -m pipeline.orchestration.run_pipeline --daily --at 09:31 --at 16:30   # scheduled (ET)

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

from pipeline.orchestration import discovery as D

# Quiet known harmless noise from third-party libraries (set before they are imported).
import warnings
warnings.filterwarnings('ignore', category=FutureWarning, module=r'pytrends.*')
os.environ.setdefault('TRANSFORMERS_VERBOSITY', 'error')

STAGES = ['screen', 'collect', 'evaluate', 'score']
# Seconds between collections, used when the state manager has no should_collect_X.
SOURCE_INTERVALS = {'yfinance': 900, 'stockanalysis': 86400, 'finra': 86400,
                    'sec_edgar': 86400, 'stocktwits': 1800, 'pytrends': 3600}
LOCK_MAX_AGE_S = 3 * 3600
LOG_DIR = D.REPO / 'logs'


def log(msg):
    print(f'[{datetime.now():%H:%M:%S}] {msg}', flush=True)


def _looks_failed(res):
    if isinstance(res, dict):
        if res.get('success') is False:
            return True
        if str(res.get('status', '')).lower() in ('failed', 'error'):
            return True
    return False


def _rollback(inst):
    """After a collector error, clear any aborted psycopg2 transaction."""
    for attr in ('pg', 'conn', 'connection', 'db'):
        obj = getattr(inst, attr, None)
        if hasattr(obj, 'rollback'):
            try:
                obj.rollback()
            except Exception:
                pass


# ── scheduling helpers ───────────────────────────────────────
def parse_hhmm(s):
    h, m = s.split(':')
    return int(h), int(m)


def next_run(now_et, times, weekdays_only=True):
    """First datetime after now_et matching one of the (hour, minute) pairs."""
    for off in range(0, 9):
        d = now_et.date() + timedelta(days=off)
        if weekdays_only and d.weekday() >= 5:
            continue
        for h, m in sorted(times):
            cand = datetime(d.year, d.month, d.day, h, m, tzinfo=now_et.tzinfo)
            if cand > now_et:
                return cand
    return None


def sleep_until(target, tick=30):
    last_print = 0
    while True:
        rem = (target - datetime.now(target.tzinfo)).total_seconds()
        if rem <= 0:
            return
        if time.time() - last_print > 600:
            log(f'waiting {rem / 60:.0f} min until {target:%a %H:%M} ET')
            last_print = time.time()
        time.sleep(min(rem, tick))


class Lock:
    def __init__(self, path):
        self.path = Path(path)

    def acquire(self, ignore=False):
        if self.path.exists() and not ignore:
            age = time.time() - self.path.stat().st_mtime
            if age < LOCK_MAX_AGE_S:
                return False
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(f'{os.getpid()} {datetime.now().isoformat()}')
        return True

    def release(self):
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


class Pipeline:
    def __init__(self, state=None, evaluator_factory=None, scorer=None,
                 collectors=None, screener=None):
        self._state, self._evaluator_factory, self._scorer = state, evaluator_factory, scorer
        self._collectors, self._screener = collectors, screener

    # lazy so --list works without any database
    @property
    def state(self):
        if self._state is None:
            from pipeline.state.stock_state_manager import StockStateManager
            self._state = StockStateManager()
        return self._state

    @property
    def collectors(self):
        if self._collectors is None:
            self._collectors = D.bind_collectors()
        return self._collectors

    @property
    def screener(self):
        if self._screener is None:
            self._screener = D.bind_screener()
        return self._screener

    @property
    def scorer(self):
        if self._scorer is None:
            from pipeline.scoring.scorer import SqueezeScorer
            self._scorer = SqueezeScorer(state=self.state)
        return self._scorer

    def evaluator(self):
        if self._evaluator_factory:
            return self._evaluator_factory()
        from pipeline.state.candidate_evaluator import CandidateEvaluator
        return CandidateEvaluator()

    # ── stage: screen ────────────────────────────────────────
    def _screen(self):
        b = self.screener
        if not b.ok:
            raise RuntimeError(f'screener not bound: {b.describe()}')
        res = b.bound()()
        n = len(res) if hasattr(res, '__len__') else None
        return {'result_size': n, 'result_type': type(res).__name__}

    # ── stage: collect ───────────────────────────────────────
    def _due(self, key, ticker):
        try:
            s = self.state
            if key == 'options':
                return s.should_collect_options(ticker)
            if key == 'news':
                return s.should_collect_news(ticker)
            if key == 'iborrowdesk':
                return s.should_collect_borrow_rate(ticker)
            return s._should_collect(ticker, key, SOURCE_INTERVALS.get(key, 900))
        except Exception:
            return True

    def _collect_source(self, key, binding, tickers, force):
        st = {'ok': 0, 'failed': 0, 'skipped': 0, 'errors': [], 'seconds': 0.0}
        t0 = time.time()
        if not binding.ok:
            st['failed'] = len(tickers)
            st['errors'].append(binding.describe())
            return st
        try:
            fn = binding.bound()
        except Exception as e:
            st['failed'] = len(tickers)
            st['errors'].append(f'could not create collector: {type(e).__name__}: {e}')
            return st
        for t in tickers:
            if not force and not self._due(key, t):
                st['skipped'] += 1
                continue
            try:
                res = fn(t)
                if _looks_failed(res):
                    raise RuntimeError(f'collector reported failure: {str(res)[:120]}')
                self.state.mark_collected(t, key)
                st['ok'] += 1
            except Exception as e:
                st['failed'] += 1
                _rollback(binding._inst)
                if len(st['errors']) < 5:
                    st['errors'].append(f'{t}: {type(e).__name__}: {e}')
        st['seconds'] = round(time.time() - t0, 1)
        return st

    def _collect(self, tickers, force, parallel):
        out = {}
        items = list(self.collectors.items())
        if parallel:
            # one thread per SOURCE (each collector instance stays single-threaded)
            with ThreadPoolExecutor(max_workers=max(len(items), 1)) as ex:
                futs = {k: ex.submit(self._collect_source, k, b, tickers, force)
                        for k, b in items}
                out = {k: f.result() for k, f in futs.items()}
        else:
            for k, b in items:
                log(f'  collecting: {k}')
                out[k] = self._collect_source(k, b, tickers, force)
        return out

    # ── stage: evaluate ──────────────────────────────────────
    def _evaluate(self):
        ev = self.evaluator()
        try:
            s = ev.evaluate_all_candidates()
            return {k: s.get(k) for k in ('candidates_evaluated', 'promoted', 'demoted',
                                          'staying', 'promoted_tickers', 'demoted_tickers')}
        finally:
            if hasattr(ev, 'close'):
                try:
                    ev.close()
                except Exception:
                    pass

    # ── stage: score ─────────────────────────────────────────
    def _score(self, tickers, store=True):
        rows, errors = [], []
        for t in tickers:
            try:
                rows.append(self.scorer.score_and_store(t) if store
                            else {'ticker': t, **self.scorer.score(t)})
                r = rows[-1]
                log(f"  {r['ticker']}: score {r['squeeze_score']:.0f} phase {r['phase_label'] if 'phase_label' in r else r['phase']}")
            except Exception as e:
                errors.append(f'{t}: {type(e).__name__}: {e}')
                log(f'  ! {t}: {type(e).__name__}: {e}')
        key = (lambda r: r.get('phase') or r.get('phase_label'))
        phases = {}
        for r in rows:
            phases[key(r)] = phases.get(key(r), 0) + 1
        models_run = {}
        for r in rows:
            for m in r.get('models_run', []):
                models_run[m] = models_run.get(m, 0) + 1
        return {'scored': len(rows), 'failed': len(errors), 'errors': errors[:5],
                'phases': phases, 'models_run': models_run,
                'models_ineligible': sum(1 for r in rows if r.get('eligible') is False),
                'stale_bars': sum(1 for r in rows if (r.get('bar_age_days') or 0) > 5),
                'low_confidence': sum(1 for r in rows if r.get('confidence') == 'low'),
                'phase_changes': [r['ticker'] for r in rows if r.get('phase_changed')]}

    # ── run ──────────────────────────────────────────────────
    def run(self, stages=STAGES, tickers=None, force=False, parallel=False, store=True):
        report = {'started': datetime.now().isoformat(timespec='seconds'), 'stages': {}}
        for stage in stages:
            t0 = time.time()
            log(f'== stage: {stage}')
            try:
                if stage == 'screen':
                    detail = self._screen()
                elif stage == 'collect':
                    tk = tickers or sorted(set(self.state.get_stocks_by_status('CANDIDATE')
                                               + self.state.get_stocks_by_status('ACTIVE')))
                    log(f'  {len(tk)} stocks, {len(self.collectors)} collectors')
                    detail = self._collect(tk, force, parallel)
                elif stage == 'evaluate':
                    detail = self._evaluate()
                elif stage == 'score':
                    tk = tickers or sorted(self.state.get_stocks_by_status('ACTIVE'))
                    log(f'  {len(tk)} ACTIVE stocks')
                    detail = self._score(tk, store)
                else:
                    raise ValueError(f'unknown stage {stage}')
                report['stages'][stage] = {'ok': True, 'seconds': round(time.time() - t0, 1),
                                           'detail': detail}
            except Exception as e:     # a failed stage never stops the later ones
                log(f'  ! stage {stage} failed: {type(e).__name__}: {e}')
                report['stages'][stage] = {'ok': False, 'seconds': round(time.time() - t0, 1),
                                           'error': f'{type(e).__name__}: {e}'}
        report['finished'] = datetime.now().isoformat(timespec='seconds')
        return report

    def close(self):
        for b in (self._collectors or {}).values():
            b.close()


# ── reporting ────────────────────────────────────────────────
def print_report(rep):
    print('\n' + '=' * 70)
    print(f"RUN {rep['started']} -> {rep['finished']}")
    for name, s in rep['stages'].items():
        print(f"  {'OK ' if s['ok'] else 'FAIL'} {name:<9} {s['seconds']:>7.1f}s")
        if not s['ok']:
            print(f"        {s['error']}")
            continue
        d = s['detail']
        if name == 'collect':
            for k, v in d.items():
                err = f"  e.g. {v['errors'][0]}" if v['errors'] else ''
                print(f"        {k:<14} ok {v['ok']:<4} failed {v['failed']:<4} skipped {v['skipped']:<4}{err}")
        else:
            print('        ' + json.dumps(d, default=str)[:400])
    print('=' * 70)


def save_report(rep):
    d = LOG_DIR / 'pipeline_runs'
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"run_{datetime.now():%Y%m%d_%H%M%S}.json"
    p.write_text(json.dumps(rep, indent=2, default=str))
    return p


def cmd_list(pl):
    print('\nScreener :', pl.screener.describe())
    print(f'Collectors ({len(pl.collectors)} found, disabled: {sorted(D.DISABLED)}):')
    for k, b in pl.collectors.items():
        print(f'  {k:<14} {b.describe()}')
    print('\nAn UNBOUND line means the naming convention missed it: add an entry to '
          'OVERRIDES in pipeline/orchestration/discovery.py.')


def cmd_check(pl):
    bad = 0
    for k, b in [('screener', pl.screener)] + list(pl.collectors.items()):
        if not b.ok:
            print(f'  FAIL {k:<14} {b.describe()}')
            bad += 1
            continue
        try:
            b.instance()
            print(f'  ok   {k:<14} {b.describe()}')
        except Exception as e:
            print(f'  FAIL {k:<14} constructor raised {type(e).__name__}: {e}')
            bad += 1
    print(f'\n{bad} problem(s)')


def cmd_seed(pl, seed, activate):
    cur = pl.state.pg.cursor()
    for t in [x.upper() for x in seed]:
        cur.execute("""INSERT INTO stocks (ticker, status, promoted_at)
                       VALUES (%s, 'CANDIDATE', NOW()) ON CONFLICT (ticker) DO NOTHING""", (t,))
    pl.state.pg.commit()
    for t in [x.upper() for x in activate]:
        cur.execute("""INSERT INTO stocks (ticker, status) VALUES (%s, 'CANDIDATE')
                       ON CONFLICT (ticker) DO NOTHING""", (t,))
        pl.state.pg.commit()
        pl.state.transition_status(t, 'ACTIVE', 'manual_activation')
    log(f'seeded {seed}; activated {activate}')


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument('--list', action='store_true')
    ap.add_argument('--check', action='store_true')
    ap.add_argument('--now', action='store_true')
    ap.add_argument('--daily', action='store_true', help='loop on weekdays at --at times (ET)')
    ap.add_argument('--at', action='append', default=[], help='HH:MM Eastern, repeatable')
    ap.add_argument('--stages', default=','.join(STAGES))
    ap.add_argument('--skip-screen', action='store_true')
    ap.add_argument('--tickers', nargs='*')
    ap.add_argument('--seed', nargs='*', default=[])
    ap.add_argument('--activate', nargs='*', default=[])
    ap.add_argument('--force', action='store_true', help='ignore collection intervals')
    ap.add_argument('--parallel', action='store_true', help='one thread per collector')
    ap.add_argument('--no-store', action='store_true')
    ap.add_argument('--ignore-lock', action='store_true')
    a = ap.parse_args(argv)

    pl = Pipeline()
    if a.list:
        return cmd_list(pl)
    if a.check:
        return cmd_check(pl)
    if a.seed or a.activate:
        cmd_seed(pl, a.seed, a.activate)
        if not (a.now or a.daily or a.at):
            return

    stages = [s for s in a.stages.split(',') if s]
    if a.skip_screen:
        stages = [s for s in stages if s != 'screen']
    bad = [s for s in stages if s not in STAGES]
    if bad:
        ap.error(f'unknown stages {bad}; choose from {STAGES}')
    tickers = [t.upper() for t in a.tickers] if a.tickers else None

    def one_run():
        lock = Lock(LOG_DIR / 'pipeline.lock')
        if not lock.acquire(a.ignore_lock):
            log('another run is in progress (lock file < 3h old); use --ignore-lock to override')
            return
        try:
            rep = pl.run(stages, tickers, a.force, a.parallel, not a.no_store)
            print_report(rep)
            log(f'report saved: {save_report(rep)}')
        finally:
            lock.release()

    if a.now or not (a.daily or a.at):
        if not a.now:
            ap.error('choose --now, --daily/--at, --list or --check')
        return one_run()

    times = [parse_hhmm(x) for x in (a.at or ['09:31'])]
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo('America/New_York')
    except Exception:
        sys.exit('Timezone data missing. Run: pip install tzdata')
    while True:
        nxt = next_run(datetime.now(tz), times)
        log(f'next run: {nxt:%a %Y-%m-%d %H:%M} ET')
        sleep_until(nxt)
        one_run()
        if not a.daily:
            break


if __name__ == '__main__':
    main()