# tests/test_orchestration.py  -  run: python tests/test_orchestration.py
import sys
import tempfile
import textwrap
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.orchestration import discovery as D                   # noqa: E402
from pipeline.orchestration.run_pipeline import (Lock, Pipeline, next_run,   # noqa: E402
                                                 parse_hhmm)

ET = ZoneInfo('America/New_York')


def write(base, rel, code):
    p = Path(base) / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(textwrap.dedent(code))


def test_discovery_by_convention():
    with tempfile.TemporaryDirectory() as d:
        sys.path.insert(0, d)
        write(d, 'pipeline/zz/yfinance_collector.py', """
            class YFinanceCollector:
                def collect(self, ticker): return {'ok': ticker}
        """)
        write(d, 'pipeline/zz/news_collector.py', """
            class NewsCollector:
                def run(self, ticker, hours=48): return 1
        """)
        write(d, 'pipeline/zz/odd_collector.py', """
            class OddCollector:
                def go(self, ticker): return 1
        """)
        write(d, 'pipeline/zz/reddit_collector.py', """
            class RedditCollector:
                def collect(self, ticker): return 1
        """)
        write(d, 'pipeline/zz/universe_screener.py', """
            class UniverseScreener:
                def run(self): return ['A', 'B']
        """)
        c = D.bind_collectors(base=d)
        assert 'reddit' not in c                              # disabled
        assert c['yfinance'].ok and c['yfinance'].method == 'collect'
        assert c['news'].ok and c['news'].method == 'run'
        assert not c['odd'].ok and 'go' in c['odd'].methods   # reported, not guessed
        assert list(c)[0] == 'yfinance'                       # ordered
        s = D.bind_screener(base=d)
        assert s.ok and s.method == 'run' and s.bound()() == ['A', 'B']
        sys.path.remove(d)


class FakeState:
    def __init__(self):
        self.marked, self.due = [], {}
    def get_stocks_by_status(self, s):
        return {'CANDIDATE': ['AAA'], 'ACTIVE': ['BBB', 'CCC']}[s]
    def should_collect_options(self, t): return self.due.get(('options', t), True)
    def should_collect_news(self, t): return True
    def should_collect_borrow_rate(self, t): return True
    def _should_collect(self, t, k, i): return self.due.get((k, t), True)
    def mark_collected(self, t, k): self.marked.append((t, k))


class FakeBinding:
    def __init__(self, fn, ok=True):
        self._fn, self._ok, self._inst = fn, ok, object()
    ok = property(lambda s: s._ok)
    def bound(self): return self._fn
    def describe(self): return 'fake'
    def close(self): pass


def test_collect_isolation_gating_force():
    st = FakeState()
    calls = []

    def good(t): calls.append(('good', t)); return {'success': True}

    def flaky(t):
        calls.append(('flaky', t))
        if t == 'BBB':
            raise RuntimeError('boom')
        return {'ok': 1}

    def reported_failure(t): return {'success': False}

    cols = {'good': FakeBinding(good), 'flaky': FakeBinding(flaky),
            'bad': FakeBinding(reported_failure), 'unbound': FakeBinding(None, ok=False)}
    pl = Pipeline(state=st, collectors=cols)
    r = pl._collect(['AAA', 'BBB', 'CCC'], force=False, parallel=False)
    assert r['good']['ok'] == 3
    assert r['flaky']['ok'] == 2 and r['flaky']['failed'] == 1       # one failure, others continue
    assert r['bad']['failed'] == 3 and r['unbound']['failed'] == 3
    assert ('BBB', 'flaky') not in st.marked and ('AAA', 'good') in st.marked
    st.due[('good', 'AAA')] = False                                   # not due -> skipped
    r2 = pl._collect(['AAA'], force=False, parallel=False)
    assert r2['good']['skipped'] == 1 and r2['good']['ok'] == 0
    r3 = pl._collect(['AAA'], force=True, parallel=True)              # force + threads
    assert r3['good']['ok'] == 1


class FakeScorer:
    def score_and_store(self, t):
        if t == 'CCC':
            raise ValueError('bad data')
        return {'ticker': t, 'squeeze_score': 40.0, 'phase': 'Heating Up',
                'phase_changed': True, 'eligible': False, 'bar_age_days': 9,
                'confidence': 'low', 'models_run': []}


class FakeEval:
    def evaluate_all_candidates(self):
        return {'candidates_evaluated': 1, 'promoted': 1, 'demoted': 0, 'staying': 0,
                'promoted_tickers': ['AAA'], 'demoted_tickers': []}
    def close(self): pass


def test_run_stages_and_failure_isolation():
    class Unbound:
        ok = False
        def describe(self): return 'UNBOUND - nothing found'
    pl = Pipeline(state=FakeState(), scorer=FakeScorer(), evaluator_factory=FakeEval,
                  collectors={'good': FakeBinding(lambda t: {'success': True})},
                  screener=Unbound())
    rep = pl.run(['screen', 'collect', 'evaluate', 'score'])
    assert rep['stages']['screen']['ok'] is False                    # screener failed ...
    assert rep['stages']['collect']['ok'] and rep['stages']['evaluate']['ok']   # ... rest ran
    sc = rep['stages']['score']['detail']
    assert sc['scored'] == 1 and sc['failed'] == 1                    # BBB ok, CCC failed
    assert sc['models_ineligible'] == 1 and sc['stale_bars'] == 1 and sc['low_confidence'] == 1
    assert sc['phase_changes'] == ['BBB']


def test_next_run_schedule():
    times = [parse_hhmm('09:31'), parse_hhmm('16:30')]
    fri_am = datetime(2026, 10, 23, 8, 0, tzinfo=ET)
    assert next_run(fri_am, times) == datetime(2026, 10, 23, 9, 31, tzinfo=ET)
    fri_noon = datetime(2026, 10, 23, 12, 0, tzinfo=ET)
    assert next_run(fri_noon, times) == datetime(2026, 10, 23, 16, 30, tzinfo=ET)
    fri_eve = datetime(2026, 10, 23, 17, 0, tzinfo=ET)
    assert next_run(fri_eve, times) == datetime(2026, 10, 26, 9, 31, tzinfo=ET)   # skips weekend
    sat = datetime(2026, 10, 24, 9, 0, tzinfo=ET)
    assert next_run(sat, times).weekday() == 0


def test_lock():
    with tempfile.TemporaryDirectory() as d:
        lk = Lock(Path(d) / 'x.lock')
        assert lk.acquire()
        assert not Lock(Path(d) / 'x.lock').acquire()
        assert Lock(Path(d) / 'x.lock').acquire(ignore=True)
        lk.release()
        assert Lock(Path(d) / 'x.lock').acquire()


if __name__ == '__main__':
    fns = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    for f in fns:
        f()
        print('PASS', f.__name__)
    print(f'\n{len(fns)} tests passed')