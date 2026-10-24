# pipeline/orchestration/discovery.py
#
# Finds your collectors and universe screener by NAMING CONVENTION, because the
# architecture doc defines them as `<source>_collector.py` modules that take a
# ticker. It never guesses silently: `run_pipeline --list` prints exactly what
# was bound, and OVERRIDES below fixes any mismatch in one line.
#
# OVERRIDES example (module/class/method for one source):
#   OVERRIDES = {'yfinance': {'module': 'pipeline.collectors.yfinance_collector',
#                             'class': 'YFinanceCollector', 'method': 'collect'},
#                'screener': {'module': 'pipeline.screener.universe_screener',
#                             'class': 'UniverseScreener', 'method': 'run'}}

import importlib
import inspect
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

COLLECT_METHODS = ['collect', 'collect_ticker', 'collect_one', 'collect_all',
                   'run', 'fetch', 'update']
SCREEN_METHODS = ['run', 'screen', 'run_screen', 'run_daily', 'run_daily_screen', 'execute']
DISABLED = {'reddit'}          # Reddit API access was not granted
ORDER = ['yfinance', 'stockanalysis', 'iborrowdesk', 'finra', 'sec_edgar',
         'stocktwits', 'pytrends', 'options', 'news']
OVERRIDES = {}


class Binding:
    def __init__(self, key, module=None, cls_name=None, method=None,
                 methods=(), error=None):
        self.key, self.module, self.cls_name, self.method = key, module, cls_name, method
        self.methods, self.error = list(methods), error
        self._inst = None

    @property
    def ok(self):
        return self.error is None and self.module and self.cls_name and self.method

    def instance(self):
        if self._inst is None:
            cls = getattr(importlib.import_module(self.module), self.cls_name)
            self._inst = cls()
        return self._inst

    def bound(self):
        return getattr(self.instance(), self.method)

    def close(self):
        if self._inst is not None and hasattr(self._inst, 'close'):
            try:
                self._inst.close()
            except Exception:
                pass

    def describe(self):
        if self.ok:
            return f'{self.module}.{self.cls_name}.{self.method}()'
        why = self.error or f'no usable method (public methods: {self.methods or "none"})'
        return f'UNBOUND - {why}'


def _modname(path, base):
    return '.'.join(path.relative_to(base).with_suffix('').parts)


def _takes_ticker(fn):
    try:
        params = list(inspect.signature(fn).parameters.values())[1:]   # drop self
    except (TypeError, ValueError):
        return False
    return any(p.name in ('ticker', 'symbol') for p in params) or any(
        p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD) and p.default is p.empty
        for p in params)


def _takes_nothing(fn):
    try:
        params = list(inspect.signature(fn).parameters.values())[1:]
    except (TypeError, ValueError):
        return False
    return not any(p.default is p.empty and p.kind in
                   (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD) for p in params)


def _inspect_module(key, modname, class_hint, method_order, arg_test, want_cls=None):
    try:
        mod = importlib.import_module(modname)
    except Exception as e:
        return Binding(key, error=f'import failed: {type(e).__name__}: {e}')
    classes = [c for _, c in inspect.getmembers(mod, inspect.isclass)
               if c.__module__ == modname]
    if want_cls:
        classes = [c for c in classes if c.__name__ == want_cls] or classes
    preferred = [c for c in classes if class_hint in c.__name__] or classes
    for cls in preferred:
        methods = [n for n, _ in inspect.getmembers(cls, inspect.isfunction)
                   if not n.startswith('_')]
        for m in method_order:
            if m in methods and arg_test(getattr(cls, m)):
                return Binding(key, modname, cls.__name__, m, methods)
        if preferred:
            last = Binding(key, modname, cls.__name__, None, methods)
    return last if preferred else Binding(key, error='no class found in module')


def _apply_override(key, kind):
    o = OVERRIDES.get(key)
    if not o:
        return None
    return Binding(key, o['module'], o['class'], o['method'])


def bind_collectors(base=None):
    base = Path(base or REPO)
    found = {}
    for path in sorted((base / 'pipeline').rglob('*_collector.py')):
        if '__pycache__' in path.parts:
            continue
        key = path.stem[:-len('_collector')]
        if key in DISABLED:
            continue
        found[key] = (_apply_override(key, 'collector') or
                      _inspect_module(key, _modname(path, base), 'Collector',
                                      COLLECT_METHODS, _takes_ticker))
    rank = {k: i for i, k in enumerate(ORDER)}
    return dict(sorted(found.items(), key=lambda kv: (rank.get(kv[0], 99), kv[0])))


def bind_screener(base=None):
    base = Path(base or REPO)
    ov = _apply_override('screener', 'screener')
    if ov:
        return ov
    for path in sorted((base / 'pipeline').rglob('*screener*.py')):
        if '__pycache__' in path.parts or path.stem.startswith('test'):
            continue
        b = _inspect_module('screener', _modname(path, base), 'Screener',
                            SCREEN_METHODS, _takes_nothing)
        if b.ok:
            return b
    return Binding('screener', error='no *screener*.py module with a Screener class '
                                      'and run()/screen() method found')