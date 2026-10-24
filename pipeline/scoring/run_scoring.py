# pipeline/scoring/run_scoring.py
#
# Runs scoring WITHOUT any Celery/Kafka wiring (zero changes to existing files).
#
#   python -m pipeline.scoring.run_scoring GME AMC          # print, no writes
#   python -m pipeline.scoring.run_scoring GME --store      # also save to Redis/DB
#   python -m pipeline.scoring.run_scoring --active         # all ACTIVE stocks once (stores)
#   python -m pipeline.scoring.run_scoring --active --loop --interval 900
#   add --json to print the raw payload instead of the readable view

import argparse
import json
import time

from pipeline.scoring.scorer import SqueezeScorer, format_text


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('tickers', nargs='*')
    ap.add_argument('--active', action='store_true', help='score every ACTIVE stock')
    ap.add_argument('--store', action='store_true', help='persist (default for --active)')
    ap.add_argument('--loop', action='store_true')
    ap.add_argument('--interval', type=int, default=900, help='seconds between loops')
    ap.add_argument('--json', action='store_true')
    a = ap.parse_args()
    if not a.tickers and not a.active:
        ap.error('give tickers or --active')

    scorer = SqueezeScorer()
    while True:
        tickers = list(a.tickers)
        if a.active:
            tickers += [t for t in scorer.state.get_stocks_by_status('ACTIVE')
                        if t not in tickers]
        persist = a.store or a.active
        for t in tickers:
            try:
                if persist:
                    s = scorer.score_and_store(t)
                    print(f"  {s['ticker']}: score {s['squeeze_score']:.0f} phase {s['phase']}"
                          f"{' (CHANGED)' if s['phase_changed'] else ''} models {s['models_run']}")
                else:
                    p = scorer.score(t)
                    print(json.dumps(p, indent=2) if a.json else format_text(p))
            except Exception as e:
                print(f'  ! {t}: {type(e).__name__}: {e}')
        if not a.loop:
            break
        time.sleep(a.interval)


if __name__ == '__main__':
    main()
