# pipeline/scoring/tasks_scoring.py   (OPTIONAL - only if you want Celery)
#
# Uses @shared_task, so it works with whatever Celery app you already have.
# The only edits in existing files are two small additions to your Celery config:
#
#   1) add the module so workers register the tasks:
#        include=[..., 'pipeline.scoring.tasks_scoring']
#   2) add a beat entry (UTC; covers both DST offsets; tune as you like):
#        from celery.schedules import crontab
#        'score-active': {
#            'task': 'scoring.score_active',
#            'schedule': crontab(minute='*/15', hour='13-21', day_of_week='1-5'),
#        },
#        'score-active-after-close': {
#            'task': 'scoring.score_active',
#            'schedule': crontab(minute=45, hour='21', day_of_week='1-5'),
#        },

from celery import shared_task

_scorer = None


def _get():
    global _scorer
    if _scorer is None:
        from pipeline.scoring.scorer import SqueezeScorer
        _scorer = SqueezeScorer()
    return _scorer


@shared_task(name='scoring.score_ticker')
def score_ticker(ticker: str) -> dict:
    return _get().score_and_store(ticker)


@shared_task(name='scoring.score_active')
def score_active() -> dict:
    s = _get()
    done, failed = [], []
    for t in s.state.get_stocks_by_status('ACTIVE'):
        try:
            done.append(s.score_and_store(t))
        except Exception as e:
            failed.append({'ticker': t, 'error': f'{type(e).__name__}: {e}'})
    return {'scored': len(done), 'failed': failed}
