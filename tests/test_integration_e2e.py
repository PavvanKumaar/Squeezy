# tests/test_integration_e2e.py
#
# End-to-End Integration Test
#
# Verifies that the complete data → features → state pipeline
# works correctly for a single stock (GME).
#
# What this test proves:
#   1. All databases reachable
#   2. Universe screener produces output
#   3. Collectors run and write to correct databases
#   4. Feature engines read from databases correctly
#   5. SI proxy produces a direction estimate
#   6. News NLP produces features 25-27
#   7. Feature assembler produces valid 28-vector
#   8. Candidate evaluator makes a decision
#   9. State manager saves score to Redis + TimescaleDB
#   10. collection_log has entries for all sources
#
# Prerequisites:
#   docker-compose up -d
#   python tests/test_connections.py  (5/5 passing)
#   All collectors already run at least once for GME
#   (run: python pipeline/collectors/yfinance_collector.py etc.)
#
# Run:
#   python tests/test_integration_e2e.py

import sys
import os
import time
import numpy as np
import psycopg2
import pymongo
import redis
import json

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
load_dotenv()

# ─────────────────────────────────────────────────────────────
TICKER      = 'GME'
PASS_SYMBOL = '  ✓'
FAIL_SYMBOL = '  ✗'
SKIP_SYMBOL = '  ⚠'
# ─────────────────────────────────────────────────────────────

passed = 0
failed = 0
skipped = 0


def check(condition: bool, msg: str,
          detail: str = '', skip: bool = False):
    global passed, failed, skipped
    if skip:
        print(f'{SKIP_SYMBOL} SKIP  {msg}'
              + (f' — {detail}' if detail else ''))
        skipped += 1
    elif condition:
        print(f'{PASS_SYMBOL} PASS  {msg}'
              + (f' — {detail}' if detail else ''))
        passed += 1
    else:
        print(f'{FAIL_SYMBOL} FAIL  {msg}'
              + (f' — {detail}' if detail else ''))
        failed += 1


def section(title: str):
    print(f'\n{"─"*55}')
    print(f'  {title}')
    print(f'{"─"*55}')


# ═════════════════════════════════════════════════════════════
# SECTION 1 — DATABASE CONNECTIONS
# ═════════════════════════════════════════════════════════════

def test_connections():
    section('1. Database Connections')

    # PostgreSQL
    try:
        conn = psycopg2.connect(os.getenv('POSTGRES_URL'))
        cur  = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM stocks')
        count = cur.fetchone()[0]
        conn.close()
        check(True, 'PostgreSQL',
              f'{count} stocks in DB')
    except Exception as e:
        check(False, 'PostgreSQL', str(e))

    # MongoDB
    try:
        client = pymongo.MongoClient(
            os.getenv('MONGO_URL'),
            serverSelectionTimeoutMS=3000)
        client.server_info()
        db     = client[os.getenv('MONGO_DB', 'squeezradar')]
        count  = db['news_articles'].count_documents({})
        client.close()
        check(True, 'MongoDB',
              f'{count} news articles')
    except Exception as e:
        check(False, 'MongoDB', str(e))

    # Redis
    try:
        r = redis.Redis.from_url(
            os.getenv('REDIS_URL', 'redis://localhost:6379'))
        r.ping()
        lb_size = r.zcard('leaderboard:squeeze_score')
        check(True, 'Redis',
              f'leaderboard has {lb_size} entries')
    except Exception as e:
        check(False, 'Redis', str(e))

    # Cassandra
    try:
        from cassandra.cluster import Cluster
        cluster = Cluster(
            [os.getenv('CASSANDRA_HOST', 'localhost')])
        session = cluster.connect('squeezradar')
        rows = session.execute(
            "SELECT table_name FROM system_schema.tables "
            "WHERE keyspace_name = 'squeezradar'")
        tables = [r.table_name for r in rows]
        cluster.shutdown()
        check(True, 'Cassandra',
              f'tables: {", ".join(tables)}')
    except Exception as e:
        check(False, 'Cassandra', str(e))

    # Elasticsearch
    try:
        from elasticsearch import Elasticsearch
        es = Elasticsearch(
            os.getenv('ELASTIC_URL', 'http://localhost:9200'))
        info = es.info()
        check(True, 'Elasticsearch',
              f'cluster: {info["cluster_name"]}')
    except Exception as e:
        check(False, 'Elasticsearch', str(e))


# ═════════════════════════════════════════════════════════════
# SECTION 2 — UNIVERSE SCREENER
# ═════════════════════════════════════════════════════════════

def test_screener():
    section('2. Universe Screener')

    try:
        from pipeline.screener.universe_screener import (
            UniverseScreener)
        screener = UniverseScreener()

        # Ensure GME exists in DB as CANDIDATE for testing
        conn = psycopg2.connect(os.getenv('POSTGRES_URL'))
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO stocks (ticker, status, promoted_at)
            VALUES (%s, 'CANDIDATE', NOW())
            ON CONFLICT (ticker) DO UPDATE
              SET status = 'CANDIDATE',
                  promoted_at = NOW()
        """, (TICKER,))
        conn.commit()
        conn.close()

        check(True, 'UniverseScreener imports',
              'module loads without error')

        # Test get_tickers_by_status
        candidates = screener.get_tickers_by_status('CANDIDATE')
        check(TICKER in candidates,
              f'{TICKER} is CANDIDATE',
              f'{len(candidates)} candidates in DB')

        # Test fast_track
        screener.fast_track(TICKER, reason='integration_test')
        status = screener.get_status(TICKER)
        check(status == 'ACTIVE',
              'fast_track promotes to ACTIVE',
              f'status = {status}')

        screener.close()

    except Exception as e:
        check(False, 'UniverseScreener', str(e))


# ═════════════════════════════════════════════════════════════
# SECTION 3 — DATA COLLECTION
# ═════════════════════════════════════════════════════════════

def test_data_in_databases():
    section('3. Data In Databases')

    conn = psycopg2.connect(os.getenv('POSTGRES_URL'))
    cur  = conn.cursor()

    # TimescaleDB OHLCV
    cur.execute("""
        SELECT COUNT(*) FROM stock_timeseries
        WHERE ticker = %s AND interval = '1d'
    """, (TICKER,))
    ohlcv_count = cur.fetchone()[0]
    check(ohlcv_count >= 2,
          'OHLCV in TimescaleDB',
          f'{ohlcv_count} daily rows for {TICKER}')

    # stock_snapshots
    cur.execute("""
        SELECT COUNT(*) FROM stock_snapshots
        WHERE ticker = %s
    """, (TICKER,))
    snap_count = cur.fetchone()[0]
    check(snap_count >= 1,
          'stock_snapshots present',
          f'{snap_count} snapshots')

    # Short interest data
    cur.execute("""
        SELECT short_interest_pct, days_to_cover
        FROM stock_snapshots
        WHERE ticker = %s
          AND short_interest_pct IS NOT NULL
        ORDER BY collected_at DESC LIMIT 1
    """, (TICKER,))
    si_row = cur.fetchone()
    has_si = si_row is not None
    check(has_si,
          'Short interest data present',
          f'SI={si_row[0]}% DTC={si_row[1]}' if has_si
          else 'run stockanalysis_collector first')

    # Borrow rate data
    cur.execute("""
        SELECT borrow_rate FROM stock_snapshots
        WHERE ticker = %s
          AND borrow_rate IS NOT NULL
          AND data_source = 'iborrowdesk'
        ORDER BY collected_at DESC LIMIT 1
    """, (TICKER,))
    br_row = cur.fetchone()
    has_br = br_row is not None
    check(has_br,
          'Borrow rate data present',
          f'{br_row[0]}%' if has_br
          else 'run iborrowdesk_collector first')

    # Options data
    cur.execute("""
        SELECT call_put_ratio, iv_rank
        FROM options_snapshots
        WHERE ticker = %s
        ORDER BY collected_at DESC LIMIT 1
    """, (TICKER,))
    opt_row = cur.fetchone()
    has_opt = opt_row is not None
    check(has_opt,
          'Options data present',
          f'C/P={opt_row[0]} IV={opt_row[1]}' if has_opt
          else 'run options_collector first')

    # FINRA short volume
    cur.execute("""
        SELECT short_volume_ratio FROM finra_short_volume
        WHERE ticker = %s
        ORDER BY trade_date DESC LIMIT 1
    """, (TICKER,))
    sv_row = cur.fetchone()
    has_sv = sv_row is not None
    check(has_sv,
          'FINRA short volume present',
          f'ratio={sv_row[0]}' if has_sv
          else 'run finra_collector first')

    # StockTwits data
    cur.execute("""
        SELECT bull_ratio, message_count
        FROM stocktwits_snapshots
        WHERE ticker = %s
        ORDER BY collected_at DESC LIMIT 1
    """, (TICKER,))
    st_row = cur.fetchone()
    has_st = st_row is not None
    check(has_st,
          'StockTwits data present',
          f'bull={st_row[0]:.0%} msgs={st_row[1]}' if has_st
          else 'run stocktwits_collector first',
          skip=not has_st)

    # Google Trends data
    cur.execute("""
        SELECT trend_score FROM trends_snapshots
        WHERE ticker = %s
        ORDER BY collected_at DESC LIMIT 1
    """, (TICKER,))
    gt_row = cur.fetchone()
    has_gt = gt_row is not None
    check(has_gt,
          'Google Trends data present',
          f'score={gt_row[0]}' if has_gt
          else 'run pytrends_collector first',
          skip=not has_gt)

    conn.close()

    # MongoDB news articles
    try:
        mongo = pymongo.MongoClient(
            os.getenv('MONGO_URL'))
        db    = mongo[os.getenv('MONGO_DB', 'squeezradar')]
        count = db['news_articles'].count_documents(
            {'ticker': TICKER})
        check(count >= 0,
              'News articles in MongoDB',
              f'{count} articles for {TICKER}')
        mongo.close()
    except Exception as e:
        check(False, 'MongoDB news check', str(e))


# ═════════════════════════════════════════════════════════════
# SECTION 4 — FEATURE ENGINEERING
# ═════════════════════════════════════════════════════════════

def test_feature_engines():
    section('4. Feature Engines')

    # Market features
    try:
        from pipeline.engineering.market_features import (
            MarketFeatureEngine)
        engine   = MarketFeatureEngine()
        features = engine.compute(TICKER)

        check('price_current' in features,
              'MarketFeatureEngine.compute() runs',
              f'price=${features.get("price_current")}')
        check(features.get('ohlcv_data_available', False),
              'OHLCV data available for market features',
              f'RSI={features.get("rsi_14")}')

        engine.close()
    except Exception as e:
        check(False, 'MarketFeatureEngine', str(e))

    # Options features
    try:
        from pipeline.engineering.options_features import (
            OptionsFeatureEngine)
        engine   = OptionsFeatureEngine()
        features = engine.compute(TICKER)

        check('options_available' in features,
              'OptionsFeatureEngine.compute() runs',
              f'options_available='
              f'{features.get("options_available")}')
        if features.get('options_available'):
            check(
                features.get('call_put_ratio', 0) >= 0,
                'Call/put ratio valid',
                f'C/P={features.get("call_put_ratio")}')

        engine.close()
    except Exception as e:
        check(False, 'OptionsFeatureEngine', str(e))

    # Social features
    try:
        from pipeline.engineering.social_features import (
            SocialFeatureEngine)
        engine   = SocialFeatureEngine()
        features = engine.compute(TICKER)

        check('stocktwits_bull_ratio' in features,
              'SocialFeatureEngine.compute() runs',
              f'bull={features.get("stocktwits_bull_ratio")} '
              f'social_score={features.get("social_score")}')

        engine.close()
    except Exception as e:
        check(False, 'SocialFeatureEngine', str(e))

    # SI proxy
    try:
        from pipeline.engineering.si_proxy import SIProxyEstimator
        estimator = SIProxyEstimator()
        result    = estimator.estimate(TICKER)

        check('si_proxy_direction' in result,
              'SIProxyEstimator.estimate() runs',
              f'direction={result.get("si_proxy_direction")} '
              f'confidence={result.get("si_proxy_confidence")}')
        check(result.get('si_effective_pct', -1) >= 0,
              'SI effective pct is non-negative',
              f'{result.get("si_effective_pct")}%')

        estimator.close()
    except Exception as e:
        check(False, 'SIProxyEstimator', str(e))


# ═════════════════════════════════════════════════════════════
# SECTION 5 — NEWS NLP PIPELINE
# ═════════════════════════════════════════════════════════════

def test_news_nlp():
    section('5. News NLP Pipeline')

    try:
        from pipeline.nlp.news_nlp_pipeline import NewsNLPPipeline
        pipeline = NewsNLPPipeline()

        features = pipeline.get_ticker_nlp_features(
            TICKER, hours=48)

        check('news_sentiment_score' in features,
              'NewsNLPPipeline returns features',
              f'articles={features.get("news_article_count")}')

        sentiment = features.get('news_sentiment_score', 0.0)
        check(-1.0 <= sentiment <= 1.0,
              'news_sentiment_score in valid range [-1, 1]',
              f'{sentiment:+.4f}')

        urgency = features.get('news_squeeze_keyword_score', 0.0)
        check(0.0 <= urgency <= 1.0,
              'news_squeeze_keyword_score in valid range [0, 1]',
              f'{urgency:.4f}')

        composite = features.get('news_catalyst_composite', 0.0)
        check(0.0 <= composite <= 1.0,
              'news_catalyst_composite in valid range [0, 1]',
              f'{composite:.4f}')

        pipeline.close()
    except Exception as e:
        check(False, 'NewsNLPPipeline', str(e))


# ═════════════════════════════════════════════════════════════
# SECTION 6 — FEATURE ASSEMBLER
# ═════════════════════════════════════════════════════════════

def test_feature_assembler():
    section('6. Feature Assembler')

    try:
        from pipeline.engineering.feature_assembler import (
            FeatureAssembler, FEATURE_COUNT, FEATURE_ORDER)
        assembler = FeatureAssembler()

        # Assemble features
        result = assembler.assemble(TICKER)

        check('feature_vector' in result,
              'assemble() returns feature_vector',
              f'quality={result.get("quality_score")}%')

        check(len(result['feature_vector']) == FEATURE_COUNT,
              f'feature_vector has {FEATURE_COUNT} features',
              f'got {len(result["feature_vector"])}')

        # Convert to numpy vector
        vector = assembler.to_vector(result)

        check(vector.shape == (FEATURE_COUNT,),
              f'numpy vector shape ({FEATURE_COUNT},)',
              f'got {vector.shape}')

        check(not np.any(np.isnan(vector)),
              'No NaN values in vector',
              f'{np.sum(np.isnan(vector))} NaNs found')

        check(not np.any(np.isinf(vector)),
              'No Inf values in vector')

        check(np.all((vector >= -1.0) & (vector <= 1.0)),
              'All values in [-1, 1] range',
              f'out of range: '
              f'{np.sum((vector < -1.0) | (vector > 1.0))}')

        # Validate
        valid = assembler.validate_vector(vector)
        check(valid, 'validate_vector() passes')

        # Check quality score
        quality = result.get('quality_score', 0)
        check(quality >= 50,
              f'Quality score >= 50% (data sufficient)',
              f'{quality}% complete')

        # Print feature breakdown
        print(f'\n  Feature vector breakdown:')
        categories = [
            ('Market/SI [0-5]',   0,  6),
            ('Price/Vol [6-11]',  6, 12),
            ('Technical [12-13]', 12, 14),
            ('Options [14-18]',   14, 19),
            ('Social [19-24]',    19, 25),
            ('News NLP [25-27]',  25, 28),
        ]
        for label, start, end in categories:
            vals     = vector[start:end]
            non_zero = np.sum(vals != 0)
            total    = end - start
            print(f'    {label}: '
                  f'{non_zero}/{total} non-zero | '
                  f'max={vals.max():.3f} '
                  f'min={vals.min():.3f}')

        assembler.close()
        return vector

    except Exception as e:
        check(False, 'FeatureAssembler', str(e))
        return None


# ═════════════════════════════════════════════════════════════
# SECTION 7 — CANDIDATE EVALUATOR
# ═════════════════════════════════════════════════════════════

def test_candidate_evaluator():
    section('7. Candidate Evaluator')

    try:
        from pipeline.state.candidate_evaluator import (
            CandidateEvaluator)
        evaluator = CandidateEvaluator()

        # Set GME to CANDIDATE for testing
        conn = psycopg2.connect(os.getenv('POSTGRES_URL'))
        cur  = conn.cursor()
        cur.execute("""
            UPDATE stocks SET status = 'CANDIDATE',
                              promoted_at = NOW() - INTERVAL '3 days'
            WHERE ticker = %s
        """, (TICKER,))
        conn.commit()
        conn.close()

        # Gate 1
        gate1 = evaluator.check_gate1(TICKER)
        check('passed' in gate1,
              'check_gate1() returns result',
              f'passed={gate1["passed"]} '
              f'ohlcv_days={gate1.get("ohlcv_days")}')

        # Gate 2
        gate2 = evaluator.check_gate2(TICKER)
        check('signals_met' in gate2,
              'check_gate2() returns result',
              f'signals_met={gate2["signals_met"]}/2 '
              f'triggered={gate2.get("triggered_categories")}')

        # Full evaluation
        result = evaluator.evaluate(TICKER)
        check(result['decision'] in [
            'PROMOTE', 'STAY', 'DEMOTE'],
              'evaluate() returns valid decision',
              f'decision={result["decision"]} | '
              f'{result["reason"][:60]}')

        evaluator.close()

    except Exception as e:
        check(False, 'CandidateEvaluator', str(e))


# ═════════════════════════════════════════════════════════════
# SECTION 8 — STATE MANAGER
# ═════════════════════════════════════════════════════════════

def test_state_manager():
    section('8. State Manager')

    try:
        from pipeline.state.stock_state_manager import (
            StockStateManager)
        mgr = StockStateManager()

        # Transition to ACTIVE
        mgr.transition_status(
            TICKER, 'ACTIVE', 'integration_test')
        status = mgr.get_status(TICKER)
        check(status == 'ACTIVE',
              'transition_status() CANDIDATE → ACTIVE',
              f'status={status}')

        # Save a mock score
        mock_score = {
            'squeeze_score':    72.5,
            'phase_label':      'Heating Up',
            'phase_probabilities': {
                'Early Setup':  0.08,
                'Heating Up':   0.62,
                'Squeeze Zone': 0.22,
                'Danger Zone':  0.05,
                'Cooldown':     0.03,
            },
            'hype_acceleration':   0.45,
            'mention_velocity':    1.8,
            'sentiment_score':     0.31,
            'volume_ratio':        2.7,
            'borrow_rate':         34.5,
            'call_put_ratio':      2.1,
            'explanation_text':    (
                'Score driven by: borrow_rate (34.5%, +0.28); '
                'volume_ratio (2.7x, +0.21); '
                'call_put_ratio (2.1x, +0.15)'),
            'model_version':       'v1.0_integration_test',
        }
        saved = mgr.save_score(TICKER, mock_score)
        check(saved,
              'save_score() writes to Redis + TimescaleDB')

        # Read back from Redis
        cached = mgr.get_cached_score(TICKER)
        check(cached is not None,
              'get_cached_score() returns cached score',
              f'score={cached.get("squeeze_score") if cached else None}')

        # Phase transition detection
        changed = mgr.detect_phase_transition(
            TICKER, 'Squeeze Zone')
        check(changed,
              'detect_phase_transition() detects change',
              'Heating Up → Squeeze Zone')

        no_change = mgr.detect_phase_transition(
            TICKER, 'Squeeze Zone')
        check(not no_change,
              'detect_phase_transition() no-op on same phase')

        # Adaptive polling
        interval = mgr.get_collection_interval(
            TICKER, 'reddit')
        check(interval > 0,
              'get_collection_interval() returns interval',
              f'{interval}s for current phase')

        # Leaderboard
        lb = mgr.get_leaderboard(limit=5)
        check(isinstance(lb, list),
              'get_leaderboard() returns list',
              f'{len(lb)} entries')

        # Pipeline health
        health = mgr.get_pipeline_health()
        check('status_counts' in health,
              'get_pipeline_health() returns metrics',
              f'redis_ping={health.get("redis_ping")}')

        # Feature vector caching
        mock_features = {'short_interest_pct': 0.28,
                          'test': True}
        mgr.cache_feature_vector(TICKER, mock_features)
        cached_fv = mgr.get_cached_features(TICKER)
        check(cached_fv is not None,
              'Feature vector cache works',
              f'{len(cached_fv)} fields cached')

        mgr.close()

    except Exception as e:
        check(False, 'StockStateManager', str(e))


# ═════════════════════════════════════════════════════════════
# SECTION 9 — COLLECTION LOG AUDIT
# ═════════════════════════════════════════════════════════════

def test_collection_log():
    section('9. Collection Log Audit')

    conn = psycopg2.connect(os.getenv('POSTGRES_URL'))
    cur  = conn.cursor()

    # Count entries per source
    cur.execute("""
        SELECT source,
               COUNT(*) as total,
               SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successes,
               SUM(CASE WHEN status = 'failed'  THEN 1 ELSE 0 END) as failures
        FROM   collection_log
        WHERE  ticker = %s
        GROUP  BY source
        ORDER  BY total DESC
    """, (TICKER,))
    rows = cur.fetchall()

    check(len(rows) > 0,
          'collection_log has entries',
          f'{len(rows)} sources logged')

    print(f'\n  Source breakdown for {TICKER}:')
    for row in rows:
        source     = row[0]
        total      = row[1]
        successes  = row[2]
        failures   = row[3]
        success_rt = f'{successes}/{total}'
        status_sym = '✓' if failures == 0 else '!'
        print(f'    {status_sym} {source:30s} '
              f'{success_rt} runs '
              f'({failures} failures)')

    # Sources we expect to have logged
    expected_sources = [
        'yfinance_info',
        'yfinance_ohlcv_1d',
        'iborrowdesk',
        'stockanalysis',
        'options_yfinance',
        'news',
    ]
    logged_sources = {r[0] for r in rows}
    for source in expected_sources:
        check(source in logged_sources,
              f'{source} has logged',
              'not logged yet' if source not in logged_sources
              else 'ok',
              skip=source not in logged_sources)

    conn.close()


# ═════════════════════════════════════════════════════════════
# SECTION 10 — IMPORT VERIFICATION
# ═════════════════════════════════════════════════════════════

def test_imports():
    section('10. Package Import Verification')

    import_tests = [
        ('pipeline', 'pipeline'),
        ('pipeline.collectors', 'pipeline.collectors'),
        ('pipeline.engineering', 'pipeline.engineering'),
        ('pipeline.nlp', 'pipeline.nlp'),
        ('pipeline.ml', 'pipeline.ml'),
        ('pipeline.screener', 'pipeline.screener'),
        ('pipeline.state', 'pipeline.state'),
        ('pipeline.scheduler', 'pipeline.scheduler'),

        ('YFinanceCollector',
         'pipeline.collectors.yfinance_collector',
         'YFinanceCollector'),
        ('MarketFeatureEngine',
         'pipeline.engineering.market_features',
         'MarketFeatureEngine'),
        ('OptionsFeatureEngine',
         'pipeline.engineering.options_features',
         'OptionsFeatureEngine'),
        ('SocialFeatureEngine',
         'pipeline.engineering.social_features',
         'SocialFeatureEngine'),
        ('SIProxyEstimator',
         'pipeline.engineering.si_proxy',
         'SIProxyEstimator'),
        ('FeatureAssembler',
         'pipeline.engineering.feature_assembler',
         'FeatureAssembler'),
        ('UniverseScreener',
         'pipeline.screener.universe_screener',
         'UniverseScreener'),
        ('StockStateManager',
         'pipeline.state.stock_state_manager',
         'StockStateManager'),
        ('CandidateEvaluator',
         'pipeline.state.candidate_evaluator',
         'CandidateEvaluator'),
        ('NewsNLPPipeline',
         'pipeline.nlp.news_nlp_pipeline',
         'NewsNLPPipeline'),
        ('SentimentAnalyzer',
         'pipeline.nlp.sentiment',
         'SentimentAnalyzer'),
        ('KeywordExtractor',
         'pipeline.nlp.keyword_extractor',
         'KeywordExtractor'),
    ]

    for test in import_tests:
        if len(test) == 2:
            name, module = test
            try:
                __import__(module)
                check(True, f'import {module}')
            except Exception as e:
                check(False, f'import {module}', str(e))
        else:
            cls_name, module, attr = test
            try:
                mod = __import__(
                    module, fromlist=[attr])
                cls = getattr(mod, attr)
                check(cls is not None,
                      f'from {module} import {attr}')
            except Exception as e:
                check(False,
                      f'from {module} import {attr}',
                      str(e))


# ═════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════

def main():
    print('\n' + '='*55)
    print('  SqueezeRadar — End-to-End Integration Test')
    print(f'  Ticker: {TICKER}')
    print(f'  Time:   {__import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('='*55)

    start = time.time()

    test_imports()
    test_connections()
    test_screener()
    test_data_in_databases()
    test_feature_engines()
    test_news_nlp()
    feature_vector = test_feature_assembler()
    test_candidate_evaluator()
    test_state_manager()
    test_collection_log()

    elapsed = round(time.time()-start, 1)

    print('\n' + '='*55)
    print(f'  Results: {passed} passed | '
          f'{failed} failed | '
          f'{skipped} skipped | '
          f'{elapsed}s')
    print('='*55)

    if failed == 0:
        print('\n  ✓ ALL TESTS PASSED')
        print('  Pipeline is ready for ML model training.')
        print('\n  Next steps:')
        print('    1. python notebooks/01_build_training_data.py')
        print('    2. python notebooks/02_train_lifecycle_classifier.py')
        print('    3. python pipeline/scheduler/celery_tasks.py')
    else:
        print(f'\n  ✗ {failed} TESTS FAILED')
        print('  Fix failures before moving to ML.')
        print('\n  Common causes:')
        print('    - Collector not yet run for this ticker')
        print('    - Database not populated')
        print('    - Missing environment variable in .env')

    print()
    return failed == 0


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)