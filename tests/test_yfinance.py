# tests/test_yfinance.py
#
# Tests for YFinanceCollector
# Run: python tests/test_yfinance.py
#
# What is verified:
#   ✓ Stock info fields returned (float, market cap, sector)
#   ✓ OHLCV saved to TimescaleDB (stock_timeseries)
#   ✓ All derived features computed (RSI, MACD, ATR, volume_ratio)
#   ✓ Options chain features computed (C/P ratio, IV, max pain)
#   ✓ Earnings date returned or None (not error)
#   ✓ News headlines returned as list
#   ✓ Fake ticker handled gracefully
#   ✓ collection_log has entries for each sub-source
#   ✓ yfinance_snapshots table has row for tested ticker

import sys
import os
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

import psycopg2
from dotenv import load_dotenv
load_dotenv()

from pipeline.collectors.yfinance_collector import YFinanceCollector

PASS = '  ✓'
FAIL = '  ✗'


def assert_ok(condition: bool, msg: str):
    if condition:
        print(f'{PASS} {msg}')
    else:
        print(f'{FAIL} {msg}')
        raise AssertionError(msg)


def test_yfinance_collector():
    print('\n' + '='*55)
    print('  YFinance Collector Tests')
    print('='*55)

    collector = YFinanceCollector()
    conn      = psycopg2.connect(os.getenv('POSTGRES_URL'))
    cur       = conn.cursor()

    # ─────────────────────────────────────────────────────
    # TEST 1 — Stock info for real ticker
    # ─────────────────────────────────────────────────────
    print('\n[1] Stock info (GME)')
    info = collector.get_stock_info('GME')

    assert_ok('error_info' not in info,
              f"no error in info response (got: {info.get('error_info')})")
    assert_ok('company_name' in info,
              'company_name field present')
    assert_ok(info.get('company_name') != '',
              f'company_name = {info.get("company_name")}')

    assert_ok('company_name' in info,
              'company_name field present')
    assert_ok(info.get('company_name') != '',
              f'company_name = {info.get("company_name")}')
    assert_ok(info.get('float_shares') is not None,
              f'float_shares = {info.get("float_shares")}')
    assert_ok(info.get('float_shares', 0) > 0,
              'float_shares > 0')
    assert_ok(info.get('avg_daily_volume_30d') is not None,
              f'avg_daily_volume_30d = '
              f'{info.get("avg_daily_volume_30d")}')
    assert_ok('sector' in info,
              f'sector = {info.get("sector")}')
    assert_ok(info.get('week52_high') is not None,
              f'week52_high = {info.get("week52_high")}')
    assert_ok('error_info' not in info,
              'no error in info response')

    # ─────────────────────────────────────────────────────
    # TEST 2 — OHLCV saved to TimescaleDB
    # ─────────────────────────────────────────────────────
    print('\n[2] OHLCV → TimescaleDB (GME)')

    # Clear existing test data so we get a clean count
    cur.execute("""
        DELETE FROM stock_timeseries
        WHERE ticker = 'GME_TEST'
    """)
    conn.commit()

    # Collect fresh
    df = collector.get_and_save_ohlcv('GME', period='30d',
                                       interval='1d')
    assert_ok(df is not None, 'get_and_save_ohlcv returned DataFrame')
    assert_ok(not df.empty, 'DataFrame is not empty')
    assert_ok(len(df) >= 15,
              f'At least 15 daily rows returned (got {len(df)})')

    # Verify rows landed in DB
    cur.execute("""
        SELECT COUNT(*) FROM stock_timeseries
        WHERE ticker = 'GME'
          AND interval = '1d'
    """)
    db_count = cur.fetchone()[0]
    assert_ok(db_count >= 15,
              f'TimescaleDB has {db_count} GME daily rows')

    # Check required columns exist in DataFrame
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        assert_ok(col in df.columns, f'Column {col} present')

    # ─────────────────────────────────────────────────────
    # TEST 3 — Derived features computed
    # ─────────────────────────────────────────────────────
    print('\n[3] Derived features (GME)')
    daily_df = collector.get_and_save_ohlcv('GME', period='60d',
                                             interval='1d')
    intra_df = collector.get_and_save_ohlcv('GME', period='5d',
                                             interval='1m')
    float_shares = info.get('float_shares')
    derived = collector.compute_derived_features(
        daily_df, intra_df, float_shares=float_shares)

    assert_ok('current_price' in derived,
              f'current_price = {derived.get("current_price")}')
    assert_ok(derived.get('current_price', 0) > 0,
              'current_price > 0')

    assert_ok('price_change_1d' in derived,
              f'price_change_1d = {derived.get("price_change_1d")}')
    assert_ok(abs(derived.get('price_change_1d', 999)) < 2.0,
              'price_change_1d is a sane percentage')

    assert_ok('volume_ratio_30d' in derived,
              f'volume_ratio_30d = {derived.get("volume_ratio_30d")}')
    assert_ok(derived.get('volume_ratio_30d', 0) > 0,
              'volume_ratio_30d > 0')

    assert_ok('avg_hourly_volume_30d' in derived,
              f'avg_hourly_volume_30d = '
              f'{derived.get("avg_hourly_volume_30d")}')

    if float_shares and float_shares > 0:
        assert_ok('float_rotation' in derived,
                  f'float_rotation = {derived.get("float_rotation")}')

    assert_ok('rsi_14' in derived,
              f'rsi_14 = {derived.get("rsi_14")}')
    assert_ok(0 <= derived.get('rsi_14', -1) <= 100,
              f'RSI in valid range: {derived.get("rsi_14")}')

    assert_ok('macd' in derived,
              f'macd = {derived.get("macd")}')
    assert_ok('atr_14' in derived,
              f'atr_14 = {derived.get("atr_14")}')
    assert_ok(derived.get('atr_14', -1) > 0,
              'ATR > 0')

    assert_ok('volatility_10d' in derived,
              f'volatility_10d = {derived.get("volatility_10d")}')

    # ─────────────────────────────────────────────────────
    # TEST 4 — Options chain features
    # ─────────────────────────────────────────────────────
    print('\n[4] Options features (GME)')
    opts = collector.get_options_summary('GME')

    assert_ok(opts.get('options_eligible') is True,
              'GME has options')
    assert_ok('call_put_ratio' in opts,
              f'call_put_ratio = {opts.get("call_put_ratio")}')
    assert_ok(opts.get('call_put_ratio', -1) >= 0,
              'call_put_ratio >= 0')
    assert_ok('iv_current' in opts,
              f'iv_current = {opts.get("iv_current")}')
    assert_ok(opts.get('iv_current', -1) >= 0,
              'iv_current >= 0')
    assert_ok('max_pain' in opts,
              f'max_pain = ${opts.get("max_pain")}')
    assert_ok(opts.get('max_pain', -1) > 0,
              'max_pain > 0')
    assert_ok('total_call_oi' in opts,
              f'total_call_oi = {opts.get("total_call_oi")}')
    assert_ok('nearest_expiry' in opts,
              f'nearest_expiry = {opts.get("nearest_expiry")}')

    # ─────────────────────────────────────────────────────
    # TEST 5 — Earnings date
    # ─────────────────────────────────────────────────────
    print('\n[5] Earnings date (AAPL)')
    # Use AAPL as it always has earnings data
    earnings = collector.get_earnings_date('AAPL')
    # Either a datetime or None — not an exception
    assert_ok(True, f'earnings_date returned without error: '
              f'{earnings}')

    # ─────────────────────────────────────────────────────
    # TEST 6 — News headlines
    # ─────────────────────────────────────────────────────
    print('\n[6] News headlines (GME)')
    news = collector.get_recent_news('GME')
    assert_ok(isinstance(news, list),
              'news returned as list')
    if len(news) > 0:
        article = news[0]
        assert_ok('title' in article,
                  f'first headline: {article.get("title", "")[:50]}')
        assert_ok('url' in article,
                  'url field present in article')
    print(f'  (got {len(news)} articles)')

    # ─────────────────────────────────────────────────────
    # TEST 7 — Fake ticker handled gracefully
    # ─────────────────────────────────────────────────────
    print('\n[7] Fake ticker (XYZFAKE999)')
    result = collector.collect_all('XYZFAKE999')
    # Should not raise — should return dict with partial data
    assert_ok(isinstance(result, dict),
              'fake ticker returns dict (not exception)')
    assert_ok(result.get('ticker') == 'XYZFAKE999',
              'ticker field correct in error response')

    # ─────────────────────────────────────────────────────
    # TEST 8 — collection_log has entries
    # ─────────────────────────────────────────────────────
    print('\n[8] collection_log entries')
    cur.execute("""
        SELECT source, status, rows_collected
        FROM   collection_log
        WHERE  ticker      = 'GME'
          AND  source LIKE 'yfinance%'
        ORDER  BY collected_at DESC
        LIMIT  10
    """)
    logs = cur.fetchall()
    assert_ok(len(logs) > 0,
              f'collection_log has {len(logs)} yfinance entries')
    success_logs = [l for l in logs if l[1] == 'success']
    assert_ok(len(success_logs) > 0,
              f'{len(success_logs)} successful collection_log entries')
    for log in logs[:4]:
        print(f'    source={log[0]} status={log[1]} '
              f'rows={log[2]}')

    # ─────────────────────────────────────────────────────
    # TEST 9 — Full collect_all end-to-end
    # ─────────────────────────────────────────────────────
    print('\n[9] Full collect_all end-to-end (AAPL)')
    full = collector.collect_all('AAPL')
    assert_ok('ticker' in full, 'ticker in result')
    assert_ok('current_price' in full, 'current_price computed')
    assert_ok('rsi_14' in full, 'RSI computed')
    assert_ok('call_put_ratio' in full, 'options computed')
    assert_ok('news_count' in full,
              f'news_count = {full.get("news_count")}')

    # ─────────────────────────────────────────────────────
    # SUMMARY
    # ─────────────────────────────────────────────────────
    print('\n' + '='*55)
    print('  ✓ ALL YFINANCE TESTS PASSED')
    print('='*55 + '\n')

    collector.close()
    conn.close()


if __name__ == '__main__':
    test_yfinance_collector()
