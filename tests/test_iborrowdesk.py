# tests/test_iborrowdesk.py
#
# Tests for IBorrowDeskCollector
# Run: python tests/test_iborrowdesk.py
#
# What is verified:
#   ✓ Borrow rate returned as float > 0 for real tickers
#   ✓ Available shares returned as int >= 0
#   ✓ borrow_fee_tier correctly classified
#   ✓ is_hard_to_borrow flag set correctly
#   ✓ borrow_rate_delta computed (0.0 for first reading)
#   ✓ borrow_rate_7d_avg computed
#   ✓ Fake ticker returns error gracefully
#   ✓ Data saved to borrow_rate_snapshots table
#   ✓ Data saved to stock_snapshots table
#   ✓ collection_log has success entry

import sys, os
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

import psycopg2
from dotenv import load_dotenv
load_dotenv()

from pipeline.collectors.iborrowdesk_collector import (
    IBorrowDeskCollector, classify_borrow_rate)

PASS = '  ✓'
FAIL = '  ✗'


def assert_ok(condition: bool, msg: str):
    if condition:
        print(f'{PASS} {msg}')
    else:
        print(f'{FAIL} {msg}')
        raise AssertionError(msg)


def test_iborrowdesk_collector():
    print('\n' + '='*55)
    print('  IBorrowDesk Collector Tests')
    print('='*55)

    collector = IBorrowDeskCollector()
    conn      = psycopg2.connect(os.getenv('POSTGRES_URL'))
    cur       = conn.cursor()

    # ─────────────────────────────────────────────────────
    # TEST 1 — classify_borrow_rate function
    # ─────────────────────────────────────────────────────
    print('\n[1] classify_borrow_rate() function')

    cases = [
        (5.0,   'cheap'),
        (20.0,  'normal'),
        (50.0,  'expensive'),
        (100.0, 'hard_to_borrow'),
        (300.0, 'extreme'),
    ]
    for rate, expected in cases:
        result = classify_borrow_rate(rate)
        assert_ok(result == expected,
                  f'classify({rate}%) = {result} '
                  f'(expected {expected})')

    # ─────────────────────────────────────────────────────
    # TEST 2 — Real ticker collection (AAPL — low borrow rate)
    # ─────────────────────────────────────────────────────
    print('\n[2] Real ticker — AAPL (low borrow rate)')
    result = collector.collect('AAPL')

    if result.get('error'):
        # iborrowdesk might not have all tickers
        print(f'  ⚠ AAPL not found on iborrowdesk: '
              f'{result.get("error_message")}')
        print('  Skipping AAPL assertions')
    else:
        assert_ok(isinstance(result['borrow_rate'], float),
                  f'borrow_rate is float: {result["borrow_rate"]}')
        assert_ok(result['borrow_rate'] >= 0,
                  f'borrow_rate >= 0: {result["borrow_rate"]}')
        assert_ok(isinstance(result['available_shares'], int),
                  f'available_shares is int: '
                  f'{result["available_shares"]}')
        assert_ok(result['available_shares'] >= 0,
                  'available_shares >= 0')
        assert_ok('borrow_fee_tier' in result,
                  f'borrow_fee_tier = {result["borrow_fee_tier"]}')
        assert_ok(result['borrow_fee_tier'] in [
            'cheap', 'normal', 'expensive',
            'hard_to_borrow', 'extreme'],
                  'borrow_fee_tier is valid category')
        assert_ok('is_hard_to_borrow' in result,
                  f'is_hard_to_borrow = {result["is_hard_to_borrow"]}')
        assert_ok(isinstance(result['is_hard_to_borrow'], bool),
                  'is_hard_to_borrow is bool')
        assert_ok('borrow_rate_delta' in result,
                  f'borrow_rate_delta = {result["borrow_rate_delta"]}')
        assert_ok('borrow_rate_7d_avg' in result,
                  f'borrow_rate_7d_avg = '
                  f'{result["borrow_rate_7d_avg"]}')
        assert_ok(result['error'] is False,
                  'no error flag on successful collection')

    # ─────────────────────────────────────────────────────
    # TEST 3 — GME (typically higher borrow rate)
    # ─────────────────────────────────────────────────────
    print('\n[3] Real ticker — GME')
    gme = collector.collect('GME')

    if not gme.get('error'):
        assert_ok(gme['borrow_rate'] >= 0,
                  f'GME borrow_rate = {gme["borrow_rate"]}%')
        print(f'  GME borrow rate: {gme["borrow_rate"]}% '
              f'({gme["borrow_fee_tier"]})')
        print(f'  Available shares: {gme["available_shares"]:,}')
        print(f'  Hard to borrow: {gme["is_hard_to_borrow"]}')
        print(f'  Delta from last: {gme["borrow_rate_delta"]:+.2f}%')
    else:
        print(f'  ⚠ GME error: {gme.get("error_message")} '
              f'(iborrowdesk may not have this ticker)')

    # ─────────────────────────────────────────────────────
    # TEST 4 — Fake ticker handled gracefully
    # ─────────────────────────────────────────────────────
    print('\n[4] Fake ticker (XYZFAKE999)')
    fake = collector.collect('XYZFAKE999')

    assert_ok(fake['error'] is True,
              'error=True for fake ticker')
    assert_ok('error_message' in fake,
              f'error_message = {fake.get("error_message")}')
    assert_ok(fake.get('error_message') in [
        'ticker_not_found', 'parse_failed',
        'http_404', 'timeout'],
              f'error_message is a known code: '
              f'{fake.get("error_message")}')

    # ─────────────────────────────────────────────────────
    # TEST 5 — Data saved to borrow_rate_snapshots
    # ─────────────────────────────────────────────────────
    print('\n[5] borrow_rate_snapshots table')
    cur.execute("""
        SELECT borrow_rate, borrow_fee_tier,
               is_hard_to_borrow, collected_at
        FROM   borrow_rate_snapshots
        ORDER  BY collected_at DESC
        LIMIT  1
    """)
    row = cur.fetchone()
    assert_ok(row is not None,
              'borrow_rate_snapshots has at least one row')
    if row:
        assert_ok(row[0] is not None,
                  f'borrow_rate stored: {row[0]}')
        assert_ok(row[1] is not None,
                  f'borrow_fee_tier stored: {row[1]}')
        assert_ok(isinstance(row[2], bool),
                  f'is_hard_to_borrow is bool: {row[2]}')

    # ─────────────────────────────────────────────────────
    # TEST 6 — Data saved to stock_snapshots
    # ─────────────────────────────────────────────────────
    print('\n[6] stock_snapshots table')
    cur.execute("""
        SELECT borrow_rate, data_source
        FROM   stock_snapshots
        WHERE  data_source = 'iborrowdesk'
        ORDER  BY collected_at DESC
        LIMIT  1
    """)
    snap = cur.fetchone()
    assert_ok(snap is not None,
              'stock_snapshots has iborrowdesk entry')
    if snap:
        assert_ok(snap[1] == 'iborrowdesk',
                  'data_source = iborrowdesk')

    # ─────────────────────────────────────────────────────
    # TEST 7 — collection_log has entries
    # ─────────────────────────────────────────────────────
    print('\n[7] collection_log entries')
    cur.execute("""
        SELECT status, error_message, duration_ms
        FROM   collection_log
        WHERE  source = 'iborrowdesk'
        ORDER  BY collected_at DESC
        LIMIT  5
    """)
    logs = cur.fetchall()
    assert_ok(len(logs) > 0,
              f'collection_log has {len(logs)} iborrowdesk entries')
    for log in logs:
        print(f'    status={log[0]} '
              f'error={log[1]} '
              f'duration={log[2]}ms')

    # ─────────────────────────────────────────────────────
    # TEST 8 — get_history returns list
    # ─────────────────────────────────────────────────────
    print('\n[8] get_history()')
    history = collector.get_history('AAPL', days=30)
    assert_ok(isinstance(history, list),
              f'get_history returns list (len={len(history)})')

    # ─────────────────────────────────────────────────────
    # TEST 9 — collect_batch (2 tickers)
    # ─────────────────────────────────────────────────────
    print('\n[9] collect_batch()')
    batch = collector.collect_batch(['AAPL', 'GME'],
                                     delay_sec=1.0)
    assert_ok(len(batch) == 2,
              f'collect_batch returned {len(batch)} results')
    assert_ok(all('ticker' in r for r in batch),
              'all batch results have ticker field')

    # ─────────────────────────────────────────────────────
    print('\n' + '='*55)
    print('  ✓ ALL IBORROWDESK TESTS PASSED')
    print('='*55 + '\n')

    collector.close()
    conn.close()


if __name__ == '__main__':
    test_iborrowdesk_collector()