# tests/test_options.py

import sys
sys.path.append('.')

import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

from pipeline.collectors.options_collector import OptionsCollector

def test_options_collector():
    print("\n" + "="*50)
    print("Testing Options Collector")
    print("="*50)

    collector = OptionsCollector()

    # Test 1 — real ticker with options
    print("\nTest 1: Real ticker (GME)")
    result = collector.collect('GME')
    assert not result['error'], \
        f"Should not error on GME: {result.get('error_message')}"
    assert 'call_put_ratio' in result
    assert 'max_pain' in result
    assert 'iv_rank' in result
    assert result['call_put_ratio'] >= 0
    assert 0 <= result['iv_rank'] <= 100
    print("  ✓ Real ticker passed")

    # Test 2 — fake ticker (should fail gracefully)
    print("\nTest 2: Fake ticker (FAKE123)")
    result = collector.collect('FAKE123')
    assert result['error'], "Should error on fake ticker"
    print("  ✓ Fake ticker handled gracefully")

    # Test 3 — data landed in PostgreSQL
    print("\nTest 3: Data in PostgreSQL")
    conn = psycopg2.connect(os.getenv('POSTGRES_URL'))
    cur = conn.cursor()
    cur.execute("""
        SELECT call_put_ratio, max_pain, iv_rank
        FROM options_snapshots
        WHERE ticker = 'GME'
        ORDER BY collected_at DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    assert row is not None, "GME data should be in DB"
    print(f"  ✓ DB has GME: C/P={row[0]:.2f} "
          f"MaxPain=${row[1]:.2f} IV={row[2]:.0f}%")

    # Test 4 — collection_log has entry
    print("\nTest 4: collection_log entry")
    cur.execute("""
        SELECT status FROM collection_log
        WHERE ticker = 'GME' AND source = 'options_yfinance'
        ORDER BY collected_at DESC LIMIT 1
    """)
    log = cur.fetchone()
    assert log is not None
    assert log[0] == 'success'
    print(f"  ✓ collection_log shows: {log[0]}")

    conn.close()
    collector.close()

    print("\n✓ ALL TESTS PASSED — options_collector ready")

if __name__ == '__main__':
    test_options_collector()