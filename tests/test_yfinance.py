"""
tests/test_yfinance.py
Tests for yfinanceCollector (TASK-101)

Per SPEC.md §7.1 every collector must have tests covering:
  1. Successful collection for a known real ticker (GME)
  2. Graceful failure for a fake ticker (FAKE123)
  3. Data landed in the correct database
  4. Entry written to collection_log with correct status
  5. Return dict contains all expected keys

Run with:
    $env:PYTHONUTF8=1; python tests/test_yfinance.py
"""

import sys, io
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

import os
import sys
sys.path.insert(0, os.path.abspath('.'))

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from pipeline.collectors.yfinace_collector import yfinanceCollector

PASS = "  [PASS]"
FAIL = "  [FAIL]"

results = []

def check(label, condition, detail=""):
    if condition:
        print(f"{PASS} {label}")
        results.append(True)
    else:
        print(f"{FAIL} {label}" + (f" — {detail}" if detail else ""))
        results.append(False)

def get_pg():
    url = os.getenv("POSTGRES_URL")
    return psycopg2.connect(url) if url else psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "squeezradar"),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
    )

# ─────────────────────────────────────────────────────────────
print("\n" + "="*55)
print("  yfinanceCollector — Test Suite")
print("="*55)

collector = yfinanceCollector()

# ── Test 1: Successful collection for GME ─────────────────────
print("\n[1] Real ticker: GME")
res = collector.collect("GME")

check("collect() returns a dict",           isinstance(res, dict))
check("error == False",                     res.get("error") is False,
      res.get("error_message"))
check("ticker field == GME",                res.get("ticker") == "GME")
check("collected_at is present",            bool(res.get("collected_at")))
check("current_price is a number",
      isinstance(res.get("current_price"), (int, float)) and res["current_price"] > 0,
      f"got {res.get('current_price')}")
check("float_shares is present",            res.get("float_shares") is not None)
check("ohlcv_rows >= 0",                    isinstance(res.get("ohlcv_rows"), int))
check("options_expiries >= 0",              isinstance(res.get("options_expiries"), int))
check("news is a list",                     isinstance(res.get("news"), list))
check("duration_ms > 0",                    res.get("duration_ms", 0) > 0)

REQUIRED_KEYS = [
    "ticker", "collected_at", "error", "error_message",
    "current_price", "float_shares", "shares_outstanding",
    "market_cap", "ohlcv_rows", "options_expiries",
    "fifty_two_week_high", "fifty_two_week_low", "duration_ms",
]
missing = [k for k in REQUIRED_KEYS if k not in res]
check("all required keys present", not missing, f"missing: {missing}")

# ── Test 2: Graceful failure for fake ticker ───────────────────
print("\n[2] Fake ticker: FAKE123")
res_fake = collector.collect("FAKE123")

check("collect() returns a dict for fake ticker",  isinstance(res_fake, dict))
check("error == True for fake ticker",             res_fake.get("error") is True)
check("error_message is a non-empty string",
      isinstance(res_fake.get("error_message"), str) and
      len(res_fake.get("error_message", "")) > 0)
check("ticker field == FAKE123",                   res_fake.get("ticker") == "FAKE123")

# ── Test 3: Data landed in PostgreSQL ─────────────────────────
print("\n[3] Database: PostgreSQL checks")
try:
    pg = get_pg()
    cur = pg.cursor()

    # stocks table
    cur.execute("SELECT ticker FROM stocks WHERE ticker = 'GME'")
    row = cur.fetchone()
    check("GME upserted into stocks table", row is not None)

    # stock_timeseries hypertable
    cur.execute(
        "SELECT COUNT(*) FROM stock_timeseries WHERE ticker = 'GME'"
    )
    ts_count = cur.fetchone()[0]
    check("OHLCV rows written to stock_timeseries",
          ts_count > 0, f"got {ts_count} rows")

    # ── Test 4: collection_log entry ──────────────────────────────
    print("\n[4] collection_log entry")
    cur.execute(
        "SELECT status, rows_collected FROM collection_log "
        "WHERE ticker = 'GME' AND source = 'yfinance' "
        "ORDER BY collected_at DESC LIMIT 1"
    )
    log_row = cur.fetchone()
    check("collection_log has entry for GME/yfinance", log_row is not None)
    if log_row:
        check("collection_log status == 'success'",
              log_row[0] == "success", f"got '{log_row[0]}'")
        check("collection_log rows_collected > 0",
              log_row[1] > 0, f"got {log_row[1]}")

    cur.close()
    pg.close()

except Exception as e:
    print(f"  [FAIL] DB checks failed: {e}")
    results.extend([False, False, False, False])

# ── Summary ──────────────────────────────────────────────────
print("\n" + "="*55)
passed = sum(results)
total  = len(results)
print(f"  {passed}/{total} assertions passed")
if passed == total:
    print("  ALL TESTS PASSED")
else:
    print("  SOME TESTS FAILED — see above")
print("="*55 + "\n")
sys.exit(0 if passed == total else 1)
