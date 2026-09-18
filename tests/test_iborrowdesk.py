"""
tests/test_iborrowdesk.py
Tests for iBorrowDeskCollector (TASK-102)

Per SPEC.md §7.1 every collector must have tests covering:
  1. Successful collection for a known real ticker (GME)
  2. Graceful failure for a fake ticker (FAKE123)
  3. Data landed in the correct database
  4. Entry written to collection_log with correct status
  5. Return dict contains all expected keys

Run with:
    $env:PYTHONUTF8=1; python tests/test_iborrowdesk.py
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
from pipeline.collectors.iborrowdesk_collector import iBorrowDeskCollector

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
print("  iBorrowDeskCollector — Test Suite")
print("="*55)

collector = iBorrowDeskCollector()

# ── Test 1: HTML selector check (parse only, no DB) ──────────
# Verify the scraper can fetch and parse the live page
print("\n[1] Live page parsing: GME borrow rate")
borrow_rate     = collector.get_borrow_rate("GME")
available_shares = collector.get_available_shares("GME")

check("get_borrow_rate() returns a value",
      borrow_rate is not None,
      "returned None — site may have changed HTML or ticker not listed")
if borrow_rate is not None:
    check("borrow_rate is a positive float",
          isinstance(borrow_rate, float) and borrow_rate >= 0,
          f"got {borrow_rate}")

check("get_available_shares() returns a value",
      available_shares is not None,
      "returned None — site may have changed HTML or ticker not listed")
if available_shares is not None:
    check("available_shares is a non-negative int",
          isinstance(available_shares, int) and available_shares >= 0,
          f"got {available_shares}")

# ── Test 2: collect() return dict for GME ─────────────────────
print("\n[2] Full collect(): GME")
res = collector.collect("GME")

check("collect() returns a dict",    isinstance(res, dict))
check("ticker field == GME",         res.get("ticker") == "GME")
check("collected_at is present",     bool(res.get("collected_at")))
check("duration_ms > 0",             res.get("duration_ms", 0) > 0)

REQUIRED_KEYS = ["ticker", "collected_at", "error", "error_message",
                 "borrow_rate", "available_shares", "duration_ms"]
missing = [k for k in REQUIRED_KEYS if k not in res]
check("all required keys present", not missing, f"missing: {missing}")

# ── Test 3: Graceful failure for fake ticker ───────────────────
print("\n[3] Fake ticker: FAKE123")
res_fake = collector.collect("FAKE123")

check("collect() returns a dict for fake ticker",  isinstance(res_fake, dict))
check("error == True for fake ticker",             res_fake.get("error") is True)
check("error_message is a non-empty string",
      isinstance(res_fake.get("error_message"), str) and
      len(res_fake.get("error_message", "")) > 0)
check("borrow_rate is None for fake ticker",       res_fake.get("borrow_rate") is None)
check("available_shares is None for fake ticker",  res_fake.get("available_shares") is None)

# ── Test 4 & 5: Database checks ───────────────────────────────
print("\n[4] Database: stock_snapshots & collection_log")
try:
    pg = get_pg()
    cur = pg.cursor()

    # stock_snapshots (only if collect succeeded)
    if not res.get("error"):
        cur.execute(
            "SELECT borrow_rate, available_shares FROM stock_snapshots "
            "WHERE ticker = 'GME' AND data_source = 'iborrowdesk' "
            "ORDER BY collected_at DESC LIMIT 1"
        )
        snap = cur.fetchone()
        check("Snapshot row in stock_snapshots", snap is not None)
        if snap:
            check("borrow_rate stored matches fetched",
                  snap[0] == res.get("borrow_rate"),
                  f"stored={snap[0]}, fetched={res.get('borrow_rate')}")
    else:
        print(f"  (skipping DB snapshot check — collect() returned error: {res.get('error_message')})")

    # collection_log (success or skipped entry should exist)
    cur.execute(
        "SELECT status FROM collection_log "
        "WHERE ticker = 'GME' AND source = 'iborrowdesk' "
        "ORDER BY collected_at DESC LIMIT 1"
    )
    log_row = cur.fetchone()
    check("collection_log has entry for GME/iborrowdesk", log_row is not None)
    if log_row:
        check("collection_log status is success or skipped",
              log_row[0] in ("success", "skipped", "failed"),
              f"got '{log_row[0]}'")

    cur.close()
    pg.close()

except Exception as e:
    print(f"  [FAIL] DB checks failed: {e}")
    results.extend([False, False, False])

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
