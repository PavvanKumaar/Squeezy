"""
tests/test_finra.py
Tests for FINRACollector (TASK-202)

Per SPEC.md §7.1 every collector must have tests covering:
  1. Successful collection for a known real ticker (GME)
  2. Graceful failure for a fake ticker (FAKE123)
  3. Data landed in the correct database (Cassandra)
  4. Entry written to collection_log with correct status
  5. Return dict contains all expected keys

Run with:
    $env:PYTHONUTF8=1; python tests/test_finra.py
"""

import sys, io
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

import os
import sys
from datetime import date, timedelta
sys.path.insert(0, os.path.abspath('.'))

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from pipeline.collectors.finra_collector import FINRACollector

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
print("  FINRACollector — Test Suite")
print("="*55)

collector = FINRACollector()

# ── Test 1: Download daily file ────────────────────────────────
print("\n[1] download_daily_file() — last business day")
last_biz = collector._last_business_day()
print(f"  Target date: {last_biz}")
df = collector.download_daily_file(last_biz)

check("download_daily_file() returns a DataFrame or None",
      df is None or hasattr(df, 'shape'))
if df is not None:
    check("DataFrame is not empty",   len(df) > 0, f"got {len(df)} rows")
    check("Symbol column exists",     "Symbol" in df.columns)
    check("ShortVolume column exists","ShortVolume" in df.columns)
    check("TotalVolume column exists","TotalVolume" in df.columns)
    check("DataFrame has 1000+ rows", len(df) > 1000,
          f"got {len(df)} — FINRA covers all US equities")
else:
    print("  (file not yet published for this date — weekend or holiday)")

# ── Test 2: get_short_volume() for GME ────────────────────────
print("\n[2] get_short_volume(): GME")
row = collector.get_short_volume("GME", last_biz, df=df)

if row is not None:
    check("row is a dict",                     isinstance(row, dict))
    check("ticker == GME",                     row.get("ticker") == "GME")
    check("short_volume is a positive int",
          isinstance(row.get("short_volume"), int) and row["short_volume"] > 0,
          f"got {row.get('short_volume')}")
    check("total_volume >= short_volume",
          row.get("total_volume", 0) >= row.get("short_volume", 0))
    check("short_volume_ratio between 0 and 1",
          0 <= row.get("short_volume_ratio", -1) <= 1,
          f"got {row.get('short_volume_ratio')}")
else:
    print("  (GME not in FINRA file — file may be unavailable)")

# ── Test 3: collect() return dict ─────────────────────────────
print("\n[3] Full collect(): GME")
res = collector.collect("GME")

check("collect() returns a dict",   isinstance(res, dict))
check("ticker field == GME",        res.get("ticker") == "GME")
check("collected_at is present",    bool(res.get("collected_at")))
check("target_date is present",     bool(res.get("target_date")))
check("duration_ms > 0",            res.get("duration_ms", 0) > 0)

REQUIRED_KEYS = ["ticker", "collected_at", "target_date", "error",
                 "error_message", "short_volume", "total_volume",
                 "short_volume_ratio", "duration_ms"]
missing = [k for k in REQUIRED_KEYS if k not in res]
check("all required keys present", not missing, f"missing: {missing}")

# ── Test 4: Graceful failure for fake ticker ───────────────────
print("\n[4] Fake ticker: FAKE123")
res_fake = collector.collect("FAKE123")

check("collect() returns a dict for fake ticker", isinstance(res_fake, dict))
check("error == False (skipped, not errored)",
      res_fake.get("error") is False,
      "expected False (ticker not in file = skipped, not a crash)")
check("short_volume is None for fake",  res_fake.get("short_volume") is None)
check("total_volume is None for fake",  res_fake.get("total_volume") is None)

# ── Test 5: History helper ─────────────────────────────────────
print("\n[5] get_short_volume_history(): GME (5 days)")
hist = collector.get_short_volume_history("GME", days=5)

check("history is a DataFrame",     hasattr(hist, 'shape'))
check("history has expected columns",
      all(c in hist.columns for c in
          ["date", "short_volume", "total_volume", "short_volume_ratio"]))
check("history has at least 1 row", len(hist) >= 1,
      f"got {len(hist)} rows")

# ── Test 6: collection_log entries ────────────────────────────
print("\n[6] collection_log: PostgreSQL")
try:
    pg = get_pg()
    cur = pg.cursor()

    cur.execute(
        "SELECT status, rows_collected FROM collection_log "
        "WHERE ticker = 'GME' AND source = 'finra' "
        "ORDER BY collected_at DESC LIMIT 1"
    )
    log_row = cur.fetchone()
    check("collection_log has entry for GME/finra", log_row is not None)
    if log_row:
        check("collection_log status is valid",
              log_row[0] in ("success", "skipped", "failed"),
              f"got '{log_row[0]}'")

    cur.close()
    pg.close()

except Exception as e:
    print(f"  [FAIL] PostgreSQL collection_log check failed: {e}")
    results.extend([False, False])

# ── Test 7: Cassandra short_volume_daily ──────────────────────
print("\n[7] Database: Cassandra short_volume_daily")
try:
    if collector._cass_session is not None and not res.get("error") \
            and res.get("short_volume") is not None:
        rows = collector._cass_session.execute(
            "SELECT short_volume, total_volume FROM short_volume_daily "
            "WHERE ticker = 'GME' LIMIT 1"
        )
        row = list(rows)
        check("Cassandra has row for GME in short_volume_daily",
              len(row) > 0)
    else:
        print("  (skipping Cassandra check — session unavailable or no data written)")

except Exception as e:
    print(f"  [FAIL] Cassandra check failed: {e}")
    results.append(False)

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
