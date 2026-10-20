"""
tests/test_week1_integration.py
Week 1 end-to-end integration test

Runs all three Week 1 collectors (yfinance, iborrowdesk, FINRA) across
3 test tickers, then verifies data in every relevant database.

Per SPEC.md §7.2:
  - Run all collectors for 3 test tickers
  - Verify collection_log has success/skipped entries for all sources
  - Complete in under 3 minutes

Run with:
    $env:PYTHONUTF8=1; python tests/test_week1_integration.py
"""

import sys, io
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

import os
import sys
import time
sys.path.insert(0, os.path.abspath('.'))

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from pipeline.collectors.yfinace_collector      import yfinanceCollector
from pipeline.collectors.iborrowdesk_collector  import iBorrowDeskCollector
from pipeline.collectors.finra_collector        import FINRACollector

TEST_TICKERS = ["GME", "AMC", "BBBY"]
SOURCES      = ["yfinance", "iborrowdesk", "finra"]

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
print("\n" + "="*60)
print("  Week 1 Integration Test — 3 tickers × 3 collectors")
print("="*60)

wall_start = time.time()
collect_results = {}

# ── Step 1: Run all collectors for all tickers ────────────────
yf_col   = yfinanceCollector()
ibd_col  = iBorrowDeskCollector()
fin_col  = FINRACollector()

print(f"\nCollecting: {', '.join(TEST_TICKERS)}\n")

for ticker in TEST_TICKERS:
    print(f"  [{ticker}] yfinance ...", end=" ", flush=True)
    r_yf = yf_col.collect(ticker)
    print("OK" if not r_yf["error"] else f"ERR ({r_yf['error_message'][:50]})")

    print(f"  [{ticker}] iborrowdesk ...", end=" ", flush=True)
    r_ibd = ibd_col.collect(ticker)
    print("OK" if not r_ibd["error"] else f"ERR ({r_ibd['error_message'][:50]})")

    print(f"  [{ticker}] FINRA ...", end=" ", flush=True)
    r_fin = fin_col.collect(ticker)
    print("OK" if not r_fin["error"] else f"SKIP/ERR ({r_fin['error_message'][:50]})")

    collect_results[ticker] = {"yfinance": r_yf, "iborrowdesk": r_ibd, "finra": r_fin}

elapsed = time.time() - wall_start

# ── Step 2: Timing check ──────────────────────────────────────
print(f"\nTotal collection time: {elapsed:.1f}s")
check("Completed in under 180 seconds (3 min)", elapsed < 180,
      f"took {elapsed:.1f}s")

# ── Step 3: Per-ticker result checks ─────────────────────────
print("\n[Results] Per-ticker collect() output")
for ticker in TEST_TICKERS:
    res = collect_results[ticker]

    # yfinance must succeed for a real ticker
    check(f"{ticker}: yfinance returned dict with required keys",
          all(k in res["yfinance"] for k in
              ["ticker", "collected_at", "error", "current_price"]))

    # iborrowdesk and FINRA may legitimately return data or not
    # depending on whether the ticker is listed — check structure only
    check(f"{ticker}: iborrowdesk returned valid result dict",
          all(k in res["iborrowdesk"] for k in
              ["ticker", "error", "borrow_rate", "available_shares"]))

    check(f"{ticker}: FINRA returned valid result dict",
          all(k in res["finra"] for k in
              ["ticker", "error", "short_volume", "total_volume"]))

# ── Step 4: Database verification ─────────────────────────────
print("\n[DB] PostgreSQL checks")
try:
    pg = get_pg()
    cur = pg.cursor()

    # stocks table has all 3 tickers
    cur.execute(
        "SELECT ticker FROM stocks WHERE ticker = ANY(%s)",
        (TEST_TICKERS,)
    )
    found_tickers = {r[0] for r in cur.fetchall()}
    for ticker in TEST_TICKERS:
        check(f"{ticker} present in stocks table", ticker in found_tickers)

    # stock_timeseries has OHLCV rows for each ticker
    for ticker in TEST_TICKERS:
        cur.execute(
            "SELECT COUNT(*) FROM stock_timeseries WHERE ticker = %s",
            (ticker,)
        )
        count = cur.fetchone()[0]
        check(f"{ticker}: OHLCV rows in stock_timeseries ({count} rows)",
              count > 0, f"got {count}")

    # collection_log has entries for all ticker×source combos
    cur.execute(
        "SELECT ticker, source, status FROM collection_log "
        "WHERE ticker = ANY(%s) AND source = ANY(%s)",
        (TEST_TICKERS, SOURCES)
    )
    log_entries = {(r[0], r[1]): r[2] for r in cur.fetchall()}
    for ticker in TEST_TICKERS:
        for source in SOURCES:
            key = (ticker, source)
            check(f"collection_log has entry for {ticker}/{source}",
                  key in log_entries,
                  f"no entry found")

    cur.close()
    pg.close()

except Exception as e:
    print(f"  [FAIL] PostgreSQL checks failed: {e}")
    results.extend([False] * (len(TEST_TICKERS) * (2 + len(SOURCES))))

# ── Step 5: Cassandra check ────────────────────────────────────
print("\n[DB] Cassandra short_volume_daily")
try:
    if fin_col._cass_session is not None:
        for ticker in TEST_TICKERS:
            rows = fin_col._cass_session.execute(
                "SELECT ticker, short_volume FROM short_volume_daily "
                "WHERE ticker = %s LIMIT 1", (ticker,)
            )
            result_rows = list(rows)
            check(f"{ticker}: row in Cassandra short_volume_daily",
                  len(result_rows) > 0,
                  "(FINRA may not have listed this ticker)")
    else:
        print("  (Cassandra unavailable — skipping)")

except Exception as e:
    print(f"  [FAIL] Cassandra checks failed: {e}")
    results.extend([False] * len(TEST_TICKERS))

# ── Summary ───────────────────────────────────────────────────
print("\n" + "="*60)
passed = sum(results)
total  = len(results)
print(f"  {passed}/{total} assertions passed  ({elapsed:.1f}s total)")
if passed == total:
    print("  WEEK 1 INTEGRATION TEST — ALL PASSED")
else:
    print("  WEEK 1 INTEGRATION TEST — SOME FAILURES (see above)")
print("="*60 + "\n")
sys.exit(0 if passed == total else 1)
