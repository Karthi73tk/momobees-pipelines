"""
Offline regression tests for sar_pipeline.py's time handling. No network, no DB.

    python tests/test_sar_timezones.py

Guards three bugs found 2026-10-04:
  1. tvDatafeed stamps bars in the MACHINE's timezone. On a UTC GitHub runner
     the whole NSE session arrived as 03:45–10:00 and collapsed into ONE "09:15"
     75-min bar per day — from 15 Sep the forward test ran on daily candles.
  2. Bar times were written to timestamptz without an offset, so Postgres
     stored IST wall-clock as UTC (every stored bar 5h30m off).
  3. A multi-bar run stamped every equity_curve row with the FINAL
     cumulative points instead of each bar's own.
"""
import os, sys, time, types, pathlib
from datetime import datetime, timezone, timedelta

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "scripts"))
for mod in ("supabase", "dotenv"):            # keep the test dependency-free
    try:
        __import__(mod)
    except ImportError:
        m = types.ModuleType(mod)
        m.create_client = lambda *a, **k: None
        m.load_dotenv = lambda *a, **k: None
        sys.modules[mod] = m

import pandas as pd
import sar_pipeline as S

IST_OFF = timezone(timedelta(hours=5, minutes=30))
failures = 0


def check(label, cond, detail=""):
    global failures
    failures += not cond
    print(f"{'✅' if cond else '❌'} {label}{'' if cond else ' — ' + str(detail)}")


def tv_bars(machine_tz, days=("2026-09-28", "2026-09-29")):
    """15-min bars exactly as tvDatafeed builds them: fromtimestamp(epoch) in
    the machine's local zone, returned naive."""
    rows = []
    for d in days:
        start = datetime.fromisoformat(d + "T09:15:00").replace(tzinfo=IST_OFF)
        for i in range(25):                                   # 09:15 .. 15:15
            epoch = (start + timedelta(minutes=15 * i)).timestamp()
            local = datetime.fromtimestamp(epoch, tz=machine_tz).replace(tzinfo=None)
            px = 25000 + i
            rows.append(dict(datetime=local, open=px, high=px + 5, low=px - 5, close=px, volume=1))
    return pd.DataFrame(rows).set_index("datetime")


def run_fetch(machine_tz, bars):
    fake = types.ModuleType("tvDatafeed")
    class TvDatafeed:
        def get_hist(self, *a, **k):
            return bars
    fake.TvDatafeed = TvDatafeed
    fake.Interval = types.SimpleNamespace(in_15_minute="15")
    sys.modules["tvDatafeed"] = fake
    S._machine_tz = lambda: machine_tz
    return S.fetch_75min()


print("Running SAR timezone tests...\n")

# ── 1. reconstruction is machine-timezone independent ──────────────────────
expected = ["09:15", "10:30", "11:45", "13:00", "14:15"]
for name, tz in (("UTC runner", timezone.utc), ("IST laptop", IST_OFF)):
    df = run_fetch(tz, tv_bars(tz))
    per_day = df.groupby(df.index.date).apply(lambda g: [t.strftime("%H:%M") for t in g.index])
    check(f"{name}: 5 bars per day at the IST bucket starts",
          all(v == expected for v in per_day), dict(per_day))
    first = df.iloc[0]
    check(f"{name}: 09:15 bar spans 09:15–10:15 only (open 25000, close 25004)",
          (first["open"], first["close"]) == (25000, 25004), (first["open"], first["close"]))

# The old code on a UTC runner: prove the test actually catches the bug.
old_bucket_input = tv_bars(timezone.utc)["close"].index.time
collapsed = {S.BUCKETS[0] if t < S.BUCKETS[1] else t for t in old_bucket_input}
check("old behaviour reproduced: UTC-stamped session falls almost entirely before 10:30",
      sum(t < S.BUCKETS[1] for t in old_bucket_input) >= 0.9 * len(old_bucket_input))

# A wrong timezone must fail loudly, not degrade to daily bars.
bad = tv_bars(timezone.utc)
try:
    S._machine_tz = lambda: IST_OFF            # lie: UTC bars treated as IST
    fake = sys.modules["tvDatafeed"]
    fake.TvDatafeed.get_hist = lambda self, *a, **k: bad
    S.fetch_75min()
    check("mis-zoned feed raises instead of collapsing", False, "no exception")
except RuntimeError as e:
    check("mis-zoned feed raises instead of collapsing", "outside the 09:15" in str(e), e)

# ── 2. timestamps round-trip as real instants ──────────────────────────────
iso = S._iso(pd.Timestamp("2026-10-01 14:15"))
check("_iso writes an explicit +05:30", iso == "2026-10-01T14:15:00+05:30", iso)
as_stored = pd.Timestamp(iso).tz_convert("UTC").isoformat()        # what PostgREST returns
check("stored instant is 08:45Z", as_stored == "2026-10-01T08:45:00+00:00", as_stored)
check("_naive(stored) recovers IST wall-clock 14:15",
      S._naive(as_stored) == pd.Timestamp("2026-10-01 14:15"), S._naive(as_stored))
check("_iso is idempotent on aware input", S._iso(as_stored) == iso, S._iso(as_stored))
check("_iso/_naive pass None through", S._iso(None) is None and S._naive(None) is None)

# ── 3. per-bar cumulative points ───────────────────────────────────────────
idx = pd.DatetimeIndex(pd.to_datetime(
    ["2026-10-01 09:15", "2026-10-01 10:30", "2026-10-01 11:45"]), name="ts")
# LONG book; bar 2 closes below both EMAs -> SAR-Reverse (realises points)
df = pd.DataFrame({"close": [25100.0, 24800.0, 24700.0],
                   "ema_fast": [25000.0, 24950.0, 24900.0],
                   "ema_slow": [24900.0, 24900.0, 24850.0]}, index=idx)
out = S.advance(df, "LONG", pd.Timestamp("2026-09-30 10:30"), 25000.0, 2,
                1_000_000.0, 0.0, pd.Timestamp("2026-09-30 14:15"), 1_000_000.0)
eq_points, net_final = out[7], out[6]
per_bar = [p[4] for p in eq_points]
check("each equity point carries 5 fields", all(len(p) == 5 for p in eq_points))
check("bar before the close keeps the pre-trade cumulative (0)", per_bar[0] == 0.0, per_bar)
check("bars from the close onward carry the realised cumulative",
      per_bar[1] == per_bar[2] == net_final and net_final != 0.0, per_bar)

print("\nAll SAR timezone tests passed!" if not failures else f"\n{failures} failure(s).")
sys.exit(1 if failures else 0)
