"""
Blue Sky — intraday poller (live quotes via tvDatafeed)
-------------------------------------------------------
Runs every 15-30 min during NSE market hours, BETWEEN the EOD screener/runner jobs.
Uses tvDatafeed LTP (no auth, no subscription) to:

  • mark every open bluesky.position to the live price (last_price),
  • raise a position's stop to breakeven once it is +8% (1R),
  • EXIT a position intraday when the live price hits its −8% stop,
  • ENTER an ARMED candidate intraday the moment its price crosses the pivot
    (the buy-stop fill), highest-RS first, sized by risk, up to 10 slots / cash.

The 50-DMA trail exit stays EOD-only (it's a closing rule). The EOD runner remains
the authoritative daily close: it finalises the day and stamps equity_curve.is_eod=true;
this poller only writes live (is_eod=false) snapshots and never re-finalises a day.

Usage:
    python bluesky_intraday_poller.py            # act only during market hours
    python bluesky_intraday_poller.py --force    # run regardless of the clock (uses last price)
    python bluesky_intraday_poller.py --preview-only
"""
from __future__ import annotations
import os, sys, json, argparse, logging, pathlib, threading, time
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client
from dotenv import load_dotenv

ROOT = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env.local"); load_dotenv(ROOT / "scripts" / ".env.local")
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
                or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY"))
SCHEMA = "bluesky"
START_CAPITAL, STOP_PCT, BE_TRIGGER, MAX_POS, MAX_FRAC = 1_000_000.0, 0.08, 0.08, 10, 0.30
NEAR_PCT = 3.0     # only watch ARMED candidates within this % below the pivot for intraday entries
IST = ZoneInfo("Asia/Kolkata")

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bluesky.intra")

# ── tvDatafeed live LTP (threaded, one client per worker) ────────────────────
_tls = threading.local()
def _tv():
    if not hasattr(_tls, "c"):
        from tvDatafeed import TvDatafeed
        _tls.c = TvDatafeed()
    return _tls.c

def _ltp_one(sym):
    from tvDatafeed import Interval
    for _ in range(3):
        try:
            d = _tv().get_hist(symbol=sym, exchange="NSE", interval=Interval.in_1_minute, n_bars=1)
            if d is not None and not d.empty:
                return sym, float(d["close"].iloc[-1])
        except Exception:
            time.sleep(0.6)
    return sym, None

def fetch_ltp(tickers):
    out = {}
    if not tickers:
        return out
    with ThreadPoolExecutor(max_workers=8) as ex:
        for s, px in ex.map(_ltp_one, tickers):
            if px is not None:
                out[s] = px
    return out


def market_open(now=None):
    now = now or datetime.now(IST)
    return now.weekday() < 5 and dtime(9, 15) <= now.time() <= dtime(15, 35)


def run_variant(sb, variant, risk_pct, ltp, armed, run_date, do_write):
    pos = {p["ticker"]: p for p in
           sb.schema(SCHEMA).table("positions").select("*").eq("variant_id", variant).execute().data}
    eq = (sb.schema(SCHEMA).table("equity_curve").select("*").eq("variant_id", variant)
          .order("as_of", desc=True).limit(1).execute().data)
    cash = float(eq[0]["cash"]) if eq else START_CAPITAL
    exited_today = {t["ticker"] for t in sb.schema(SCHEMA).table("trades").select("ticker")
                    .eq("variant_id", variant).eq("exit_date", run_date).execute().data}
    risk, closed, opened, changed = risk_pct / 100.0, [], [], []

    # exits + breakeven + live mark
    for tkr in list(pos):
        p = pos[tkr]; px = ltp.get(tkr)
        if px is None:
            continue
        entry, stop, sh, be = float(p["entry_price"]), float(p["stop_price"]), int(p["shares"]), p["be_locked"]
        if not be and px >= entry * (1 + BE_TRIGGER):
            stop, be = max(stop, entry), True
        if px <= stop:                                     # intraday hard stop
            cash += px * sh
            closed.append(dict(variant_id=variant, ticker=tkr, entry_date=p["entry_date"], entry_price=entry,
                               shares=sh, exit_date=run_date, exit_price=round(px, 2),
                               return_pct=round((px / entry - 1) * 100, 2), pnl=round((px - entry) * sh, 2),
                               exit_reason="stop_be" if be else "stop_8pct"))
            pos.pop(tkr); exited_today.add(tkr)
        else:
            p["stop_price"], p["be_locked"], p["last_price"] = round(stop, 2), be, round(px, 2)
            changed.append(tkr)

    # intraday entries: ARMED candidate whose live price has crossed its pivot
    held = set(pos)
    fresh = [(c["ticker"], int(c["rs"]), float(c["pivot"])) for c in armed
             if c["ticker"] not in held and c["ticker"] not in exited_today
             and ltp.get(c["ticker"]) is not None and ltp[c["ticker"]] >= float(c["pivot"])]
    fresh.sort(key=lambda x: x[1], reverse=True)
    n_passed = 0
    for tkr, rs, pivot in fresh:
        if len(pos) >= MAX_POS:
            n_passed += 1; continue
        equity = cash + sum(int(p["shares"]) * ltp.get(k, float(p["entry_price"])) for k, p in pos.items())
        target = min(risk / STOP_PCT * equity, MAX_FRAC * equity, cash)
        shares = int(target // pivot)
        if shares <= 0:
            n_passed += 1; continue
        cash -= shares * pivot
        pos[tkr] = dict(variant_id=variant, ticker=tkr, entry_date=run_date, entry_price=round(pivot, 2),
                        shares=shares, stop_price=round(pivot * (1 - STOP_PCT), 2), pivot=round(pivot, 2),
                        be_locked=False, last_price=round(ltp[tkr], 2))
        opened.append(tkr)

    pos_val = sum(int(p["shares"]) * ltp.get(k, float(p["entry_price"])) for k, p in pos.items())
    equity = cash + pos_val
    if do_write:
        sb.schema(SCHEMA).table("positions").delete().eq("variant_id", variant).execute()
        if pos:
            sb.schema(SCHEMA).table("positions").insert(
                [{k: v for k, v in p.items() if k != "id"} for p in pos.values()]).execute()
        if closed:
            sb.schema(SCHEMA).table("trades").insert(closed).execute()
        sb.schema(SCHEMA).table("equity_curve").upsert(dict(
            variant_id=variant, as_of=run_date, cash=round(cash, 2), positions_value=round(pos_val, 2),
            equity=round(equity, 2), n_open=len(pos), n_passed=n_passed, is_eod=False,
            updated_at=datetime.now(IST).isoformat()), on_conflict="variant_id,as_of").execute()
    return dict(variant=variant, equity=round(equity), n_open=len(pos), opened=opened,
                exited=[c["ticker"] for c in closed], be_raised=len(changed))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--preview-only", action="store_true")
    args = ap.parse_args()
    if not args.force and not market_open():
        log.info("market closed — nothing to do (use --force to run anyway)."); return
    run_date = datetime.now(IST).date().isoformat()
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # candidate universe today = the ARMED watchlist; positions need live marks too
    dates = sorted({r["run_date"] for r in sb.schema(SCHEMA).table("daily_candidates")
                    .select("run_date").order("run_date", desc=True).limit(600).execute().data}, reverse=True)
    today_run = dates[0] if dates else run_date
    armed = [c for c in sb.schema(SCHEMA).table("daily_candidates")
             .select("ticker,rs,pivot,status,dist_to_pivot_pct").eq("run_date", today_run).execute().data
             if c["status"] == "ARMED" and c.get("dist_to_pivot_pct") is not None
             and c["dist_to_pivot_pct"] <= NEAR_PCT]
    held = {p["ticker"] for p in sb.schema(SCHEMA).table("positions").select("ticker").execute().data}
    universe = sorted(held | {c["ticker"] for c in armed})
    log.info("=== INTRADAY POLL %s — %d held + %d armed = %d quotes ===",
             datetime.now(IST).strftime("%H:%M"), len(held), len(armed), len(universe))
    ltp = fetch_ltp(universe)
    log.info("LTP fetched for %d/%d names", len(ltp), len(universe))

    summ = []
    for v, risk in (("A", 1.5), ("B", 1.0)):
        s = run_variant(sb, v, risk, ltp, armed, run_date, not args.preview_only)
        summ.append(s)
        log.info("  %s: equity ₹%s | open %d | entered %s | exited %s | be+%d",
                 v, f"{s['equity']:,}", s["n_open"], s["opened"] or "-", s["exited"] or "-", s["be_raised"])

    if not args.preview_only:
        pathlib.Path("results").mkdir(exist_ok=True)
        pathlib.Path("results/bluesky_intraday.json").write_text(json.dumps(dict(
            script="Blue Sky Intraday", succeeded=len(summ), failed=0, skipped=0, total=2, errors=[])))
    log.info("done.")


if __name__ == "__main__":
    main()
