"""
Blue Sky Breakout — EOD virtual portfolio runner  ->  bluesky.positions/trades/equity_curve
--------------------------------------------------------------------------------------------
Runs AFTER bluesky_screener_d.py each evening. For each variant (₹10L start), it:
  1. Reads its open book (bluesky.positions) and current cash (from the last equity_curve row).
  2. EXITS on today's close — the paper-trading rule set:
        • breakeven lock: once a position is +8% (1R), raise its stop to entry.
        • hard stop: CLOSE <= stop  -> exit at close.                 (stop_8pct / stop_be)
        • trail:     CLOSE < 50-DMA -> exit at close.                 (below_50d)
  3. ENTRIES: fresh breakouts from bluesky.daily_candidates (today's close cleared the PRIOR
     session's pivot), highest-RS first, sized by risk, filled at the pivot, up to 10 slots /
     available cash.
  4. Writes the updated book, realised trades, and the daily NAV row — all to the bluesky schema.

Two variants, identical rules except risk/trade:  A = 1.5% risk,  B = 1.0% risk.

Usage:
    python bluesky_portfolio_runner_d.py                # process today, write to Supabase
    python bluesky_portfolio_runner_d.py --preview-only # compute + print, no DB write
Requires: supabase, tradingview-screener, pandas, python-dotenv.
"""
from __future__ import annotations
import os, sys, json, argparse, logging, pathlib
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
from supabase import create_client
from tradingview_screener import Query, col
from dotenv import load_dotenv

ROOT = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env.local"); load_dotenv(ROOT / "scripts" / ".env.local")
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
                or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY"))
SCHEMA = "bluesky"

START_CAPITAL = 1_000_000.0
STOP_PCT      = 0.08
BE_TRIGGER    = 0.08      # +8% (1R) -> stop to breakeven
MAX_POS       = 10
MAX_FRAC      = 0.30
COOLDOWN_D    = 5

VARIANTS = {                                   # variant_id -> config (the two paper-trading books)
    "A": dict(name="Blue Sky — 1.5% risk", risk_pct=1.5),
    "B": dict(name="Blue Sky — 1.0% risk", risk_pct=1.0),
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("bluesky.pf")


def fetch_prices() -> dict:
    """ticker -> (close, sma50, high) for the whole NSE mainboard (for exits on held names)."""
    px = {}
    for offset in (0, 2000, 4000):
        try:
            _n, df = (Query().set_markets("india").select("name", "close", "SMA50", "high")
                      .where(col("exchange") == "NSE", col("is_primary") == True,
                             col("type") == "stock", col("subtype") == "common",
                             col("market_cap_basic") > 0)
                      .limit(2000).offset(offset).get_scanner_data())
        except Exception:
            break
        if df is None or df.empty:
            break
        for _, r in df.iterrows():
            px[r["name"]] = (r.get("close"), r.get("SMA50"), r.get("high"))
        if len(df) < 2000:
            break
    return px


def sb_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def ensure_variants(sb):
    rows = [dict(variant_id=v, name=c["name"], start_capital=START_CAPITAL,
                 risk_pct=c["risk_pct"], stop_pct=STOP_PCT * 100, stop_ref="entry",
                 entry_mode="at_pivot", max_positions=MAX_POS, max_position_frac=MAX_FRAC * 100,
                 notes="Paper-trading rule set: closing -8% stop from entry, 50-DMA trail, "
                       "breakeven lock at +8%.")
            for v, c in VARIANTS.items()]
    sb.schema(SCHEMA).table("variants").upsert(rows, on_conflict="variant_id").execute()


def load_book(sb, variant):
    pos = sb.schema(SCHEMA).table("positions").select("*").eq("variant_id", variant).execute().data
    eq = (sb.schema(SCHEMA).table("equity_curve").select("*").eq("variant_id", variant)
          .order("as_of", desc=True).limit(1).execute().data)
    cash = float(eq[0]["cash"]) if eq else START_CAPITAL
    last_date = eq[0]["as_of"] if eq else None
    return {p["ticker"]: p for p in pos}, cash, last_date


def run_variant(sb, variant, cfg, prices, today_c, prior_pivot, run_date, do_write):
    pos, cash, last_date = load_book(sb, variant)
    if last_date is not None and str(last_date) >= run_date:
        log.info("  %s: already processed through %s — skip", variant, last_date); return None
    risk = cfg["risk_pct"] / 100.0
    closed, opened = [], []

    # ---- exits (paper-trading rules, on the close) ----
    for tkr in list(pos):
        p = pos[tkr]
        c, s50, hi = prices.get(tkr, (None, None, None))
        if c is None or c != c:
            continue
        entry, stop, shares, be = float(p["entry_price"]), float(p["stop_price"]), int(p["shares"]), p["be_locked"]
        if not be and hi is not None and hi == hi and hi >= entry * (1 + BE_TRIGGER):
            stop = max(stop, entry); be = True
        reason = None
        if c <= stop:
            reason = "stop_be" if be else "stop_8pct"
        elif s50 == s50 and c < s50:
            reason = "below_50d"
        if reason:
            cash += c * shares
            closed.append(dict(variant_id=variant, ticker=tkr, entry_date=p["entry_date"],
                               entry_price=entry, shares=shares, exit_date=run_date, exit_price=round(c, 2),
                               return_pct=round((c / entry - 1) * 100, 2), pnl=round((c - entry) * shares, 2),
                               exit_reason=reason))
            pos.pop(tkr)
        else:
            p["stop_price"], p["be_locked"], p["last_price"] = round(stop, 2), be, round(c, 2)

    # ---- entries: fresh breakouts, highest-RS first ----
    held = set(pos) | {t["ticker"] for t in closed}
    cands = []
    for r in today_c:
        t = r["ticker"]
        if t in held:
            continue
        c = prices.get(t, (None, None, None))[0]
        if c is None or c != c:
            c = float(r["close"])
        piv = prior_pivot.get(t)
        breakout = (piv is not None and c >= piv) if piv is not None else (r["status"] == "TRIGGERED")
        if breakout:
            cands.append((t, int(r["rs"]), float(piv if piv is not None else r["pivot"])))
    cands.sort(key=lambda x: x[1], reverse=True)
    n_passed = 0
    for tkr, rs, pivot in cands:
        if len(pos) >= MAX_POS:
            n_passed += 1; continue
        equity = cash + sum(int(pp["shares"]) * (prices.get(k, (pp["last_price"],))[0] or float(pp["entry_price"]))
                            for k, pp in pos.items())
        target = min(risk / STOP_PCT * equity, MAX_FRAC * equity, cash)
        shares = int(target // pivot)
        if shares <= 0:
            n_passed += 1; continue
        cash -= shares * pivot
        pos[tkr] = dict(variant_id=variant, ticker=tkr, entry_date=run_date, entry_price=round(pivot, 2),
                        shares=shares, stop_price=round(pivot * (1 - STOP_PCT), 2), pivot=round(pivot, 2),
                        be_locked=False, last_price=round(pivot, 2))
        opened.append(tkr)

    # ---- NAV ----
    pos_val = sum(int(p["shares"]) * (prices.get(k, (p["last_price"],))[0] or float(p["entry_price"]))
                  for k, p in pos.items())
    equity = cash + pos_val
    summ = dict(variant=variant, equity=round(equity), cash=round(cash), n_open=len(pos),
                opened=opened, closed=[c["ticker"] for c in closed], n_passed=n_passed)

    if do_write:
        # replace the open book
        sb.schema(SCHEMA).table("positions").delete().eq("variant_id", variant).execute()
        if pos:
            clean = [{k: v for k, v in p.items() if k != "id"} for p in pos.values()]
            sb.schema(SCHEMA).table("positions").insert(clean).execute()
        if closed:
            sb.schema(SCHEMA).table("trades").insert(closed).execute()
        sb.schema(SCHEMA).table("equity_curve").upsert(dict(
            variant_id=variant, as_of=run_date, cash=round(cash, 2),
            positions_value=round(pos_val, 2), equity=round(equity, 2),
            n_open=len(pos), n_passed=n_passed,
            updated_at=datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()),
            on_conflict="variant_id,as_of").execute()
    return summ


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--preview-only", action="store_true")
    args = ap.parse_args()
    run_date = datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()
    log.info("=== BLUE SKY PORTFOLIO RUNNER — %s ===", run_date)
    sb = sb_client()
    if not args.preview_only:
        ensure_variants(sb)

    # candidates: today's list + the prior session's pivots (for breakout detection)
    dc = sb.schema(SCHEMA).table("daily_candidates")
    dates = sorted({r["run_date"] for r in
                    dc.select("run_date").order("run_date", desc=True).limit(2000).execute().data},
                   reverse=True)
    today_run = dates[0] if dates else run_date
    today_c = dc.select("*").eq("run_date", today_run).execute().data
    prior_pivot = {}
    if len(dates) > 1:
        for r in dc.select("ticker,pivot").eq("run_date", dates[1]).execute().data:
            prior_pivot[r["ticker"]] = float(r["pivot"])
    log.info("candidates: %d today (%s), prior-session pivots: %d", len(today_c), today_run, len(prior_pivot))

    prices = fetch_prices()
    log.info("prices fetched for %d names", len(prices))

    summaries = []
    for v, cfg in VARIANTS.items():
        s = run_variant(sb, v, cfg, prices, today_c, prior_pivot, run_date, not args.preview_only)
        if s:
            summaries.append(s)
            log.info("  %s (%.1f%% risk): equity ₹%s | open %d | entered %s | exited %s | passed %d",
                     v, cfg["risk_pct"], f"{s['equity']:,}", s["n_open"], s["opened"] or "-",
                     s["closed"] or "-", s["n_passed"])

    if not args.preview_only:
        pathlib.Path("results").mkdir(exist_ok=True)
        pathlib.Path("results/bluesky_portfolio.json").write_text(json.dumps(dict(
            script="Blue Sky Portfolio", succeeded=len(summaries), failed=0, skipped=0,
            total=len(VARIANTS), errors=[])))
        try:
            from notify import notify_summary
            notify_summary("bluesky_portfolio_runner_d",
                           [(f"Var {s['variant']} equity", f"₹{s['equity']:,}") for s in summaries])
        except Exception:
            pass
    log.info("done.")


if __name__ == "__main__":
    main()
