"""
Rebuild the ST2 model portfolio (model.*) across a gap, then hand back to the
daily runner.

The daily runner can only act on weeks whose top-50 ranking is still stored in
strategies.momentum_weekly_history, which keeps 30 days. For older weeks this
script re-ranks the N500 as of each basis date with the weekly scanner's own
maths (52w performance / 52w volatility on TradingView daily closes) and feeds
that ranking to the runner. A week the scanner never announced (a Friday
holiday, before the basis-day fix) gets its basis session and Supertrend regime
computed the same way. Nothing is written to strategies.*; reconstructed weeks
exist only for this run and are flagged in the log and the run's events.

Caveats, also printed at the end of every run:
  * The universe is today's universe.n500, not the membership on each basis
    date (no point-in-time NIFTY 500 list exists for 2026). Names that joined
    or left at the Sep 2026 reconstitution can differ.
  * A reconstructed ranking is what the strategy would have chosen from the
    data, not a record of what the live scanner did choose. The fidelity check
    re-ranks the latest basis date that IS stored and reports the overlap.

Usage:
    python scripts/backfill_model_portfolio.py --dry-run      # simulate, write nothing to model.*
    python scripts/backfill_model_portfolio.py                # last NAV + 1 -> today
    python scripts/backfill_model_portfolio.py --end 2026-10-14
"""
import os
import sys
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import model_portfolio_runner_d as R          # noqa: E402
import momentum_weekly_scanner as W           # noqa: E402

log = logging.getLogger("backfill_model_portfolio")
sb = R.sb

# A real (non dry-run) backfill refuses to write when the reconstruction looks
# degraded: too many universe fetches failed (TradingView throttling), or the
# re-ranked newest stored week disagrees with what the live scanner chose.
MAX_FETCH_FAILURES = 10
MIN_FIDELITY = 0.9


def _sessions(start, end):
    R.fetch_and_cache_benchmark()
    res = sb.schema("model").table("benchmark_history_cache").select("date") \
        .gte("date", start).lte("date", end).order("date").execute()
    return [r["date"] for r in res.data]


def _next_execution_day(basis):
    b = pd.Timestamp(basis)
    ahead = (W.REBALANCE_EXECUTION_DOW - b.weekday()) % 7 or 7
    return (b + pd.Timedelta(days=ahead)).date().isoformat()


def _has_history(basis_date):
    res = sb.schema("strategies").table("momentum_weekly_history").select("ticker") \
        .eq("asof_date", basis_date).limit(1).execute()
    return bool(res.data)


def _regime_asof(basis_date):
    df = W.fetch_history(W.REGIME_INDEX_SYMBOL, W.REGIME_INDEX_EXCHANGE,
                         n_bars=W.REGIME_N_BARS, interval=W.REGIME_TIMEFRAME)
    df = df[df.index.normalize() <= pd.Timestamp(basis_date)]   # weekly bars are stamped Monday
    return W.evaluate_regime_filter(df)


def _fetch_universe_closes():
    tickers = W.get_n500_tickers(sb)["ticker"].tolist()
    closes, failed = {}, []
    with ThreadPoolExecutor(max_workers=W.MAX_WORKERS) as pool:
        futs = {pool.submit(W.fetch_history, t, "NSE"): t for t in tickers}
        for i, fut in enumerate(as_completed(futs), 1):
            t = futs[fut]
            try:
                df = fut.result()
                closes[t] = df["close"].set_axis(df.index.normalize())
            except Exception as exc:
                failed.append(t)
                log.warning("skip %s: %s", t, exc)
            if i % 100 == 0:
                log.info("  fetched %d/%d", i, len(tickers))
    log.info("Universe closes: %d fetched, %d failed (%s)", len(closes), len(failed), ", ".join(failed[:10]))
    return closes, failed


def _rank_asof(closes, basis_date):
    cutoff = pd.Timestamp(basis_date)
    rows = []
    for t, c in closes.items():
        perf, vol, _ = W.performance_and_volatility(c[c.index <= cutoff], W.LOOKBACK_DAYS)
        if np.isnan(perf) or np.isnan(vol) or vol == 0:
            continue
        rows.append((t, perf / vol))
    rows.sort(key=lambda r: r[1], reverse=True)
    return [{"ticker": t, "rank": i} for i, (t, _) in enumerate(rows, 1)]


def build_basis_override(start, end):
    sessions = _sessions((pd.Timestamp(start) - pd.Timedelta(days=21)).date().isoformat(), end)
    schedule = R.load_rebalance_schedule()
    in_range = [s for s in schedule if start <= s["execution_date"] <= end]
    announced_weeks = {pd.Timestamp(s["basis_date"]).to_period("W") for s in schedule}

    override = {}
    # 1. weeks the scanner never announced -> the last session of that week is the basis
    by_week = {}
    for d in sessions:
        by_week.setdefault(pd.Timestamp(d).to_period("W"), []).append(d)
    for week, days in sorted(by_week.items()):
        basis = max(days)
        execution = _next_execution_day(basis)
        if week in announced_weeks or not (start <= execution <= end):
            continue
        regime = _regime_asof(basis)
        log.info("Unannounced week: basis %s (%s) -> regime %s, executes %s",
                 basis, pd.Timestamp(basis).strftime("%a"), regime["regime_action"], execution)
        override[basis] = {"execution_date": execution, "regime_action": regime["regime_action"],
                           "computed_regime": True}

    # 2. invested weeks whose ranking is no longer stored -> re-rank as of the basis date
    need = [b for b, o in override.items() if o["regime_action"] != W.REGIME_ACTION_ON_BEARISH]
    need += [s["basis_date"] for s in in_range
             if (s["regime_action"] or "INVEST") != W.REGIME_ACTION_ON_BEARISH]
    need = sorted({b for b in need if not _has_history(b)})
    if not need:
        log.info("Every invested week in range has a stored ranking -- nothing to reconstruct.")
        return override, None, []

    log.info("Reconstructing rankings for %d basis date(s): %s", len(need), ", ".join(need))
    closes, failed = _fetch_universe_closes()
    for b in need:
        picks = _rank_asof(closes, b)
        sched = next((s for s in schedule if s["basis_date"] == b), None)
        entry = override.setdefault(b, {"execution_date": sched["execution_date"],
                                        "regime_action": sched["regime_action"]})
        entry["picks"] = picks
        log.info("  %s: %d ranked · top 10 %s", b, len(picks), " ".join(p["ticker"] for p in picks[:10]))

    # Fidelity check: re-rank the newest basis date that IS stored and compare top 50s
    fidelity = None
    stored = [s["basis_date"] for s in schedule if _has_history(s["basis_date"])]
    if stored:
        ref = max(stored)
        live = sb.schema("strategies").table("momentum_weekly_history").select("ticker,rank") \
            .eq("asof_date", ref).lte("rank", 50).execute().data
        live_top = {r["ticker"] for r in live}
        rebuilt_top = {p["ticker"] for p in _rank_asof(closes, ref) if p["rank"] <= 50}
        fidelity = (ref, len(live_top & rebuilt_top), len(live_top))
        log.info("Fidelity check vs stored ranking %s: %d/%d of the live top 50 reproduced", *fidelity)

    problems = []
    if len(failed) > MAX_FETCH_FAILURES:
        problems.append(f"{len(failed)} universe fetches failed (max {MAX_FETCH_FAILURES})")
    if fidelity is None:
        problems.append("no stored week to check the reconstruction against")
    elif fidelity[2] and fidelity[1] / fidelity[2] < MIN_FIDELITY:
        problems.append(f"fidelity {fidelity[1]}/{fidelity[2]} below {MIN_FIDELITY:.0%}")
    return override, fidelity, problems


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", help="first date (default: day after the last recorded model NAV)")
    ap.add_argument("--end", default=R._now_ist().strftime("%Y-%m-%d"), help="last date (default: today IST)")
    ap.add_argument("--dry-run", action="store_true", help="simulate; write nothing to model.*")
    args = ap.parse_args()

    start = args.start
    if start is None:
        last = R.last_recorded_nav_date()
        start = (pd.Timestamp(last) + pd.Timedelta(days=1)).date().isoformat() if last else args.end
    if start > args.end:
        log.info("Model portfolio is already up to date (next date to record: %s).", start)
        return
    log.info("=== MODEL PORTFOLIO BACKFILL %s -> %s%s ===", start, args.end, " (DRY RUN)" if args.dry_run else "")

    override, fidelity, problems = build_basis_override(start, args.end)
    if problems and not args.dry_run:
        for msg in problems:
            log.error("Reconstruction not trustworthy: %s", msg)
        log.error("Nothing written. Re-run later (or inspect with --dry-run).")
        sys.exit(1)
    for msg in problems:
        log.warning("Reconstruction would be refused for a real run: %s", msg)

    sim_state = None
    if args.dry_run:
        port = sb.schema("model").table("portfolios").select("*").eq("strategy_key", "momentum_weekly").execute().data[0]
        holdings = sb.schema("model").table("holdings").select("*").eq("portfolio_id", port["id"]).eq("status", "open").execute().data
        sim_state = {"cash": port["remaining_cash"], "holdings": holdings,
                     "last_rebalanced_at": port.get("last_rebalanced_at")}

    summary = R.run_catchup(args.end, args.dry_run, start_date=start,
                            basis_override=override, sim_state=sim_state)

    log.info("=" * 70)
    log.info("BACKFILL SUMMARY %s -> %s%s", start, args.end, " (DRY RUN)" if args.dry_run else "")
    log.info("  sessions written : %d", summary["succeeded"])
    log.info("  skipped          : %d (holidays / unsettled / already recorded)", summary["skipped"])
    log.info("  failed           : %d", summary["failed"])
    for e in summary["errors"]:
        log.info("  · %s", e)
    nav_path = (summary.get("sim_state") or {}).get("nav_history") or []
    if nav_path:
        log.info("  simulated NAV path (date, nav, nifty50, nifty500):")
        for row in nav_path:
            log.info("    %s", row)
    log.info("  reconstructed rankings : %s",
             ", ".join(sorted(b for b, o in override.items() if o.get("picks"))) or "none")
    log.info("  computed regimes       : %s", ", ".join(
        f"{b}={o['regime_action']}" for b, o in sorted(override.items()) if o.get("computed_regime")) or "none")
    if fidelity:
        log.info("  fidelity check         : %s re-ranked -> %d/%d of the live top 50", *fidelity)
    log.info("  CAVEAT: universe = today's universe.n500 (no point-in-time membership for 2026);")
    log.info("          reconstructed weeks are what the strategy would have picked, not a live record.")
    log.info("=" * 70)
    if summary["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
