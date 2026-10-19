import os
import sys
import argparse
import time
import logging
from datetime import datetime, timedelta, timezone, time as dtime
import pandas as pd
import json

from supabase import create_client
from dotenv import load_dotenv
from tvDatafeed import TvDatafeed, Interval

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("model_portfolio_runner")

env_path = os.path.join(os.path.dirname(__file__), "..", ".env.local")
load_dotenv(env_path)
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Missing Supabase credentials in .env.local")
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
tv = TvDatafeed()

IST = timezone(timedelta(hours=5, minutes=30))
SESSION_SETTLED = dtime(15, 45)   # NSE closes 15:30 IST; after this a daily bar is final


def _now_ist():
    return datetime.now(IST)


def _session_settled(date_str):
    """False only for today's session before the close -- its daily bar is still live."""
    now = _now_ist()
    return date_str < now.date().isoformat() or now.time() >= SESSION_SETTLED


def _drop_unsettled_bar(df):
    """Never cache today's in-progress daily bar as if it were a close (a manual
    dispatch during market hours would otherwise pin an intraday print as EOD)."""
    if not df.empty and not _session_settled(df.index[-1].strftime("%Y-%m-%d")):
        return df.iloc[:-1]
    return df

def fetch_and_cache_ticker(ticker, exchange, min_bars=400):
    for attempt in range(1, 4):
        try:
            df = tv.get_hist(symbol=ticker, exchange=exchange, interval=Interval.in_daily, n_bars=min_bars)
            if df is None or df.empty:
                if attempt < 3:
                    time.sleep(2)
                    continue
                return
            df.index = df.index.normalize()
            df = _drop_unsettled_bar(df)
            rows = []
            for date, row in df.iterrows():
                rows.append({
                    "ticker": ticker,
                    "date": date.strftime("%Y-%m-%d"),
                    "close": float(row["close"])
                })
            if rows:
                for i in range(0, len(rows), 500):
                    sb.schema("model").table("price_history_cache").upsert(rows[i:i+500], on_conflict="ticker,date").execute()
            return
        except Exception as e:
            if attempt < 3:
                log.warning(f"Error fetching and caching {ticker} (attempt {attempt}/3): {e}. Retrying in 2s...")
                time.sleep(2)
            else:
                log.warning(f"Error fetching and caching {ticker} after 3 attempts: {e}")

def fetch_price(ticker, exchange, date_str):
    # Check cache first
    res = sb.schema("model").table("price_history_cache").select("close").eq("ticker", ticker).eq("date", date_str).execute()
    if res.data:
        return res.data[0]["close"]
    
    # Fetch from TV and cache
    fetch_and_cache_ticker(ticker, exchange)
    
    # Check again
    res = sb.schema("model").table("price_history_cache").select("close").eq("ticker", ticker).eq("date", date_str).execute()
    if res.data:
        return res.data[0]["close"]
    
    log.warning(f"Price for {ticker} not found for {date_str} even after TV fetch.")
    return None

def fetch_and_cache_benchmark(min_bars=400):
    try:
        df_n50 = tv.get_hist(symbol="NIFTY", exchange="NSE", interval=Interval.in_daily, n_bars=min_bars)
        df_n500 = tv.get_hist(symbol="CNX500", exchange="NSE", interval=Interval.in_daily, n_bars=min_bars)
        if df_n50 is None or df_n50.empty or df_n500 is None or df_n500.empty:
            return
        df_n50.index = df_n50.index.normalize()
        df_n500.index = df_n500.index.normalize()
        df_merged = _drop_unsettled_bar(pd.merge(df_n50['close'], df_n500['close'], left_index=True, right_index=True, suffixes=('_n50', '_n500')))
        
        rows = []
        for date, row in df_merged.iterrows():
            rows.append({
                "date": date.strftime("%Y-%m-%d"),
                "nifty50_close": float(row["close_n50"]),
                "nifty500_close": float(row["close_n500"])
            })
        if rows:
            for i in range(0, len(rows), 500):
                sb.schema("model").table("benchmark_history_cache").upsert(rows[i:i+500], on_conflict="date").execute()
    except Exception as e:
        log.warning(f"Error fetching benchmark: {e}")

def fetch_benchmarks(date_str):
    for attempt in range(1, 4):
        try:
            res = sb.schema("model").table("benchmark_history_cache").select("*").eq("date", date_str).execute()
            if res.data:
                return res.data[0]["nifty50_close"], res.data[0]["nifty500_close"]
            
            fetch_and_cache_benchmark()
            
            res = sb.schema("model").table("benchmark_history_cache").select("*").eq("date", date_str).execute()
            if res.data:
                return res.data[0]["nifty50_close"], res.data[0]["nifty500_close"]
            return None, None
        except Exception as e:
            if attempt < 3:
                log.warning(f"Error checking benchmark cache (attempt {attempt}/3): {e}. Retrying...")
                time.sleep(2)
            else:
                return None, None

def fetch_last_known_price(ticker, before_date_str):
    res = sb.schema("model").table("price_history_cache").select("date,close") \
        .eq("ticker", ticker).lt("date", before_date_str).order("date", desc=True).limit(1).execute()
    if res.data:
        return res.data[0]
    return None

def fetch_prices_batch(tickers, exchange, date_str):
    prices = {}
    tickers = list(tickers)
    if tickers:
        res = sb.schema("model").table("price_history_cache").select("ticker,close") \
            .in_("ticker", tickers).eq("date", date_str).execute()
        prices = {r["ticker"]: float(r["close"]) for r in res.data}
    for t in tickers:
        if t in prices:
            continue
        p = fetch_price(t, exchange, date_str)
        if p is not None:
            prices[t] = float(p)
    return prices


def load_rebalance_schedule(basis_override=None):
    """Every weekly rebalance the scanner has announced, oldest first.

    strategies.momentum_weekly_regime is the source of truth: it is never purged.
    This runner used to detect rebalance days from momentum_weekly_history alone,
    which keeps only 30 days, so any catch-up older than that saw no rebalances at
    all. History still contributes weeks whose Friday regime check failed.
    `basis_override` (backfill only) fills weeks the scanner never announced."""
    sched = {}
    hist = sb.schema("strategies").table("momentum_weekly_history") \
        .select("asof_date,rebalance_execution_date") \
        .eq("is_rebalance_basis_day", True).eq("rank", 1).execute()
    for r in hist.data:
        if r.get("rebalance_execution_date"):
            sched[r["rebalance_execution_date"]] = {
                "basis_date": r["asof_date"], "execution_date": r["rebalance_execution_date"],
                "regime_action": None}
    reg = sb.schema("strategies").table("momentum_weekly_regime") \
        .select("asof_date,rebalance_execution_date,regime_action").execute()
    for r in reg.data:
        sched[r["rebalance_execution_date"]] = {
            "basis_date": r["asof_date"], "execution_date": r["rebalance_execution_date"],
            "regime_action": r["regime_action"]}
    for basis_date, o in (basis_override or {}).items():
        sched.setdefault(o["execution_date"], {
            "basis_date": basis_date, "execution_date": o["execution_date"],
            "regime_action": o.get("regime_action")})
    return sorted(sched.values(), key=lambda s: s["execution_date"])


def due_rebalance(schedule, run_date, last_rebalanced_at):
    """(rebalance due on run_date or None, previous execution date).

    Due = the latest announced execution date on/before run_date that has not been
    applied yet. Comparing against last_rebalanced_at (not run_date == Monday) means
    a Monday holiday rolls the rebalance to the next session instead of losing it."""
    past = [s for s in schedule if s["execution_date"] <= run_date and s["basis_date"] < run_date]
    if not past:
        return None, None
    latest = past[-1]
    previous = past[-2]["execution_date"] if len(past) > 1 else None
    if last_rebalanced_at and str(last_rebalanced_at)[:10] >= latest["execution_date"]:
        return None, previous
    return latest, previous


def load_picks(basis_date, basis_override=None):
    res = sb.schema("strategies").table("momentum_weekly_history").select("ticker,rank") \
        .eq("asof_date", basis_date).order("rank").execute()
    if res.data:
        return res.data, "history"
    o = (basis_override or {}).get(basis_date)
    if o and o.get("picks"):
        return o["picks"], "reconstructed"
    return [], None

def run_portfolio_for_date(run_date: str, dry_run: bool = False, sim_state: dict = None,
                           schedule: list = None, basis_override: dict = None):
    """
    Executes the rebalance logic for momentum_weekly.
    If sim_state is provided, it reads/writes state to it instead of the database.
    `basis_override` (backfill only) supplies picks for basis dates that
    momentum_weekly_history no longer holds.
    """
    events = []
    
    # 1. Benchmark closes. This used to read benchmark_history_cache directly, and
    # nothing else ever refilled that cache after the 24 Jul 2026 backfill -- so
    # every later day "skipped" as a market holiday and the model froze for months.
    # fetch_benchmarks() refreshes the cache from TradingView on a miss.
    nifty50_close, nifty500_close = fetch_benchmarks(run_date)
    if nifty50_close is None:
        log.warning(f"No NIFTY/CNX500 close for {run_date} even after refreshing the cache -- not a trading session. Skipping.")
        return {"status": "skipped", "reason": "no_session"}
        
    # 2. Get current portfolio state
    if sim_state is not None:
        current_cash = sim_state["cash"]
        remaining_cash = sim_state["cash"]
        open_holdings = sim_state["holdings"]
        target_size = 25
        last_rebalanced_at = sim_state.get("last_rebalanced_at")
    else:
        port_res = sb.schema("model").table("portfolios").select("*").eq("strategy_key", "momentum_weekly").execute()
        if not port_res.data:
            log.error("Portfolio not found for momentum_weekly")
            return {"status": "error", "reason": "Portfolio not found"}
        port = port_res.data[0]
        remaining_cash = port["remaining_cash"]
        target_size = port["target_size"]
        last_rebalanced_at = port.get("last_rebalanced_at")
        
        holdings_res = sb.schema("model").table("holdings").select("*").eq("portfolio_id", port["id"]).eq("status", "open").execute()
        open_holdings = holdings_res.data
    
    # 3. Determine if a rebalance is due today
    if schedule is None:
        schedule = load_rebalance_schedule(basis_override)
    due, previous_rebalance_date = due_rebalance(schedule, run_date, last_rebalanced_at)
    is_rebalance_day = due is not None
    basis_date = due["basis_date"] if due else None
    
    p_sells = []
    p_buys = []
    p_m2m = []
    p_stale_closures = []
    
    all_tickers_to_price = set([h["ticker"] for h in open_holdings])
    
    if is_rebalance_day:
        log.info(f"Rebalance Day! Basis date: {basis_date} (announced for {due['execution_date']})")
        regime_action = due.get("regime_action") or "INVEST"
        
        if regime_action == "GO_CASH":
            log.info("Regime is GO_CASH. Liquidating all equities and buying GOLDBEES.")
            for h in open_holdings:
                if h["ticker"] != "GOLDBEES":
                    p_sells.append({"ticker": h["ticker"], "quantity": h["quantity"], "exit_reason": "go_cash_liquidation"})
                    all_tickers_to_price.add(h["ticker"])
                else:
                    p_m2m.append({"ticker": "GOLDBEES"})
                    all_tickers_to_price.add("GOLDBEES")
            
            if not any(h["ticker"] == "GOLDBEES" for h in open_holdings):
                p_buys.append({"ticker": "GOLDBEES", "quantity": 0}) # Will compute quantity after fetching prices
                all_tickers_to_price.add("GOLDBEES")
        else:
            log.info("Regime is INVEST.")
            # Fetch top 50 picks for basis_date
            history_picks, picks_source = load_picks(basis_date, basis_override)
            if not history_picks:
                # An empty pick list would read as "everything dropped out of the top
                # 50" and liquidate the whole book. Refuse instead.
                msg = (f"NO_PICKS: no ranking stored for basis {basis_date} (momentum_weekly_history "
                       f"keeps 30 days). Run scripts/backfill_model_portfolio.py to rebuild it.")
                log.error(msg)
                return {"status": "error", "reason": msg, "events": events}
            if picks_source == "reconstructed":
                events.append(f"RECONSTRUCTED_PICKS: basis {basis_date} ranked as-of from TradingView history")
            
            top_50 = [p["ticker"] for p in history_picks if p["rank"] <= 50]
            
            # Sell holdings that dropped out of top 50, OR if we hold GOLDBEES
            for h in open_holdings:
                if h["ticker"] == "GOLDBEES" or h["ticker"] not in top_50:
                    p_sells.append({"ticker": h["ticker"], "quantity": h["quantity"], "exit_reason": "strategy_sell"})
                else:
                    p_m2m.append({"ticker": h["ticker"]})
                    
            # Determine how many slots we have to buy
            num_sells = len(p_sells)
            num_open_kept = len(open_holdings) - num_sells
            num_to_buy = target_size - num_open_kept
            
            if num_to_buy > 0:
                # Find valid new picks from top 25
                # We need active tickers. Let's fetch universe.nse_universe
                univ_res = sb.schema("universe").table("nse_universe").select("ticker,is_active,price").execute()
                univ_map = {row["ticker"]: row for row in univ_res.data}
                
                # Only pull candidates up to rank 50
                candidates = [p for p in history_picks if p["rank"] <= 50]
                
                new_buys = []
                for c in candidates:
                    t = c["ticker"]
                    if t in [h["ticker"] for h in open_holdings if h["ticker"] != "GOLDBEES"]:
                        continue # Already holding
                    if univ_map.get(t, {}).get("is_active", False) and (univ_map.get(t, {}).get("price") or 0) >= 1:
                        new_buys.append(t)
                        if len(new_buys) == num_to_buy:
                            break
                
                for t in new_buys:
                    p_buys.append({"ticker": t, "quantity": 0})
                    all_tickers_to_price.add(t)
    else:
        # Not a rebalance day, everything is M2M
        for h in open_holdings:
            p_m2m.append({"ticker": h["ticker"]})
            
    # Fetch prices for all involved tickers
    prices = fetch_prices_batch(list(all_tickers_to_price), "NSE", run_date)
    
    # helper for missing price
    def handle_missing_holding(t, qty):
        # returns (action, data)
        # action in ["stale_close", "m2m", "skip"]
        if t == "GOLDBEES":
            msg = f"URGENT_ALERT: GOLDBEES missing price on {run_date}. This is likely a symbol/feed error, not a delisting."
            log.error(msg)
            events.append(msg)
            # fallback to M2M using current_price to avoid breaking portfolio
            old_price = next((h.get("current_price", 0) for h in open_holdings if h["ticker"] == t), 0)
            return "m2m", {"ticker": t, "current_price": old_price}
            
        last_known = fetch_last_known_price(t, run_date)
        if last_known is None:
            # no history at all? Just skip.
            old_price = next((h.get("current_price", 0) for h in open_holdings if h["ticker"] == t), 0)
            return "m2m", {"ticker": t, "current_price": old_price}
        
        # calculate gap size
        gap_days = (datetime.strptime(run_date, "%Y-%m-%d") - datetime.strptime(last_known["date"], "%Y-%m-%d")).days
            
        if is_rebalance_day and previous_rebalance_date and last_known["date"] < previous_rebalance_date:
            msg = f"STALE_CLOSURE: {t} missed 2+ rebalances. Last date: {last_known['date']} (gap {gap_days} days). Auto-closing."
            log.info(msg)
            events.append(msg)
            return "stale_close", {"ticker": t, "exit_price": last_known["close"], "quantity": qty, "exit_date": last_known["date"]}
        else:
            msg = f"SKIP_TICKER: {t} missing price on {run_date} (gap {gap_days} days). Retaining in M2M."
            log.info(msg)
            events.append(msg)
            return "m2m", {"ticker": t, "current_price": last_known["close"]}

    # Process Sells
    final_sells = []
    final_m2m = []
    
    for s in p_sells:
        t = s["ticker"]
        if t in prices:
            final_sells.append({"ticker": t, "exit_price": prices[t], "quantity": s["quantity"], "exit_reason": s.get("exit_reason", "strategy_sell")})
        else:
            action, data = handle_missing_holding(t, s["quantity"])
            if action == "stale_close":
                p_stale_closures.append(data)
            elif action == "m2m":
                final_m2m.append(data)
            
    # Process M2M
    for m in p_m2m:
        t = m["ticker"]
        if t in prices:
            final_m2m.append({"ticker": t, "current_price": prices[t]})
        else:
            qty = next((h.get("quantity", 0) for h in open_holdings if h["ticker"] == t), 0)
            action, data = handle_missing_holding(t, qty)
            if action == "stale_close":
                p_stale_closures.append(data)
            elif action == "m2m":
                final_m2m.append(data)

    # Calculate available cash for buys
    sell_proceeds = sum([s["quantity"] * s["exit_price"] for s in final_sells])
    stale_proceeds = sum([s["quantity"] * s["exit_price"] for s in p_stale_closures])
    total_cash_for_buys = remaining_cash + sell_proceeds + stale_proceeds
    
    # Process Buys
    final_buys = []
    if len(p_buys) > 0:
        valid_buys = []
        for b in p_buys:
            if b["ticker"] in prices:
                valid_buys.append(b)
            else:
                msg = f"BUY_SKIPPED: {b['ticker']} missing price on {run_date}, excluded from this rebalance."
                log.warning(msg)
                events.append(msg)
        
        if len(valid_buys) > 0:
            cash_per_stock = total_cash_for_buys / len(valid_buys)
            for b in valid_buys:
                t = b["ticker"]
                px = prices[t]
                qty = cash_per_stock / px
                final_buys.append({"ticker": t, "entry_price": px, "quantity": qty})
        else:
            log.warning(f"All buys missed prices on {run_date}. Skipping buys.")
            
    # Execute RPC
    rpc_payload = {
        "p_strategy_key": "momentum_weekly",
        "p_nav_date": run_date,
        "p_sells": final_sells,
        "p_buys": final_buys,
        "p_m2m": final_m2m,
        "p_benchmark_nifty50_nav": float(nifty50_close),
        "p_benchmark_nifty500_nav": float(nifty500_close),
        "p_is_rebalance_day": is_rebalance_day,
        "p_stale_closures": p_stale_closures
    }
    
    if dry_run:
        log.info(f"DRY RUN: Would execute RPC with payload: {rpc_payload}")
        if sim_state is not None:
            # Apply trades to sim_state
            for s in final_sells + p_stale_closures:
                sim_state["holdings"] = [h for h in sim_state["holdings"] if h["ticker"] != s["ticker"]]
                sim_state["cash"] += s["quantity"] * s["exit_price"]
            for b in final_buys:
                sim_state["holdings"].append({"ticker": b["ticker"], "quantity": b["quantity"], "current_price": b["entry_price"]})
                sim_state["cash"] -= b["quantity"] * b["entry_price"]
            for m in final_m2m:
                for h in sim_state["holdings"]:
                    if h["ticker"] == m["ticker"]:
                        h["current_price"] = m["current_price"]
            if is_rebalance_day:
                sim_state["last_rebalanced_at"] = run_date
            nav = sim_state["cash"] + sum(h["quantity"] * (h.get("current_price") or 0) for h in sim_state["holdings"])
            sim_state.setdefault("nav_history", []).append((run_date, round(nav, 2), float(nifty50_close), float(nifty500_close)))
            log.info(f"DRY RUN NAV {run_date}: {nav:,.2f} ({len(sim_state['holdings'])} open, cash {sim_state['cash']:,.2f})")
            return {"status": "success", "sim_state": sim_state, "events": events}
        return {"status": "success", "events": events}

    try:
        res = sb.schema("model").rpc("execute_model_rebalance", rpc_payload).execute()
        # Fallback check just in case postgrest-py version doesn't throw
        if hasattr(res, "error") and res.error:
            log.error(f"RPC Error on {run_date}: {res.error}")
            return {"status": "error", "reason": str(res.error), "events": events}
            
        log.info(f"RPC Response: {res.data}")
        if isinstance(res.data, dict) and res.data.get("status") == "skipped":
            return {"status": "skipped", "reason": res.data.get("reason", "already recorded"), "events": events}
        return {"status": "success", "events": events}
    except Exception as e:
        log.error(f"RPC Exception on {run_date}: {e}")
        return {"status": "error", "reason": str(e), "events": events}


def _has_later_session(date_str):
    res = sb.schema("model").table("benchmark_history_cache").select("date") \
        .gt("date", date_str).limit(1).execute()
    return bool(res.data)


def last_recorded_nav_date():
    port = sb.schema("model").table("portfolios").select("id").eq("strategy_key", "momentum_weekly").execute()
    if not port.data:
        return None
    res = sb.schema("model").table("nav_history").select("nav_date") \
        .eq("portfolio_id", port.data[0]["id"]).order("nav_date", desc=True).limit(1).execute()
    return res.data[0]["nav_date"] if res.data else None


def run_catchup(end_date: str, dry_run: bool = False, start_date: str = None,
                basis_override: dict = None, sim_state: dict = None):
    """Process every weekday after the last recorded NAV up to end_date, in order.

    One run used to touch only today, so a single missed day (a cancelled CI run,
    a feed hiccup) stayed a hole forever and a long outage was never repaired.
    Stops at the first error so a later day is never written on top of a gap."""
    if start_date is None:
        last = last_recorded_nav_date()
        start_date = (pd.Timestamp(last) + pd.Timedelta(days=1)).date().isoformat() if last else end_date
    days = [d.date().isoformat() for d in pd.bdate_range(start_date, end_date)]
    if len(days) > 1:
        log.info(f"Catching up {len(days)} weekday(s): {days[0]} -> {days[-1]}")

    schedule = load_rebalance_schedule(basis_override)
    summary = {"succeeded": 0, "failed": 0, "skipped": 0, "errors": [], "processed": []}
    for d in days:
        if not _session_settled(d):
            log.info(f"{d}: session not closed yet -- leaving it for the EOD run.")
            summary["skipped"] += 1
            continue
        result = run_portfolio_for_date(d, dry_run, sim_state=sim_state, schedule=schedule,
                                        basis_override=basis_override)
        status = result.get("status")
        summary["errors"].extend(f"{d} {e}" for e in result.get("events", []))
        if status == "success":
            summary["succeeded"] += 1
            summary["processed"].append(d)
            if result.get("sim_state") is not None:
                sim_state = result["sim_state"]
        elif status == "skipped":
            summary["skipped"] += 1
            if result.get("reason") == "no_session" and not _has_later_session(d):
                # No close yet for d and nothing after it: a feed outage or an
                # unpublished day, not a holiday. Stop so d is retried next run
                # instead of becoming a permanent hole behind a later NAV.
                log.info(f"{d}: no close published yet -- the next run resumes from here.")
                break
        else:
            summary["failed"] += 1
            summary["errors"].append(f"{d} {result.get('reason', 'error')}")
            break
    summary["sim_state"] = sim_state
    return summary


if __name__ == "__main__":
    import pathlib

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="Last date to process, YYYY-MM-DD (default: today IST)",
                        default=_now_ist().strftime("%Y-%m-%d"))
    parser.add_argument("--dry-run", action="store_true", help="Skip the database RPC execution")
    args = parser.parse_args()

    summary = run_catchup(args.date, args.dry_run)
    succeeded, failed, skipped = summary["succeeded"], summary["failed"], summary["skipped"]
    errors = summary["errors"]
        
    # SPECIAL CASE: URGENT_ALERT for GOLDBEES
    if any("URGENT_ALERT" in e for e in errors):
        failed = max(failed, 1)
        
    out_dir = pathlib.Path("results")
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / "model_portfolio.json"
    
    out_file.write_text(json.dumps({
        "script": "model_portfolio_runner_d.py",
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
        "errors": errors
    }))
    
    if failed > 0:
        sys.exit(1)
