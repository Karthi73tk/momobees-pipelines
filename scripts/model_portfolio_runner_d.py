import os
import sys
import argparse
import time
import logging
from datetime import datetime
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
        df_merged = pd.merge(df_n50['close'], df_n500['close'], left_index=True, right_index=True, suffixes=('_n50', '_n500'))
        
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
    for t in tickers:
        p = fetch_price(t, exchange, date_str)
        if p is not None:
            prices[t] = float(p)
    return prices

def run_portfolio_for_date(run_date: str, dry_run: bool = False, sim_state: dict = None):
    """
    Executes the rebalance logic for momentum_weekly.
    If sim_state is provided, it reads/writes state to it instead of the database.
    """
    events = []
    
    # 1. Fetch benchmark prices
    bench_res = sb.schema("model").table("benchmark_history_cache").select("nifty50_close,nifty500_close").eq("date", run_date).execute()
    if not bench_res.data:
        log.warning(f"Could not fetch benchmark prices for {run_date} (market holiday or data not yet published). Skipping.")
        return {"status": "skipped", "reason": "market_holiday"}
        
    nifty50_close = bench_res.data[0]["nifty50_close"]
    nifty500_close = bench_res.data[0]["nifty500_close"]
        
    # 2. Get current portfolio state
    if sim_state is not None:
        current_cash = sim_state["cash"]
        remaining_cash = sim_state["cash"]
        open_holdings = sim_state["holdings"]
        target_size = 25
    else:
        port_res = sb.schema("model").table("portfolios").select("*").eq("strategy_key", "momentum_weekly").execute()
        if not port_res.data:
            log.error("Portfolio not found for momentum_weekly")
            return {"status": "error", "reason": "Portfolio not found"}
        port = port_res.data[0]
        remaining_cash = port["remaining_cash"]
        target_size = port["target_size"]
        
        holdings_res = sb.schema("model").table("holdings").select("*").eq("portfolio_id", port["id"]).eq("status", "open").execute()
        open_holdings = holdings_res.data
    
    # 3. Determine if it's a rebalance execution day
    hist_res = sb.schema("strategies").table("momentum_weekly_history").select("asof_date").eq("rebalance_execution_date", run_date).limit(1).execute()
    
    is_rebalance_day = len(hist_res.data) > 0
    basis_date = hist_res.data[0]["asof_date"] if is_rebalance_day else None
    
    previous_rebalance_date = None
    if is_rebalance_day:
        prev_res = sb.schema("strategies").table("momentum_weekly_history") \
            .select("rebalance_execution_date").lt("rebalance_execution_date", run_date) \
            .order("rebalance_execution_date", desc=True).limit(1).execute()
        if prev_res.data:
            previous_rebalance_date = prev_res.data[0]["rebalance_execution_date"]
    
    p_sells = []
    p_buys = []
    p_m2m = []
    p_stale_closures = []
    
    all_tickers_to_price = set([h["ticker"] for h in open_holdings])
    
    if is_rebalance_day:
        log.info(f"Rebalance Day! Basis date: {basis_date}")
        # Fetch regime
        regime_res = sb.schema("strategies").table("momentum_weekly_regime").select("regime_action").eq("asof_date", basis_date).execute()
        regime_action = regime_res.data[0]["regime_action"] if regime_res.data else "INVEST"
        
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
            picks_res = sb.schema("strategies").table("momentum_weekly_history").select("ticker,rank").eq("asof_date", basis_date).order("rank").execute()
            history_picks = picks_res.data
            
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
            return {"status": "success", "sim_state": sim_state, "events": events}
        return {"status": "success", "events": events}

    try:
        res = sb.schema("model").rpc("execute_model_rebalance", rpc_payload).execute()
        # Fallback check just in case postgrest-py version doesn't throw
        if hasattr(res, "error") and res.error:
            log.error(f"RPC Error on {run_date}: {res.error}")
            return {"status": "error", "reason": str(res.error), "events": events}
            
        log.info(f"RPC Response: {res.data}")
        return {"status": "success", "events": events}
    except Exception as e:
        log.error(f"RPC Exception on {run_date}: {e}")
        return {"status": "error", "reason": str(e), "events": events}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="YYYY-MM-DD", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--dry-run", action="store_true", help="Skip the database RPC execution")
    args = parser.parse_args()
    
    result = run_portfolio_for_date(args.date, args.dry_run)
    
    status = result.get("status")
    events = result.get("events", [])
    reason = result.get("reason", "")
    
    succeeded, failed, skipped = 0, 0, 0
    errors = events.copy()
    
    if status == "skipped":
        skipped = 1
    elif status == "error":
        failed = 1
        if reason:
            errors.append(reason)
    elif status == "success":
        succeeded = 1
        
    # SPECIAL CASE: URGENT_ALERT for GOLDBEES
    if any("URGENT_ALERT" in e for e in errors):
        failed = 1
        succeeded = 0
        
    import json
    import pathlib
    import sys
    
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
