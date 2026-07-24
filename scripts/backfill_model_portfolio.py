import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv

from supabase import create_client
import os

env_path = os.path.join(os.path.dirname(__file__), "..", ".env.local")
load_dotenv(env_path)
sb = create_client(os.environ.get("NEXT_PUBLIC_SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_ROLE_KEY"))

# We can reuse run_portfolio_for_date from model_portfolio_runner_d
# The backfill script just iterates over dates and calls it.

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from model_portfolio_runner_d import run_portfolio_for_date

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("backfill_model_portfolio")

def run_backfill(start_date=None, end_date=None, dates=None, dry_run=False):
    if dates:
        date_list = [d.strip() for d in dates.split(",")]
        log.info(f"Starting backfill for specific dates: {date_list} (Dry Run: {dry_run})")
        dates_to_run = [pd.to_datetime(d) for d in date_list]
    else:
        log.info(f"Starting backfill from {start_date} to {end_date} (Dry Run: {dry_run})")
        current_date = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        dates_to_run = []
        while current_date <= end:
            dates_to_run.append(current_date)
            current_date += timedelta(days=1)
    
    results = []
    
    sim_state = None
    if dry_run:
        # Initialize simulated state from DB to allow continuing from real runs
        port_res = sb.schema("model").table("portfolios").select("*").eq("strategy_key", "momentum_weekly").execute()
        if port_res.data:
            port = port_res.data[0]
            holdings_res = sb.schema("model").table("holdings").select("*").eq("portfolio_id", port["id"]).eq("status", "open").execute()
            sim_state = {"cash": port["remaining_cash"], "holdings": holdings_res.data}
        else:
            sim_state = {"cash": 1000000.0, "holdings": []}
    
    for current_date in dates_to_run:
        date_str = current_date.strftime("%Y-%m-%d")
        log.info(f"--- Processing Backfill for {date_str} ---")
        try:
            res = run_portfolio_for_date(date_str, dry_run=dry_run, sim_state=sim_state)
            if isinstance(res, dict):
                if res.get("status") == "error":
                    results.append((date_str, "ERROR", res.get("reason")))
                elif res.get("status") == "success" and dry_run and "sim_state" in res:
                    sim_state = res["sim_state"]
        except Exception as e:
            log.error(f"Exception during {date_str}: {e}")
            results.append((date_str, "FATAL_EXCEPTION", str(e)))
        
    log.info("Backfill complete.")
    if results:
        log.warning("The following dates had errors during backfill:")
        for r in results:
            log.warning(f"  {r[0]}: {r[1]} - {r[2]}")
    else:
        log.info("No errors during backfill!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default="2026-07-10", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=datetime.now().strftime("%Y-%m-%d"), help="End date (YYYY-MM-DD)")
    parser.add_argument("--dates", type=str, help="Comma-separated list of YYYY-MM-DD dates to run")
    parser.add_argument("--dry-run", action="store_true", help="Skip database RPC writes")
    args = parser.parse_args()
    
    run_backfill(args.start, args.end, args.dates, args.dry_run)
