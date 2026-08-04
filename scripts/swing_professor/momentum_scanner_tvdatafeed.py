"""
Momentum Swing Scanner — "Purple Dot" Strategy (Manas Arora / Trading with Groww)
-----------------------------------------------------------------------------------
Two-stage scanner built to plug into the same stack as data_sync_engine_nse_all_d.py,
data_sync_engine_n750_d.py, rrg_pipeline_w.py, and stage_analysis_pipeline_w.py.
Stage 2 follows the same parallel-fetch architecture as those scripts:
per-thread TvDatafeed instances, a connection semaphore, and exponential
backoff with connection recreation on 429s.

  STAGE 1 (broad, cheap)
      tradingview_screener runs the primary volume/RVOL momentum screen across
      the whole NSE mainboard (~2,000 stocks) in a single API call and returns
      ~50-300 names that already look "hot" today.
      (Skipped entirely when using --from-universe.)

  STAGE 2 (deep, per-stock, parallel)
      tvDatafeed pulls ~6 months of daily OHLCV history for every candidate
      (indicator data such as SMA is NOT available from tvDatafeed, so it's
      computed locally from the raw bars) via a ThreadPoolExecutor and runs
      the full checklist on each:

        1. 10 SMA & 20 SMA both upward-sloping
        2. >=30% up from the recent (1-3mo) swing low  ("prior force")
        3. At least one "Purple Dot" during the rally leg
           (>=5% single-day move on >=500k volume  -> proves speed/liquidity)
        4. Pullback from the swing high is <=25-30% (15-20% ideal)
        5. No "Red Dot" (high-volume down day) inside the pullback
        6. Volume-candle quality: red/pullback days should be thin (below-avg volume)

Only stocks that clear ALL of Stage 2 are written to the shortlist. Everything
is scored so you can rank rather than just filter.

------------------------------------------------------------------------------
INSTALL
    pip install pandas python-dotenv tradingview-screener supabase
    pip install --upgrade --no-cache-dir git+https://github.com/rongardF/tvdatafeed.git

    tvDatafeed is an unofficial TradingView websocket client (not on PyPI).
    Logging in with a real TradingView account (env vars below) raises the
    rate limits noticeably vs. the no-login mode, and is strongly recommended
    before running --workers > 2 against the full universe.

ENV VARS (optional, put in .env.local)
    TV_USERNAME, TV_PASSWORD           -> tvDatafeed login (recommended)
    NEXT_PUBLIC_SUPABASE_URL           -> needed for --from-universe / --push-supabase
    SUPABASE_SERVICE_ROLE_KEY

USAGE
    python momentum_scanner_tvdatafeed.py
    python momentum_scanner_tvdatafeed.py --tickers RELIANCE,TCS,MAZDOCK --no-stage1
    python momentum_scanner_tvdatafeed.py --from-universe --workers 4
    python momentum_scanner_tvdatafeed.py --from-universe --limit 100 --workers 2

Rate limiting strategy (same as the sync engines):
    - MAX_WORKERS=4 default — safe for free TradingView accounts
    - threading.Semaphore caps simultaneous open WebSocket connections
    - 429s AND empty/no-data responses both trigger exponential backoff
      (2s -> 4s -> 8s -> 16s), with the thread-local connection recreated on 429
    - Per-worker REQUEST_DELAY courtesy gap between calls

DISCLAIMER
    Educational tooling only, not financial advice. Verify every field name
    against your installed `tradingview_screener` version before relying on
    Stage 1 in production — TradingView renames screener columns occasionally.
"""

import os
import sys
import time
import logging
import argparse
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Optional, List, Tuple

import pandas as pd
from dotenv import load_dotenv

load_dotenv(".env.local")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Strategy thresholds (from the checklist) ─────────────────────────────────
PRICE_MIN               = 30
DAILY_CHANGE_MIN_PCT    = 3.0
AVG_VOL_30D_MIN         = 200_000
RVOL_MIN                = 3.0

LOOKBACK_BARS           = 150     # ~7 months of daily bars fetched per stock
FORCE_LOOKBACK_DAYS     = 63      # ~3 trading months, for the "prior force" low/high
SMA_SLOPE_LOOKBACK      = 5       # compare SMA today vs. SMA N bars ago

PURPLE_DOT_MOVE_PCT     = 5.0
PURPLE_DOT_MIN_VOLUME   = 500_000

PULLBACK_MAX_PCT        = 30.0    # hard cutoff — disqualify above this
PULLBACK_IDEAL_PCT      = 20.0    # below this = "ideal" quality flag
PRIOR_FORCE_MIN_PCT     = 30.0    # min rise from swing low required

STAGE1_DEFAULT_LIMIT    = 1000

# ── Parallelism & rate limiting (same pattern as data_sync_engine_n750_d.py,
#    rrg_pipeline_w.py, stage_analysis_pipeline_w.py) ─────────────────────────
MAX_WORKERS       = 4      # safe default for free/no-login TradingView accounts
REQUEST_DELAY     = 0.5    # courtesy gap per worker request, seconds
MAX_RETRIES       = 4      # 1 original + 3 retries
BACKOFF_BASE      = 2.0    # 2s -> 4s -> 8s -> 16s
BACKOFF_MAX       = 60.0
UPSERT_BATCH_SIZE = 50     # ticker-results-per-flush for autosave/push

# Semaphore caps concurrent open WebSocket connections regardless of worker count.
_connection_semaphore = threading.Semaphore(MAX_WORKERS)


# ── Stage 1: broad TradingView screener pass ─────────────────────────────────
def stage1_screener_candidates(limit: int = STAGE1_DEFAULT_LIMIT) -> pd.DataFrame:
    """
    Runs the primary momentum screen from the strategy guide:
        NSE mainboard, price>=30, day change>=3%, 30D avg vol>=200k, RVOL>3.0
    Returns a DataFrame with at least a 'name' column of tickers.

    NOTE: column names below (relative_volume_10d_calc, average_volume_30d_calc,
    change) match the common tradingview_screener field names at time of writing.
    If Stage 1 raises a KeyError/empty result, open a Python REPL and inspect
    Query().select() output to confirm the exact field names for your installed
    version, then adjust below.
    """
    from tradingview_screener import Query, col

    log.info("Stage 1: running broad NSE momentum screen (limit=%d) ...", limit)
    _count, df = (
        Query()
        .set_markets("india")
        .select("name", "close", "change", "volume",
                "average_volume_30d_calc", "relative_volume_10d_calc")
        .where(
            col("exchange") == "NSE",
            col("is_primary") == True,
            col("type") == "stock",
            col("subtype") == "common",
            col("close") >= PRICE_MIN,
            col("change") >= DAILY_CHANGE_MIN_PCT,
            col("average_volume_30d_calc") >= AVG_VOL_30D_MIN,
            col("relative_volume_10d_calc") > RVOL_MIN,
        )
        .order_by("relative_volume_10d_calc", ascending=False)
        .limit(limit)
        .get_scanner_data()
    )
    log.info("Stage 1: %d candidates passed the broad screen.", len(df))
    return df


# ── Stage 2: per-thread TvDatafeed + fetch with retry/backoff ────────────────
_thread_local = threading.local()


def _get_thread_tv() -> "TvDatafeed":
    """Return (or lazily create) a per-thread TvDatafeed instance.
    A single TvDatafeed instance holds one websocket connection and is NOT
    thread-safe to share — each worker thread gets its own."""
    from tvDatafeed import TvDatafeed
    if not hasattr(_thread_local, "tv"):
        username = os.environ.get("TV_USERNAME")
        password = os.environ.get("TV_PASSWORD")
        if username and password:
            _thread_local.tv = TvDatafeed(username=username, password=password)
        else:
            _thread_local.tv = TvDatafeed()
    return _thread_local.tv


def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "429" in msg or "too many requests" in msg


def get_daily_history_with_retry(symbol: str, exchange: str = "NSE",
                                  n_bars: int = LOOKBACK_BARS) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Fetch daily OHLCV with exponential backoff on 429 / transient errors
    AND on empty responses (an unofficial websocket client can return an
    empty frame transiently — that's retried here, not treated as final).
    Uses the semaphore to cap concurrent open WebSocket connections.
    """
    from tvDatafeed import Interval

    last_reason = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with _connection_semaphore:
                time.sleep(REQUEST_DELAY)
                tv = _get_thread_tv()
                df = tv.get_hist(symbol=symbol, exchange=exchange,
                                  interval=Interval.in_daily, n_bars=n_bars)

            if df is not None and not df.empty:
                df = df.reset_index().rename(columns={"index": "datetime", "symbol": "tv_symbol"})
                return df, None

            last_reason = "empty response"
            if attempt < MAX_RETRIES:
                wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
                log.warning("    %s: empty response (attempt %d/%d) — retrying in %.0fs",
                            symbol, attempt, MAX_RETRIES, wait)
                time.sleep(wait)

        except Exception as exc:
            last_reason = f"{type(exc).__name__}: {exc}"
            is_429 = _is_rate_limit_error(exc)
            if attempt < MAX_RETRIES:
                wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
                if is_429:
                    # drop the poisoned connection, force a fresh one on retry
                    if hasattr(_thread_local, "tv"):
                        del _thread_local.tv
                    log.warning("    %s: 429 (attempt %d/%d) — backing off %.0fs",
                                symbol, attempt, MAX_RETRIES, wait)
                else:
                    log.warning("    %s: %s (attempt %d/%d) — retrying in %.0fs",
                                symbol, type(exc).__name__, attempt, MAX_RETRIES, wait)
                time.sleep(wait)
            else:
                log.error("    %s: giving up after %d attempts (%s)", symbol, MAX_RETRIES, last_reason)

    return None, last_reason or "unknown"


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["sma10"] = df["close"].rolling(10).mean()
    df["sma20"] = df["close"].rolling(20).mean()
    df["pct_change"] = df["close"].pct_change() * 100
    df["is_purple_dot"] = (df["pct_change"] >= PURPLE_DOT_MOVE_PCT) & \
                           (df["volume"] >= PURPLE_DOT_MIN_VOLUME)
    df["is_red_dot"] = (df["pct_change"] <= -PURPLE_DOT_MOVE_PCT) & \
                        (df["volume"] >= PURPLE_DOT_MIN_VOLUME)
    return df


def evaluate_stock(df: Optional[pd.DataFrame]) -> Optional[dict]:
    """Runs the full checklist on one stock's OHLCV history.
    Returns a result dict if it clears every rule, else None."""
    min_bars_needed = 20 + SMA_SLOPE_LOOKBACK
    if df is None or len(df) < max(min_bars_needed, FORCE_LOOKBACK_DAYS // 2):
        return None

    df = compute_indicators(df)
    last = df.iloc[-1]

    # 1. SMA slope condition
    sma10_now, sma10_then = df["sma10"].iloc[-1], df["sma10"].iloc[-1 - SMA_SLOPE_LOOKBACK]
    sma20_now, sma20_then = df["sma20"].iloc[-1], df["sma20"].iloc[-1 - SMA_SLOPE_LOOKBACK]
    if pd.isna(sma10_then) or pd.isna(sma20_then):
        return None
    sma10_up = sma10_now > sma10_then
    sma20_up = sma20_now > sma20_then
    if not (sma10_up and sma20_up):
        return None

    # 2. Prior force: swing low -> swing high within lookback window
    window = df.tail(FORCE_LOOKBACK_DAYS).reset_index(drop=True)
    low_idx = window["low"].idxmin()
    recent_low = window.loc[low_idx, "low"]
    recent_low_date = window.loc[low_idx, "datetime"]

    # swing high must occur AT OR AFTER the swing low to represent the rally leg
    after_low = window[window["datetime"] >= recent_low_date]
    high_idx = after_low["high"].idxmax()
    swing_high = after_low.loc[high_idx, "high"]
    swing_high_date = after_low.loc[high_idx, "datetime"]

    if recent_low <= 0:
        return None
    up_from_low_pct = (swing_high - recent_low) / recent_low * 100
    if up_from_low_pct < PRIOR_FORCE_MIN_PCT:
        return None

    # 3. Purple dot frequency during the rally leg (low -> high)
    rally_leg = df[(df["datetime"] >= recent_low_date) & (df["datetime"] <= swing_high_date)]
    purple_dot_count = int(rally_leg["is_purple_dot"].sum())
    if purple_dot_count < 3:
        return None  # fails the speed/liquidity filter

    # 4. Pullback depth from swing high to latest close
    pullback_pct = (swing_high - last["close"]) / swing_high * 100
    if pullback_pct > PULLBACK_MAX_PCT:
        return None

    # 5. Red dot check inside the pullback window (after the swing high)
    pullback_window = df[df["datetime"] > swing_high_date]
    red_dot_count = int(pullback_window["is_red_dot"].sum())
    if red_dot_count > 0:
        return None

    # 6. Volume-candle quality: down days in the pullback should be thin (below avg vol)
    avg_vol_20 = df["volume"].tail(20).mean()
    down_days = pullback_window[pullback_window["pct_change"] < 0]
    thin_red_ratio = float((down_days["volume"] < avg_vol_20).mean()) if len(down_days) else 1.0

    if pullback_pct <= PULLBACK_IDEAL_PCT:
        quality_flag = "IDEAL - pullback <=20%, clean"
    else:
        quality_flag = "OK - pullback 20-30%, acceptable"

    score = (
        min(up_from_low_pct, 150) * 0.4
        + purple_dot_count * 6
        + (PULLBACK_MAX_PCT - pullback_pct) * 1.2
        + thin_red_ratio * 20
    )

    return {
        "close": round(float(last["close"]), 2),
        "up_from_low_pct": round(up_from_low_pct, 1),
        "swing_low_date": recent_low_date.date().isoformat() if hasattr(recent_low_date, "date") else str(recent_low_date),
        "swing_high_date": swing_high_date.date().isoformat() if hasattr(swing_high_date, "date") else str(swing_high_date),
        "pullback_pct": round(pullback_pct, 1),
        "purple_dots_in_rally": purple_dot_count,
        "red_dots_in_pullback": red_dot_count,
        "thin_red_day_ratio": round(thin_red_ratio, 2),
        "sma10_slope_up": sma10_up,
        "sma20_slope_up": sma20_up,
        "quality_flag": quality_flag,
        "score": round(score, 1),
    }


def load_universe_from_supabase(min_price: float = PRICE_MIN,
                                 schema: str = "universe",
                                 table: str = "nse_universe") -> list:
    """
    Pulls all active tickers from the Supabase universe table populated by
    data_sync_engine_nse_all_d.py, prefiltered on price (the only cheap
    liquidity-adjacent field available there — no volume column exists in
    this table, so RVOL/avg-volume filtering still has to happen elsewhere
    or be skipped for this path).
    """
    from supabase import create_client

    url = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError(
            "Missing NEXT_PUBLIC_SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY in .env.local"
        )

    sb = create_client(url, key)
    tickers = []
    page_size = 1000
    offset = 0
    while True:
        resp = (
            sb.schema(schema).table(table)
            .select("ticker, price, is_active")
            .eq("is_active", True)
            .gte("price", min_price)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = resp.data
        if not rows:
            break
        tickers.extend(r["ticker"] for r in rows)
        if len(rows) < page_size:
            break
        offset += page_size

    log.info("Loaded %d active tickers (price >= %.0f) from %s.%s",
              len(tickers), min_price, schema, table)
    return tickers


# ── Orchestration ─────────────────────────────────────────────────────────────
def process_ticker(ticker: str, bars_cache: Optional[dict] = None) -> Tuple[str, Optional[dict], Optional[str]]:
    hist, error = get_daily_history_with_retry(ticker)
    if hist is None:
        return ticker, None, error
    if bars_cache is not None:
        bars_cache[ticker] = hist    # side-effect cache; return signature unchanged
    res = evaluate_stock(hist)
    if res:
        res = {"ticker": ticker, **res}
        return ticker, res, None
    return ticker, None, "no_pass"


def run_scan(tickers=None, use_stage1=True, stage1_limit=STAGE1_DEFAULT_LIMIT,
             from_universe=False, universe_min_price=PRICE_MIN,
             limit=None, save_csv=True, bars_cache: Optional[dict] = None) -> pd.DataFrame:
    if tickers is None:
        if from_universe:
            tickers = load_universe_from_supabase(min_price=universe_min_price)
        elif use_stage1:
            stage1_df = stage1_screener_candidates(limit=stage1_limit)
            if stage1_df.empty:
                log.warning("Stage 1 returned zero candidates. Nothing to scan.")
                return pd.DataFrame()
            tickers = stage1_df["name"].tolist()
        else:
            raise ValueError("Pass --tickers, --from-universe, or leave stage1 enabled.")

    if limit:
        tickers = tickers[:limit]
        log.info("Limiting run to first %d tickers.", limit)

    autosave_path = f"momentum_shortlist_{date.today().isoformat()}.csv"
    autosave_every = UPSERT_BATCH_SIZE

    log.info("Stage 2: scanning %d tickers with %d workers ... (autosaving every %d to %s)",
              len(tickers), MAX_WORKERS, autosave_every, autosave_path)

    results = []
    completed = 0
    total = len(tickers)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_ticker, t, bars_cache): t for t in tickers}

        for future in as_completed(futures):
            ticker = futures[future]
            completed += 1
            if completed % 25 == 0 or completed == 1:
                log.info("[%d/%d] Stage 2 progress ...", completed, total)
            
            try:
                res_ticker, res_dict, error = future.result()
                if res_dict:
                    results.append(res_dict)
                    log.info("    -> PASSED  %s  score=%.1f  %s", res_ticker, res_dict["score"], res_dict["quality_flag"])
                elif error and error != "no_pass":
                    log.warning("    skip %s: %s", ticker, error)
            except Exception as exc:
                log.warning("    skip %s: Unhandled %s: %s", ticker, type(exc).__name__, exc)

            if save_csv and completed % autosave_every == 0 and results:
                pd.DataFrame(results).sort_values("score", ascending=False).to_csv(autosave_path, index=False)
                log.info("    ...autosaved %d passing results so far (%d/%d scanned)",
                          len(results), completed, total)

    out = pd.DataFrame(results)
    if not out.empty:
        out = out.sort_values("score", ascending=False).reset_index(drop=True)

    log.info("=" * 70)
    log.info("Checklist pass rate: %d / %d candidates cleared every rule.",
              len(out), total)
    log.info("=" * 70)

    if save_csv and not out.empty:
        out.to_csv(autosave_path, index=False)
        log.info("Saved final shortlist -> %s", autosave_path)

    return out


def push_to_supabase(df: pd.DataFrame, table: str = "momentum_watchlist",
                      schema: str = "universe") -> None:
    """Optional: upsert the shortlist into Supabase so it's queryable elsewhere.
    Assumes a table with columns matching the shortlist DataFrame + a
    'scanned_at' timestamp; create/migrate it yourself before using this."""
    from supabase import create_client
    from datetime import datetime, timezone

    url = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    if not url or not key:
        log.error("Missing Supabase env vars — skipping push.")
        return

    sb = create_client(url, key)
    rows = df.to_dict(orient="records")
    now = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r["scanned_at"] = now
    try:
        sb.schema(schema).table(table).upsert(rows, on_conflict="ticker").execute()
        log.info("Pushed %d rows to %s.%s", len(rows), schema, table)
    except Exception as exc:
        log.error("Supabase push failed: %s: %s", type(exc).__name__, exc)


# ── CLI ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Purple-Dot momentum swing scanner.")
    parser.add_argument("--tickers", type=str, default=None,
                         help="Comma-separated tickers to scan directly (skips Stage 1).")
    parser.add_argument("--no-stage1", action="store_true",
                         help="Require --tickers; do not run the broad screener.")
    parser.add_argument("--stage1-limit", type=int, default=STAGE1_DEFAULT_LIMIT,
                         help="Max candidates pulled from Stage 1 (default 300).")
    parser.add_argument("--from-universe", action="store_true",
                         help="Scan the full nse_universe table from Supabase instead of "
                              "the Stage 1 screener (bigger, slower, no RVOL prefilter).")
    parser.add_argument("--universe-min-price", type=float, default=PRICE_MIN,
                         help="Price floor applied when pulling from Supabase (default 30).")
    parser.add_argument("--limit", type=int, default=None,
                         help="Cap the number of tickers scanned — useful for testing "
                              "--from-universe before committing to a full run.")
    parser.add_argument("--push-supabase", action="store_true",
                         help="Upsert the resulting shortlist into Supabase.")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS,
                         help=f"Number of parallel fetch workers (default: {MAX_WORKERS}).")
    args = parser.parse_args()

    MAX_WORKERS = args.workers
    _connection_semaphore = threading.Semaphore(MAX_WORKERS)

    manual_tickers = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None
    if args.no_stage1 and not manual_tickers and not args.from_universe:
        parser.error("--no-stage1 requires --tickers or --from-universe")

    shortlist = run_scan(
        tickers=manual_tickers,
        use_stage1=not args.no_stage1 and manual_tickers is None and not args.from_universe,
        stage1_limit=args.stage1_limit,
        from_universe=args.from_universe and manual_tickers is None,
        universe_min_price=args.universe_min_price,
        limit=args.limit,
    )

    if not shortlist.empty:
        print("\n" + shortlist.to_string(index=False))
    else:
        print("\nNo stocks cleared the full checklist on this run.")

    if args.push_supabase and not shortlist.empty:
        push_to_supabase(shortlist)