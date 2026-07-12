"""
Weekly Momentum Scanner (N500-52W-Perf/Vol)
============================================

Stripped-down variant of momentum_scanner.py:
  - Universe : Supabase view/table `universe.n500` (Nifty 500 only, not N750)
  - No filters: ATH/retracement, EMA200, relative-strength, market-trend and
    price/volume gates are all removed on purpose. Every ticker that has
    enough history gets ranked.
  - Ranking formula (identical math to the "sharpe_return" function in
    momentum_scanner.py):
        performance_52w_pct = ROC(close, 252 trading days) * 100
        volatility_52w_pct  = (std(close) / mean(close)) * 100   over the same
                               253-bar window (coefficient of variation)
        momentum_score      = performance_52w_pct / volatility_52w_pct
    Ranking is purely by momentum_score, descending.
  - Storage:
        strategies.momentum_weekly          -- live "where do we stand today"
                                                snapshot, overwritten on every run
        strategies.momentum_weekly_history  -- append-only, one row per
                                                ticker per asof_date, so
                                                mid-week values aren't lost
  - Cadence: this pipeline is meant to run DAILY. The lookback window is
    still 252 DAILY closes (~52 weeks), which gives a much more stable
    volatility estimate than 52 weekly bars would. Running the pipeline
    daily just means you always know where the ranking stands mid-week;
    it does NOT mean you act on it mid-week.
  - Rebalancing (actually buying/selling based on the ranking) uses
    Friday's close as the calculation basis, executed the following Monday:
        REBALANCE_BASIS_DOW     = 4 (Friday)  -> the row flagged
                                   is_rebalance_basis_day = True
        REBALANCE_EXECUTION_DOW = 0 (Monday)  -> stored on that same row
                                   as rebalance_execution_date, so downstream
                                   portfolio logic knows exactly which date
                                   to act on using Friday's numbers.
    Every other day's run is purely informational (mid-week standings).

Portfolio construction (handled by whatever consumes this table, not stored
here as filter logic):
  - TOP_N   = 25  -> target new-entry rank
  - EXIT_RANK = 50 -> existing holdings are kept as long as their rank <= 50
  - Rebalance: calculated off Friday's close, executed the following Monday

Requirements
------------
    pip install tvdatafeed tradingview-screener supabase pandas numpy python-dotenv
"""

import os
import time
import random
import json
import logging
import argparse
import pathlib
import threading
from typing import Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

try:
    from tvDatafeed import TvDatafeed, Interval
except ImportError as e:
    raise SystemExit(
        "tvdatafeed not installed. Run:\n"
        "  pip install --upgrade git+https://github.com/StreamAlpha/tvdatafeed.git"
    ) from e

try:
    from supabase import create_client, Client
except ImportError as e:
    raise SystemExit("Run: pip install supabase") from e

try:
    from dotenv import load_dotenv
    load_dotenv(".env.local")
except ImportError:
    raise SystemExit("Run: pip install python-dotenv")


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("momentum_weekly_scanner")

# =========================================================================
# CONFIG
# =========================================================================

SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

UNIVERSE_SCHEMA = "universe"
UNIVERSE_TABLE = "n500"          # <-- N500 view/table, not n750

MOMENTUM_SCHEMA = "strategies"
MOMENTUM_TABLE = "momentum_weekly"

TV_USERNAME = os.environ.get("TV_USERNAME")
TV_PASSWORD = os.environ.get("TV_PASSWORD")

TIMEFRAME = Interval.in_daily
LOOKBACK_DAYS = 252               # ~52 trading weeks
N_BARS = 400                      # a little headroom above LOOKBACK_DAYS+1

# Regime filter must be computed on the WEEKLY chart (matches what you see on
# TradingView when you eyeball SUPERTREND_1_2.5 on CNX500) -- NOT daily bars.
REGIME_TIMEFRAME = Interval.in_weekly
REGIME_N_BARS = 150               # plenty of weekly history for ST(1, 2.5) to stabilize

# --- Ranking / portfolio knobs ---
RANK_CRITERIA = "momentum_score"  # performance_52w_pct / volatility_52w_pct
TOP_N = 25
EXIT_RANK = 50
REBALANCE_FREQ = "weekly"
REBALANCE_BASIS_DOW = 4            # Python weekday(): Mon=0 .. Sun=6. 4 = Friday close (calc basis).
REBALANCE_EXECUTION_DOW = 0        # 0 = Monday (day trades based on the Friday snapshot execute).

# --- Regime filter (checked only on the Friday basis day) ---
REGIME_TABLE = "momentum_weekly_regime"
REGIME_ENABLED = True
REGIME_INDEX_SYMBOL = "CNX500"   # verify this matches your TV feed's exact NIFTY 500 index symbol
REGIME_INDEX_EXCHANGE = "NSE"
REGIME_FILTER_NAME = "SUPERTREND_1_2.5"
SUPERTREND_PERIOD = 1
SUPERTREND_MULTIPLIER = 2.5
REGIME_ACTION_ON_BEARISH = "GO_CASH"          # matches "Regime Filter Action" = Go Cash
UNCORRELATED_ASSET_ENABLED = True
UNCORRELATED_ASSET_SYMBOL = "GOLDBEES"
UNCORRELATED_ALLOCATION_PCT = 100

# --- Retry / backoff on tvdatafeed failures ---
RETRY_COUNT = 4
BACKOFF_BASE = 2.0
BACKOFF_MAX = 60.0
REQUEST_DELAY_SEC = 0.5
REQUEST_JITTER = 0.3
PROGRESS_LOG_EVERY = 25

MAX_WORKERS = 6
_connection_semaphore = threading.Semaphore(MAX_WORKERS)


# =========================================================================
# Data classes
# =========================================================================

@dataclass
class ScanResult:
    ticker: str
    close: float
    performance_52w_pct: float
    volatility_52w_pct: float
    momentum_score: float
    valid: bool
    data_bars: int
    reason_failed: Optional[str]


# =========================================================================
# Supabase helpers
# =========================================================================

def get_n500_tickers(client: "Client") -> pd.DataFrame:
    resp = (
        client.schema(UNIVERSE_SCHEMA)
        .table(UNIVERSE_TABLE)
        .select("ticker, company_name")
        .execute()
    )
    df = pd.DataFrame(resp.data)
    log.info("Fetched %d tickers from %s.%s", len(df), UNIVERSE_SCHEMA, UNIVERSE_TABLE)
    return df


def clean_db_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, np.generic):
        return value.item()
    return value


def momentum_table(client: "Client"):
    return client.schema(MOMENTUM_SCHEMA).table(MOMENTUM_TABLE)


def _build_rows(ranked_df: pd.DataFrame, asof_date: str, week_start_date: str,
                 is_rebalance_basis_day: bool, rebalance_execution_date: Optional[str]):
    day_of_week = pd.Timestamp(asof_date).strftime("%A").upper()
    rows = []
    for row in ranked_df.itertuples(index=False):
        rows.append({
            "ticker": clean_db_value(getattr(row, "ticker")),
            "rank": int(clean_db_value(getattr(row, "rank"))),
            "asof_date": asof_date,
            "week_start_date": week_start_date,
            "exchange": "NSE",
            "close": clean_db_value(getattr(row, "close")),
            "performance_52w_pct": clean_db_value(getattr(row, "performance_52w_pct")),
            "volatility_52w_pct": clean_db_value(getattr(row, "volatility_52w_pct")),
            "momentum_score": clean_db_value(getattr(row, "momentum_score")),
            "data_bars": int(clean_db_value(getattr(row, "data_bars"))),
            "rank_criteria": RANK_CRITERIA,
            "day_of_week": day_of_week,
            "is_rebalance_basis_day": is_rebalance_basis_day,
            "rebalance_execution_date": rebalance_execution_date,
            "metadata": {
                "top_n": TOP_N,
                "exit_rank": EXIT_RANK,
                "rebalance_freq": REBALANCE_FREQ,
                "rebalance_basis_dow": REBALANCE_BASIS_DOW,
                "rebalance_execution_dow": REBALANCE_EXECUTION_DOW,
                "lookback_days": LOOKBACK_DAYS,
                "universe": f"{UNIVERSE_SCHEMA}.{UNIVERSE_TABLE}",
            },
        })
    return rows


def save_momentum_weekly_snapshot(
    client: "Client",
    ranked_df: pd.DataFrame,
    asof_date: str,
    week_start_date: str,
    is_rebalance_basis_day: bool,
    rebalance_execution_date: Optional[str],
):
    """Overwrites the live table (today's standings) and appends the same
    rows to the history table (so mid-week values are never lost)."""
    try:
        rows = _build_rows(
            ranked_df, asof_date, week_start_date, is_rebalance_basis_day, rebalance_execution_date,
        )

        # --- live table: delete-all-insert, always reflects "today" ---
        momentum_table(client).delete().neq("ticker", "").execute()
        if rows:
            batch_size = 500
            for start in range(0, len(rows), batch_size):
                momentum_table(client).insert(rows[start:start + batch_size]).execute()

        log.info(
            "Refreshed %s.%s: %d ranked rows as of %s (week starting %s, rebalance_basis_day=%s%s)",
            MOMENTUM_SCHEMA, MOMENTUM_TABLE, len(rows), asof_date, week_start_date,
            is_rebalance_basis_day,
            f", executes {rebalance_execution_date}" if rebalance_execution_date else "",
        )

        # --- history table: append-only, upsert on (ticker, asof_date) so ---
        # re-running the same day is idempotent instead of duplicating rows.
        if rows:
            history_client = client.schema(MOMENTUM_SCHEMA).table(f"{MOMENTUM_TABLE}_history")
            batch_size = 500
            for start in range(0, len(rows), batch_size):
                history_client.upsert(
                    rows[start:start + batch_size],
                    on_conflict="ticker,asof_date",
                ).execute()
            log.info(
                "Appended %d rows to %s.%s_history for %s",
                len(rows), MOMENTUM_SCHEMA, MOMENTUM_TABLE, asof_date,
            )

        return len(rows)
    except Exception as exc:
        raise RuntimeError(
            f"Could not refresh Supabase tables {MOMENTUM_SCHEMA}.{MOMENTUM_TABLE} / "
            f"{MOMENTUM_SCHEMA}.{MOMENTUM_TABLE}_history. Run 001_create_momentum_weekly.sql once, then retry."
        ) from exc


def save_regime_check(
    client: "Client",
    asof_date: str,
    week_start_date: str,
    rebalance_execution_date: str,
    regime_result: dict,
):
    """Upserts one row per Friday into strategies.momentum_weekly_regime.
    Never deleted -- this table is the running audit trail of every regime
    call, so `on_conflict` only guards against re-running the same Friday."""
    try:
        row = {
            "asof_date": asof_date,
            "week_start_date": week_start_date,
            "rebalance_execution_date": rebalance_execution_date,
            "index_symbol": REGIME_INDEX_SYMBOL,
            "regime_filter": REGIME_FILTER_NAME,
            "supertrend_period": SUPERTREND_PERIOD,
            "supertrend_multiplier": SUPERTREND_MULTIPLIER,
            "close": regime_result["close"],
            "supertrend_value": regime_result["supertrend_value"],
            "trend_direction": regime_result["trend_direction"],
            "regime_action": regime_result["regime_action"],
            "uncorrelated_asset_enabled": UNCORRELATED_ASSET_ENABLED,
            "uncorrelated_asset_symbol": UNCORRELATED_ASSET_SYMBOL if UNCORRELATED_ASSET_ENABLED else None,
            "uncorrelated_allocation_pct": UNCORRELATED_ALLOCATION_PCT if UNCORRELATED_ASSET_ENABLED else None,
            "data_bars": regime_result["data_bars"],
            "metadata": {
                "index_exchange": REGIME_INDEX_EXCHANGE,
                "action_on_bearish": REGIME_ACTION_ON_BEARISH,
            },
        }

        client.schema(MOMENTUM_SCHEMA).table(REGIME_TABLE).upsert(
            row, on_conflict="asof_date",
        ).execute()

        log.info(
            "Regime check %s (%s, ST%.0f/%.1f): close=%.2f supertrend=%s trend=%s -> %s "
            "(effective %s)",
            asof_date, REGIME_INDEX_SYMBOL, SUPERTREND_PERIOD, SUPERTREND_MULTIPLIER,
            regime_result["close"], regime_result["supertrend_value"],
            regime_result["trend_direction"], regime_result["regime_action"],
            rebalance_execution_date,
        )
        return True
    except Exception as exc:
        raise RuntimeError(
            f"Could not upsert Supabase table {MOMENTUM_SCHEMA}.{REGIME_TABLE}. "
            "Run 001_create_momentum_weekly.sql once, then retry."
        ) from exc


# =========================================================================
# tvdatafeed helpers (same pattern as momentum_scanner.py)
# =========================================================================

_thread_local = threading.local()


def get_tv_session(force_new: bool = False) -> "TvDatafeed":
    if force_new and hasattr(_thread_local, "tv"):
        del _thread_local.tv
    if not hasattr(_thread_local, "tv"):
        _thread_local.tv = TvDatafeed(TV_USERNAME, TV_PASSWORD) if TV_USERNAME else TvDatafeed()
    return _thread_local.tv


def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "429" in msg or "too many requests" in msg


def _courtesy_sleep():
    time.sleep(REQUEST_DELAY_SEC + random.uniform(0, REQUEST_JITTER))


def fetch_history(symbol: str, exchange: str, n_bars: int = N_BARS, interval=None) -> pd.DataFrame:
    if interval is None:
        interval = TIMEFRAME
    last_exc = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            with _connection_semaphore:
                tv = get_tv_session(force_new=(attempt > 1))
                df = tv.get_hist(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    n_bars=n_bars,
                )
                _courtesy_sleep()

            if df is None or df.empty:
                raise ValueError(f"No data returned for {exchange}:{symbol}")

            df = df.copy()
            df.index = pd.to_datetime(df.index)
            df.sort_index(inplace=True)
            df.columns = [c.lower() for c in df.columns]
            df = df[~df["close"].isna()]
            return df

        except Exception as exc:
            last_exc = exc
            is_429 = _is_rate_limit_error(exc)
            if attempt < RETRY_COUNT:
                wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
                if is_429:
                    log.warning(
                        "429 on %s:%s (attempt %d/%d) -- backing off %.0fs",
                        exchange, symbol, attempt, RETRY_COUNT, wait,
                    )
                else:
                    log.warning(
                        "Attempt %d/%d failed for %s:%s (%s: %s) -- reconnecting, retrying in %.0fs",
                        attempt, RETRY_COUNT, exchange, symbol, type(exc).__name__, exc, wait,
                    )
                time.sleep(wait)
    raise last_exc


def trim_to_asof(df: pd.DataFrame, asof: str) -> pd.DataFrame:
    if asof == "t-1" and len(df) > 1:
        return df.iloc[:-1].copy()
    return df


def roc(series: pd.Series, length: int) -> float:
    if len(series) <= length:
        return np.nan
    return (series.iloc[-1] / series.iloc[-1 - length] - 1.0) * 100.0


def supertrend(df: pd.DataFrame, period: int = 1, multiplier: float = 2.5):
    """Standard Supertrend indicator (same construction TradingView uses).

    With period=1 the ATR term is just that day's True Range (no smoothing),
    which is why this variant reacts fast -- exactly the point of a
    Supertrend(1, 2.5) regime filter.

    Returns (supertrend_line: pd.Series, direction: pd.Series of +1/-1),
    where +1 = uptrend (price above the trailing stop -> stay invested) and
    -1 = downtrend (price below the trailing stop -> go cash).
    """
    high, low, close = df["high"], df["low"], df["close"]
    hl2 = (high + low) / 2.0

    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()

    upperband = hl2 + multiplier * atr
    lowerband = hl2 - multiplier * atr

    final_upper = upperband.copy()
    final_lower = lowerband.copy()
    direction = pd.Series(index=df.index, dtype="int64")
    line = pd.Series(index=df.index, dtype="float64")

    for i in range(len(df)):
        if i == 0 or pd.isna(atr.iloc[i]):
            direction.iloc[i] = 1
            line.iloc[i] = lowerband.iloc[i] if not pd.isna(lowerband.iloc[i]) else close.iloc[i]
            continue

        if upperband.iloc[i] < final_upper.iloc[i - 1] or close.iloc[i - 1] > final_upper.iloc[i - 1]:
            final_upper.iloc[i] = upperband.iloc[i]
        else:
            final_upper.iloc[i] = final_upper.iloc[i - 1]

        if lowerband.iloc[i] > final_lower.iloc[i - 1] or close.iloc[i - 1] < final_lower.iloc[i - 1]:
            final_lower.iloc[i] = lowerband.iloc[i]
        else:
            final_lower.iloc[i] = final_lower.iloc[i - 1]

        prev_dir = direction.iloc[i - 1]
        if prev_dir == 1:
            cur_dir = -1 if close.iloc[i] < final_lower.iloc[i] else 1
        else:
            cur_dir = 1 if close.iloc[i] > final_upper.iloc[i] else -1

        direction.iloc[i] = cur_dir
        line.iloc[i] = final_lower.iloc[i] if cur_dir == 1 else final_upper.iloc[i]

    return line, direction


def evaluate_regime_filter(index_df: pd.DataFrame) -> dict:
    """Runs the Supertrend(1, 2.5) regime check on the latest bar of
    index_df (expected to already be trimmed to the Friday close)."""
    line, direction = supertrend(index_df, SUPERTREND_PERIOD, SUPERTREND_MULTIPLIER)
    close = index_df["close"].iloc[-1]
    st_value = line.iloc[-1]
    is_bullish = direction.iloc[-1] == 1
    action = "STAY_INVESTED" if is_bullish else REGIME_ACTION_ON_BEARISH

    return {
        "close": float(close),
        "supertrend_value": float(st_value) if not pd.isna(st_value) else None,
        "trend_direction": "UP" if is_bullish else "DOWN",
        "regime_action": action,
        "data_bars": len(index_df),
    }


def performance_and_volatility(close: pd.Series, length: int = LOOKBACK_DAYS):
    """Same math as sharpe_return() in momentum_scanner.py, split into its
    two components: performance_52w_pct and volatility_52w_pct."""
    close = close.dropna()
    if len(close) <= length:
        return np.nan, np.nan, len(close)

    window = close.tail(length + 1)
    perf_pct = roc(window, length)
    vol_pct = (window.std() / window.mean()) * 100.0 if window.mean() else np.nan
    return perf_pct, vol_pct, len(window)


# =========================================================================
# Scan
# =========================================================================

def scan_one_ticker(ticker: str, exchange: str, asof: str) -> ScanResult:
    df = trim_to_asof(fetch_history(ticker, exchange), asof)
    close = df["close"]

    perf_pct, vol_pct, bars_used = performance_and_volatility(close, LOOKBACK_DAYS)

    reason_failed = None
    valid = True
    if np.isnan(perf_pct) or np.isnan(vol_pct):
        reason_failed = f"insufficient daily closes for 52w calc: {bars_used} <= {LOOKBACK_DAYS}"
        valid = False
    elif vol_pct == 0:
        reason_failed = "volatility is zero, cannot rank"
        valid = False

    momentum_score = (perf_pct / vol_pct) if valid else np.nan

    return ScanResult(
        ticker=ticker,
        close=close.iloc[-1] if not close.empty else np.nan,
        performance_52w_pct=perf_pct,
        volatility_52w_pct=vol_pct,
        momentum_score=momentum_score,
        valid=valid,
        data_bars=bars_used,
        reason_failed=reason_failed,
    )


def run_scan(
    asof: str = "live",
    max_workers: int = MAX_WORKERS,
    save_results: bool = True,
):
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise SystemExit("Set NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env.local")

    start_time = time.time()

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    universe = get_n500_tickers(supabase)
    universe_total = len(universe)

    log.info(
        "Running WEEKLY momentum scan (N500) as-of: %s | workers: %d",
        "YESTERDAY'S CLOSE (T-1)" if asof == "t-1" else "LIVE / LATEST CLOSE",
        max_workers,
    )

    results: list[ScanResult] = []
    skipped: list[tuple] = []
    progress_lock = threading.Lock()
    completed = 0
    market_date = None

    def log_progress():
        elapsed = time.time() - start_time
        rate = elapsed / completed
        remaining = rate * (universe_total - completed)
        log.info(
            "Progress: %d/%d (%.0f%%) | valid so far: %d | skipped so far: %d | "
            "elapsed: %.1fmin | ETA: %.1fmin",
            completed, universe_total, 100 * completed / universe_total,
            sum(1 for r_ in results if r_.valid), len(skipped),
            elapsed / 60, remaining / 60,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_ticker = {
            pool.submit(scan_one_ticker, row["ticker"], "NSE", asof): row["ticker"]
            for _, row in universe.iterrows()
        }

        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                result = future.result()
                with progress_lock:
                    results.append(result)
                    if market_date is None:
                        pass
            except Exception as exc:
                log.warning("Skipping %s after %d attempts: %s", ticker, RETRY_COUNT, exc)
                with progress_lock:
                    skipped.append((ticker, str(exc)))

            with progress_lock:
                completed += 1
                if completed % PROGRESS_LOG_EVERY == 0 or completed == universe_total:
                    log_progress()

    result_df = pd.DataFrame([r.__dict__ for r in results])

    valid_df = (
        result_df[result_df["valid"] & result_df["momentum_score"].notna()].copy()
        if not result_df.empty else result_df
    )
    invalid_df = (
        result_df[~result_df["valid"]].copy()
        if not result_df.empty else pd.DataFrame()
    )

    if not valid_df.empty:
        valid_df = valid_df.sort_values("momentum_score", ascending=False)
        valid_df["rank"] = range(1, len(valid_df) + 1)

    top_n_df = valid_df[valid_df["rank"] <= TOP_N] if not valid_df.empty else valid_df
    hold_zone_df = (
        valid_df[(valid_df["rank"] > TOP_N) & (valid_df["rank"] <= EXIT_RANK)]
        if not valid_df.empty else valid_df
    )

    # asof_date / week_start_date derived from the last completed trading day
    if not result_df.empty:
        # Re-fetch just to know the latest bar date cheaply from one successful ticker
        # (all NSE equities share the same trading calendar/date).
        sample_ticker = result_df.iloc[0]["ticker"]
        sample_df = trim_to_asof(fetch_history(sample_ticker, "NSE", n_bars=5), asof)
        market_date = pd.Timestamp(sample_df.index[-1]).date()
    else:
        market_date = pd.Timestamp.today().date()

    asof_date = market_date.isoformat()
    week_start_date = (market_date - pd.Timedelta(days=market_date.weekday())).isoformat()
    is_rebalance_basis_day = market_date.weekday() == REBALANCE_BASIS_DOW

    rebalance_execution_date = None
    if is_rebalance_basis_day:
        days_ahead = (REBALANCE_EXECUTION_DOW - market_date.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7
        rebalance_execution_date = (market_date + pd.Timedelta(days=days_ahead)).isoformat()

    regime_result = None
    if is_rebalance_basis_day and REGIME_ENABLED:
        try:
            regime_df = trim_to_asof(
                fetch_history(
                    REGIME_INDEX_SYMBOL, REGIME_INDEX_EXCHANGE,
                    n_bars=REGIME_N_BARS, interval=REGIME_TIMEFRAME,
                ), asof,
            )
            regime_result = evaluate_regime_filter(regime_df)
            if save_results:
                save_regime_check(
                    supabase, asof_date, week_start_date, rebalance_execution_date, regime_result,
                )
        except Exception as exc:
            log.error(
                "Regime filter check failed for %s -- Monday's action is NOT determined. "
                "Investigate before rebalancing: %s", asof_date, exc,
            )

    saved_count = 0
    if save_results:
        saved_count = save_momentum_weekly_snapshot(
            supabase, valid_df, asof_date, week_start_date,
            is_rebalance_basis_day, rebalance_execution_date,
        )

    total_elapsed = time.time() - start_time

    log.info("=" * 70)
    log.info(
        "WEEKLY MOMENTUM SCAN SUMMARY (N500, as of %s, %s%s)",
        asof_date,
        market_date.strftime("%A"),
        f" -- REBALANCE BASIS, executes {rebalance_execution_date}" if is_rebalance_basis_day
        else " -- info-only, no rebalance",
    )
    log.info("  Universe fetched from Supabase : %d", universe_total)
    log.info("  Successfully fetched from TV    : %d", universe_total - len(skipped))
    log.info("  Skipped (failed after retries) : %d", len(skipped))
    log.info("  Invalid 52w perf/vol            : %d", len(invalid_df))
    log.info("  Ranked                          : %d", len(valid_df))
    log.info("  Top-%d                          : %d", TOP_N, len(top_n_df))
    log.info("  Hold zone (rank %d-%d)          : %d", TOP_N + 1, EXIT_RANK, len(hold_zone_df))
    log.info("  Saved to Supabase %s.%s : %d", MOMENTUM_SCHEMA, MOMENTUM_TABLE, saved_count)
    log.info("  Total time                      : %.1f min", total_elapsed / 60)
    if skipped:
        preview = ", ".join(t for t, _ in skipped[:30])
        log.info("  Skipped tickers: %s%s", preview, " ..." if len(skipped) > 30 else "")
    log.info("=" * 70)

    return {
        "all_results": result_df,
        "ranked": valid_df,
        "top_n": top_n_df,
        "hold_zone": hold_zone_df,
        "invalid": invalid_df,
        "skipped": skipped,
        "saved_count": saved_count,
        "asof_date": asof_date,
        "week_start_date": week_start_date,
        "is_rebalance_basis_day": is_rebalance_basis_day,
        "rebalance_execution_date": rebalance_execution_date,
        "regime_result": regime_result,
        "asof": asof,
    }


def run_regime_only(
    asof: str = "live",
    target_date: Optional[str] = None,
    save_results: bool = True,
):
    """Re-runs ONLY the Supertrend regime check, without rescanning all 500
    tickers. Useful for a manual retry after a Friday failure.

    By default this trusts the latest available index bar (safe on Sat/Sun,
    since no new bar exists until Monday's session). If you're retrying
    AFTER Monday's bar has appeared, pass target_date='YYYY-MM-DD' (the
    Friday you meant to check) to force the calculation onto that date
    instead of silently picking up Monday's bar.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise SystemExit("Set NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env.local")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    regime_df = trim_to_asof(
        fetch_history(
            REGIME_INDEX_SYMBOL, REGIME_INDEX_EXCHANGE,
            n_bars=REGIME_N_BARS, interval=REGIME_TIMEFRAME,
        ), asof,
    )

    if target_date:
        cutoff = pd.Timestamp(target_date)
        regime_df = regime_df[regime_df.index <= cutoff]
        if regime_df.empty:
            raise SystemExit(f"No {REGIME_INDEX_SYMBOL} bars available on/before {target_date}")

    market_date = pd.Timestamp(regime_df.index[-1]).date()
    if market_date.weekday() != REBALANCE_BASIS_DOW:
        log.warning(
            "Latest usable bar is %s (%s), not the configured basis day (weekday %d). "
            "Proceeding anyway since --regime-only was run explicitly -- double-check this "
            "is the date you intended before trusting the result for Monday's rebalance.",
            market_date, market_date.strftime("%A"), REBALANCE_BASIS_DOW,
        )

    asof_date = market_date.isoformat()
    week_start_date = (market_date - pd.Timedelta(days=market_date.weekday())).isoformat()
    days_ahead = (REBALANCE_EXECUTION_DOW - market_date.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    rebalance_execution_date = (market_date + pd.Timedelta(days=days_ahead)).isoformat()

    regime_result = evaluate_regime_filter(regime_df)
    if save_results:
        save_regime_check(supabase, asof_date, week_start_date, rebalance_execution_date, regime_result)

    return {
        "asof_date": asof_date,
        "week_start_date": week_start_date,
        "rebalance_execution_date": rebalance_execution_date,
        "regime_result": regime_result,
    }


def _write_result(succeeded: int, failed: int, skipped: int, total: int, errors: list) -> None:
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/momentum_weekly.json").write_text(json.dumps({
        "script":    "Weekly Momentum Scanner (N500)",
        "succeeded": succeeded,
        "failed":    failed,
        "skipped":   skipped,
        "total":     total,
        "errors":    errors,
    }))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Momentum Scanner (N500, perf/vol only) -- run daily, rebalance weekly"
    )
    parser.add_argument(
        "--yesterday", action="store_true",
        help="Anchor calculations to yesterday's completed close (T-1) instead of the live/latest bar.",
    )
    parser.add_argument(
        "--workers", type=int, default=MAX_WORKERS,
        help=f"Number of parallel TV sessions/threads (default: {MAX_WORKERS}).",
    )
    parser.add_argument(
        "--no-save", action="store_true",
        help=f"Run the scan without refreshing the Supabase {MOMENTUM_SCHEMA}.{MOMENTUM_TABLE} table.",
    )
    parser.add_argument(
        "--regime-only", action="store_true",
        help="Skip the full N500 scan/ranking and only (re)run the Supertrend regime check. "
             "Use this to retry a failed Friday regime check without rescanning 500 tickers.",
    )
    parser.add_argument(
        "--date", default=None,
        help="YYYY-MM-DD. Forces the regime check onto this specific date's close, instead of "
             "trusting the latest available bar. Only used with --regime-only. Needed if you're "
             "retrying after Monday's bar has already appeared (otherwise the retry would "
             "silently evaluate Monday instead of the Friday you meant to check).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asof_mode = "t-1" if args.yesterday else "live"

    if args.regime_only:
        out = run_regime_only(
            asof=asof_mode,
            target_date=args.date,
            save_results=not args.no_save,
        )
        rr = out["regime_result"]
        print(f"\n=== REGIME-ONLY CHECK ({REGIME_FILTER_NAME} on {REGIME_INDEX_SYMBOL}) ===")
        print(f"As-of date: {out['asof_date']} | Week start: {out['week_start_date']}")
        print(
            f"close={rr['close']:.2f} supertrend={rr['supertrend_value']:.2f} "
            f"trend={rr['trend_direction']} -> {rr['regime_action']}"
        )
        print(f"Effective Monday: {out['rebalance_execution_date']}")
        if rr["regime_action"] != "STAY_INVESTED" and UNCORRELATED_ASSET_ENABLED:
            print(f"  -> Monday: move to {UNCORRELATED_ASSET_SYMBOL} ({UNCORRELATED_ALLOCATION_PCT}% allocation)")
        if args.no_save:
            print(f"(--no-save: not written to {MOMENTUM_SCHEMA}.{REGIME_TABLE})")
        else:
            print(f"Saved to Supabase {MOMENTUM_SCHEMA}.{REGIME_TABLE}")
        raise SystemExit(0)

    MAX_WORKERS = args.workers
    _connection_semaphore = threading.Semaphore(MAX_WORKERS)

    out = run_scan(
        asof=asof_mode,
        max_workers=args.workers,
        save_results=not args.no_save,
    )

    print("\n=== TOP", TOP_N, "(N500 weekly, perf/vol only) ===")
    if out["top_n"].empty:
        print("(no stocks ranked)")
    else:
        print(
            out["top_n"][
                ["rank", "ticker", "close", "performance_52w_pct", "volatility_52w_pct", "momentum_score", "data_bars"]
            ].to_string(index=False)
        )

    if not out["invalid"].empty:
        print("\n=== INSUFFICIENT 252-DAY HISTORY ===")
        print(out["invalid"][["ticker", "data_bars", "reason_failed"]].to_string(index=False))

    rebalance_note = (
        f"REBALANCE BASIS (executes Monday {out['rebalance_execution_date']})"
        if out["is_rebalance_basis_day"] else "info-only (no rebalance today)"
    )
    print(f"\nAs-of date: {out['asof_date']} | Week start: {out['week_start_date']} | {rebalance_note}")

    if out["regime_result"]:
        rr = out["regime_result"]
        print(
            f"Regime filter ({REGIME_FILTER_NAME} on {REGIME_INDEX_SYMBOL}): "
            f"close={rr['close']:.2f} supertrend={rr['supertrend_value']:.2f} "
            f"trend={rr['trend_direction']} -> {rr['regime_action']}"
        )
        if rr["regime_action"] != "STAY_INVESTED" and UNCORRELATED_ASSET_ENABLED:
            print(f"  -> Monday: move to {UNCORRELATED_ASSET_SYMBOL} ({UNCORRELATED_ALLOCATION_PCT}% allocation)")

    print(f"Saved to Supabase {MOMENTUM_SCHEMA}.{MOMENTUM_TABLE}: {out['saved_count']} rows")
    if out["skipped"]:
        print(f"Skipped {len(out['skipped'])} tickers (see log above for details)")

    failed_tickers = [t for t, _reason in out["skipped"]]
    _write_result(
        succeeded=len(out["all_results"]) - len(out["skipped"]),
        failed=len(out["skipped"]),
        skipped=len(out["invalid"]),
        total=len(out["all_results"]),
        errors=failed_tickers[:10],
    )
