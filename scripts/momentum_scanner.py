"""
Momentum Investing Scanner (N750-TM-T30-W60-RM-M)
==================================================

Re-implements the rules configured in the "Momentum Investing Scanner" tool
using:
  - Supabase table `universe.n750` for the stock universe
  - tradingview_screener for a fast live-only bulk prefilter
  - tvdatafeed (unofficial TradingView data feed) for OHLCV history

Rules implemented
------------------
Universe:
    Nifty 750 (Nifty 500 + Nifty Microcap 250), active tickers only, NSE.

Liquidity / Price filters:
    - price_above  = 100
    - price_below  = 10000
    - avg_volume   > 0   (i.e. effectively no minimum, kept for completeness)

Trend filters (per stock):
    - Retracement: close >= (1 - RETRACEMENT_PCT) * ATH   (ATH = all-time high close)
    - close > EMA(200)

Momentum score:
    - Weighted ROC across up to 4 lookback periods (252/120/90/60).
    - In the current config only the 252-day period is enabled (weight 1),
      the others are present but switched off -> momentum_score = ROC(252).
    - Toggle PERIODS_ENABLED below to activate the other lookbacks.

Relative strength filter:
    - benchmark = NSE:NIFTY TOTAL MKT (change BENCHMARK_SYMBOL if your TV
      symbol differs, e.g. "NIFTYTOTALMKT" / "NIFTY500" depending on feed)
    - RS = close / benchmark_close
    - condition: RS > EMA(RS, 200)          ("Popular" preset: Ratio above 200-EMA)

Market Trend Filter (global regime gate, not per-stock):
    - index   = NSE:NIFTY 50  ("NIFTY")
    - rule    = close vs EMA(100)
    - action  = "Only Exit - Stop New Entry"
                -> when Nifty50 < EMA100, ALLOW_NEW_ENTRIES is set False and the
                   scanner only reports names for monitoring/exit, not fresh buys.

Ranking (for portfolio construction, from the Simulator):
    - Rank surviving stocks by Sharpe Return (mean/std of daily returns,
      annualised) over the momentum lookback window.
    - TOP_N = 30, EXIT_RANK = 60 (buffer zone for holding, used when you wire
      this into a rebalance/portfolio script -- this file only produces the
      ranked scan output).

Requirements
------------
    pip install tvdatafeed tradingview-screener supabase pandas numpy
    # tvdatafeed is not on PyPI under that exact name in all environments;
    # if `pip install tvdatafeed` fails, install from source:
    #   pip install --upgrade git+https://github.com/StreamAlpha/tvdatafeed.git
"""

import os
import time
import random
import logging
import argparse
import threading
from datetime import datetime
from dataclasses import dataclass
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

# --- external deps -----------------------------------------------------
try:
    from tvDatafeed import TvDatafeed, Interval
except ImportError as e:
    raise SystemExit(
        "tvdatafeed not installed. Run:\n"
        "  pip install --upgrade git+https://github.com/StreamAlpha/tvdatafeed.git"
    ) from e

try:
    from tradingview_screener import Query, Column
except ImportError:
    Query = None
    Column = None

try:
    from supabase import create_client, Client
except ImportError as e:
    raise SystemExit("Run: pip install supabase") from e

try:
    from dotenv import load_dotenv
    # Looks for .env.local in the current working directory.
    # Change the path here if you run this script from a different folder
    # than where .env.local lives.
    load_dotenv(".env.local")
except ImportError:
    raise SystemExit("Run: pip install python-dotenv")


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("momentum_scanner")

# =========================================================================
# CONFIG  -- mirrors the screenshot fields
# =========================================================================

# --- Supabase ---
# Reads the same .env.local your Next.js app already uses.
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
UNIVERSE_SCHEMA = "universe"
UNIVERSE_TABLE = "n750"
MOMENTUM_SCHEMA = "strategies"
MOMENTUM_TABLE = "momentum"

# --- TradingView login (optional -- anonymous works for delayed NSE data) ---
TV_USERNAME = os.environ.get("TV_USERNAME")  # or None for anonymous session
TV_PASSWORD = os.environ.get("TV_PASSWORD")

# --- Scan config (from screenshot 1) ---
CHART_TYPE = "Candle"
TIMEFRAME = Interval.in_daily
N_BARS = 5000  # broader history so ATH is closer to true all-time high

PRICE_ABOVE = 100
PRICE_BELOW = 10000
VOLUME_METHOD = "average"     # "average" | "median"
VOLUME_ABOVE = 0

RETRACEMENT_ENABLED = True
RETRACEMENT_PCT = 0.25        # "within 25%"
RETRACEMENT_MODE = "ATH"      # ATH | 52w_high | 52w_low | ATL

EMA_FILTER_ENABLED = True
EMA_FILTER_LEN = 200

# Period / Weight block: only 252 is checked in the screenshot
PERIODS_ENABLED = {252: True, 120: False, 90: False, 60: False}
PERIOD_WEIGHTS = {252: 1, 120: 1, 90: 1, 60: 1}
REQUIRE_VALID_MOMENTUM = True

# Relative Momentum / RS Sub-condition
RELATIVE_MOMENTUM_ENABLED = True
BENCHMARK_EXCHANGE = "NSE"
BENCHMARK_SYMBOL = "NIFTY_TOTAL_MKT"  # NSE: NIFTY TOTAL MKT (confirmed via TradingView)
RS_EMA_LEN = 200                     # "Ratio above 200-EMA"

# --- Market Trend Filter (screenshot 3) ---
MARKET_FILTER_ENABLED = True
MARKET_INDEX_EXCHANGE = "NSE"
MARKET_INDEX_SYMBOL = "NIFTY"        # NSE: Nifty 50
MARKET_FILTER_TYPE = "EMA"           # EMA | D_SMART | MAST
MARKET_FILTER_LEN = 100
MARKET_FILTER_ACTION = "only_exit_stop_new_entry"

# --- Ranking (from Simulator, screenshot 2) ---
RANK_CRITERIA = "sharpe"      # Sharpe Return
TOP_N = 30
EXIT_RANK = 60

REQUEST_SLEEP_SEC = 1.0  # legacy knob, superseded by REQUEST_DELAY_SEC below

# --- Live-mode bulk prefilter ---
# TradingView's screener fields are live/current only. They are useful for the
# default live scan, but they cannot represent yesterday's completed EMA/ATH
# state, so --yesterday intentionally falls back to tvdatafeed for everything.
SCREENER_ENABLED = True
SCREENER_MARKET = "india"
SCREENER_LIMIT = 5000
SCREENER_COLUMNS = {
    "symbol": "name",
    "exchange": "exchange",
    "close": "close",
    "volume": "volume",
    "ema200": "EMA200",
    "ath": "High.All",
}

# --- Retry / backoff behaviour when tvdatafeed fails on a symbol ---
RETRY_COUNT = 4          # total attempts per symbol (1 initial + 3 retries)
BACKOFF_BASE = 2.0       # exponential: 2s -> 4s -> 8s -> 16s ...
BACKOFF_MAX = 60.0       # cap so a bad run doesn't spiral

# --- Courtesy delay after every successful/failed fetch, jittered so
#     parallel workers don't all hit TV in lockstep ---
REQUEST_DELAY_SEC = 0.5
REQUEST_JITTER = 0.3

# --- Progress reporting while looping over ~750 tickers ---
PROGRESS_LOG_EVERY = 25  # log a progress line every N tickers processed

# --- Parallelism ---
# Each worker holds its own TvDatafeed websocket session, so one dying
# connection doesn't stall the whole scan. A semaphore additionally caps
# how many of those connections can be open/in-flight at once, independent
# of thread count -- belt and suspenders against TV throttling.
# 4-6 is safe for free/anonymous TradingView access; raise only if you have
# a Pro/Pro+ account and are seeing clean runs.
MAX_WORKERS = 6
_connection_semaphore = threading.Semaphore(MAX_WORKERS)


# =========================================================================
# Data classes
# =========================================================================

@dataclass
class ScanResult:
    ticker: str
    close: float
    ath: float
    retracement_pct: float
    ema200: float
    above_ema200: bool
    rs_above_rs_ema200: bool
    momentum_score: float
    momentum_valid: bool
    sharpe: float
    passed: bool
    data_bars: int
    reasons_failed: list


# =========================================================================
# Supabase: fetch universe
# =========================================================================

def get_n750_tickers(client: "Client") -> pd.DataFrame:
    resp = (
        client.schema(UNIVERSE_SCHEMA)
        .table(UNIVERSE_TABLE)
        .select("ticker, company_name, exchange, is_active")
        .eq("is_active", True)
        .execute()
    )
    df = pd.DataFrame(resp.data)
    log.info("Fetched %d active tickers from %s.%s", len(df), UNIVERSE_SCHEMA, UNIVERSE_TABLE)
    return df


def clean_db_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, np.generic):
        return value.item()
    return value


def momentum_table(client: "Client"):
    if MOMENTUM_SCHEMA == "public":
        return client.table(MOMENTUM_TABLE)
    return client.schema(MOMENTUM_SCHEMA).table(MOMENTUM_TABLE)


def save_momentum_snapshot(
    client: "Client",
    qualified_df: pd.DataFrame,
    asof_date: str,
    new_entries_allowed: bool,
):
    """Refresh latest qualified rows in strategies.momentum."""
    try:
        momentum_table(client).delete().neq("ticker", "").execute()

        if qualified_df.empty:
            log.info(
                "Refreshed %s.%s: deleted old rows; no qualified stocks to insert",
                MOMENTUM_SCHEMA, MOMENTUM_TABLE,
            )
            return 0

        rows = []
        for row in qualified_df.itertuples(index=False):
            rows.append({
                "ticker": clean_db_value(getattr(row, "ticker")),
                "rank": int(clean_db_value(getattr(row, "rank"))),
                "asof_date": asof_date,
                "exchange": "NSE",
                "close": clean_db_value(getattr(row, "close")),
                "ath": clean_db_value(getattr(row, "ath")),
                "retracement_pct": clean_db_value(getattr(row, "retracement_pct")),
                "ema200": clean_db_value(getattr(row, "ema200")),
                "above_ema200": bool(clean_db_value(getattr(row, "above_ema200"))),
                "rs_above_rs_ema200": bool(clean_db_value(getattr(row, "rs_above_rs_ema200"))),
                "momentum_score": clean_db_value(getattr(row, "momentum_score")),
                "sharpe_return": clean_db_value(getattr(row, "sharpe")),
                "data_bars": int(clean_db_value(getattr(row, "data_bars"))),
                "market_new_entries_allowed": bool(new_entries_allowed),
                "rank_criteria": RANK_CRITERIA,
                "metadata": {
                    "top_n": TOP_N,
                    "exit_rank": EXIT_RANK,
                    "periods_enabled": PERIODS_ENABLED,
                    "period_weights": PERIOD_WEIGHTS,
                    "price_above": PRICE_ABOVE,
                    "price_below": PRICE_BELOW,
                    "retracement_pct": RETRACEMENT_PCT,
                    "ema_filter_len": EMA_FILTER_LEN,
                    "benchmark": f"{BENCHMARK_EXCHANGE}:{BENCHMARK_SYMBOL}",
                    "rs_ema_len": RS_EMA_LEN,
                    "market_filter": {
                        "symbol": f"{MARKET_INDEX_EXCHANGE}:{MARKET_INDEX_SYMBOL}",
                        "type": MARKET_FILTER_TYPE,
                        "length": MARKET_FILTER_LEN,
                        "action": MARKET_FILTER_ACTION,
                    },
                },
            })

        batch_size = 500
        for start in range(0, len(rows), batch_size):
            momentum_table(client).insert(rows[start:start + batch_size]).execute()

        log.info(
            "Refreshed %s.%s: inserted %d qualified rows as of %s",
            MOMENTUM_SCHEMA, MOMENTUM_TABLE, len(rows), asof_date,
        )
        return len(rows)
    except Exception as exc:
        raise RuntimeError(
            f"Could not refresh Supabase table {MOMENTUM_SCHEMA}.{MOMENTUM_TABLE}. "
            "Run supabase/migrations/001_create_momentum.sql once, then retry."
        ) from exc


# =========================================================================
# tvdatafeed helpers -- one persistent session per worker thread, gated by
# a semaphore, with exponential backoff on failure. Modeled on the
# connection pattern from data_sync_engine_n750_d.py.
# =========================================================================

_thread_local = threading.local()


def get_tv_session(force_new: bool = False) -> "TvDatafeed":
    """Return (or lazily create) this thread's own TvDatafeed instance.
    On a connection failure we discard it and open a fresh one next attempt
    rather than retrying on a socket that's already dead."""
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


def fetch_history(symbol: str, exchange: str) -> pd.DataFrame:
    """Fetch OHLCV history with exponential backoff on failure.

    - The semaphore caps how many connections/requests are in flight at
      once, independent of how many threads exist.
    - On a 429 (rate limit) the thread's session is discarded so the next
      attempt opens a clean connection; other errors (timeouts, dropped
      sockets) also get a fresh session since a dead websocket rarely
      recovers on retry.
    - A small jittered sleep happens after every attempt, inside the
      semaphore, so parallel workers don't all hit TV in lockstep.
    """
    last_exc = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            with _connection_semaphore:
                tv = get_tv_session(force_new=(attempt > 1))
                df = tv.get_hist(
                    symbol=symbol,
                    exchange=exchange,
                    interval=TIMEFRAME,
                    n_bars=N_BARS,
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
    """
    asof == "live"  -> use data as-is (last row may be an in-progress bar
                        if the market is currently open).
    asof == "t-1"   -> drop the most recent bar so every calculation
                        (price, EMA, ATH, ROC, RS) is anchored to
                        yesterday's completed close instead of today's
                        partial/live one. Safe to use any time, but the
                        point is to make intraday runs deterministic.
    """
    if asof == "t-1" and len(df) > 1:
        return df.iloc[:-1].copy()
    return df


def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def roc(series: pd.Series, length: int) -> float:
    if len(series) <= length:
        return np.nan
    return (series.iloc[-1] / series.iloc[-1 - length] - 1.0) * 100.0


def sharpe_return(close: pd.Series, length: int) -> float:
    """Momentify-style Sharpe Return.

    The app describes Sharpe Return as return adjusted for standard deviation.
    Empirically this matches ranking by ROC over the lookback window divided by
    the coefficient of variation of closing prices in that same window, rather
    than textbook mean daily return / daily return standard deviation.
    """
    close = close.dropna()
    if len(close) <= length:
        return np.nan

    window = close.tail(length + 1)
    period_return = roc(window, length)
    price_cv = window.std() / window.mean() if window.mean() else np.nan
    if np.isnan(period_return) or np.isnan(price_cv) or price_cv == 0:
        return np.nan

    return period_return / price_cv


# =========================================================================
# Filters
# =========================================================================

def passes_price_volume(df: pd.DataFrame) -> tuple[bool, list]:
    reasons = []
    close = df["close"].iloc[-1]
    if not (PRICE_ABOVE <= close <= PRICE_BELOW):
        reasons.append(f"price {close:.2f} outside [{PRICE_ABOVE}, {PRICE_BELOW}]")

    vol_series = df["volume"].tail(20)
    vol_stat = vol_series.mean() if VOLUME_METHOD == "average" else vol_series.median()
    if vol_stat <= VOLUME_ABOVE:
        reasons.append(f"volume {vol_stat:.0f} <= {VOLUME_ABOVE}")

    return (len(reasons) == 0), reasons


def passes_retracement(df: pd.DataFrame) -> tuple[bool, float, float, list]:
    close = df["close"].iloc[-1]
    ath = df["close"].max()  # all-time-high within fetched history
    retr_pct = (ath - close) / ath if ath else np.nan
    reasons = []
    if RETRACEMENT_ENABLED and retr_pct > RETRACEMENT_PCT:
        reasons.append(f"retracement {retr_pct:.1%} > {RETRACEMENT_PCT:.0%} from ATH")
    return (len(reasons) == 0), ath, retr_pct, reasons


def passes_ema_filter(df: pd.DataFrame) -> tuple[bool, float, list]:
    e = ema(df["close"], EMA_FILTER_LEN).iloc[-1]
    close = df["close"].iloc[-1]
    reasons = []
    above = close > e
    if EMA_FILTER_ENABLED and not above:
        reasons.append(f"close {close:.2f} not above EMA{EMA_FILTER_LEN} {e:.2f}")
    return (len(reasons) == 0), e, reasons


def compute_momentum_score(df: pd.DataFrame) -> float:
    score = 0.0
    total_weight = 0.0
    for period, enabled in PERIODS_ENABLED.items():
        if not enabled:
            continue
        w = PERIOD_WEIGHTS[period]
        r = roc(df["close"], period)
        if np.isnan(r):
            continue
        score += w * r
        total_weight += w
    return score / total_weight if total_weight else np.nan


def compute_momentum_score_checked(df: pd.DataFrame) -> tuple[float, bool, list]:
    """Compute weighted ROC and explain invalid enabled periods.

    A 252-day ROC needs 253 daily closes: today's close plus the close 252
    trading days back. If any enabled period cannot be computed, the stock is
    excluded from pass/rank output instead of receiving a NaN momentum score.
    """
    reasons = []
    score = 0.0
    total_weight = 0.0
    closes = df["close"].dropna()

    for period, enabled in PERIODS_ENABLED.items():
        if not enabled:
            continue

        required_closes = period + 1
        if len(closes) < required_closes:
            reasons.append(
                f"insufficient daily closes for ROC{period}: {len(closes)} < {required_closes}"
            )
            continue

        r = roc(closes, period)
        if np.isnan(r):
            reasons.append(f"momentum ROC{period} is NaN")
            continue

        w = PERIOD_WEIGHTS[period]
        score += w * r
        total_weight += w

    if total_weight == 0:
        reasons.append("no enabled momentum period produced a valid value")
        return np.nan, False, reasons

    momentum_score = score / total_weight
    if np.isnan(momentum_score):
        reasons.append("weighted momentum score is NaN")
        return momentum_score, False, reasons

    return momentum_score, True, reasons


def passes_relative_strength(df: pd.DataFrame, bench: pd.DataFrame) -> tuple[bool, list]:
    reasons = []
    if not RELATIVE_MOMENTUM_ENABLED:
        return True, reasons

    merged = pd.DataFrame({
        "stock": df["close"],
        "bench": bench["close"],
    }).dropna()
    if len(merged) < RS_EMA_LEN:
        reasons.append(f"insufficient overlapping RS history: {len(merged)} < {RS_EMA_LEN}")
        return False, reasons

    rs = merged["stock"] / merged["bench"]
    rs_ema = ema(rs, RS_EMA_LEN)

    above = rs.iloc[-1] > rs_ema.iloc[-1]
    if not above:
        reasons.append(f"RS {rs.iloc[-1]:.4f} not above RS-EMA{RS_EMA_LEN} {rs_ema.iloc[-1]:.4f}")
    return above, reasons


def fetch_live_screener_snapshot(universe: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Fetch live/current TradingView screener fields for the N750 universe.

    The screener API has no historical offset support. This function is only
    used for live scans as a cheap candidate reducer before tvdatafeed does the
    historical calculations that still require OHLCV bars.
    """
    if Query is None or Column is None:
        log.warning(
            "tradingview_screener is not installed; using full tvdatafeed scan. "
            "Install with: pip install tradingview-screener"
        )
        return None

    tickers = sorted({str(t).strip().upper() for t in universe["ticker"].dropna() if str(t).strip()})
    if not tickers:
        return None

    selected_columns = list(dict.fromkeys(SCREENER_COLUMNS.values()))

    try:
        _, df = (
            Query()
            .set_markets(SCREENER_MARKET)
            .select(*selected_columns)
            .where(
                Column(SCREENER_COLUMNS["exchange"]) == "NSE",
                Column(SCREENER_COLUMNS["symbol"]).isin(tickers),
            )
            .limit(SCREENER_LIMIT)
            .get_scanner_data()
        )
    except Exception as exc:
        log.warning("TradingView screener prefilter failed; using full tvdatafeed scan: %s", exc)
        return None

    if df is None or df.empty:
        log.warning("TradingView screener returned no rows; using full tvdatafeed scan")
        return None

    df = df.copy()
    # tradingview_screener includes a prefixed raw `ticker` column like
    # `NSE:RELIANCE` even when selecting `name`; keep `name` as our plain symbol.
    if "ticker" in df.columns and SCREENER_COLUMNS["symbol"] in df.columns:
        df.drop(columns=["ticker"], inplace=True)

    rename_map = {
        SCREENER_COLUMNS["symbol"]: "ticker",
        SCREENER_COLUMNS["exchange"]: "exchange",
        SCREENER_COLUMNS["close"]: "close",
        SCREENER_COLUMNS["volume"]: "volume",
        SCREENER_COLUMNS["ema200"]: "ema200",
        SCREENER_COLUMNS["ath"]: "ath",
    }
    df.rename(columns=rename_map, inplace=True)

    required = {"ticker", "close", "ema200", "ath"}
    missing = required - set(df.columns)
    if missing:
        log.warning(
            "TradingView screener response missing required columns %s; using full tvdatafeed scan",
            sorted(missing),
        )
        return None

    df["ticker"] = df["ticker"].astype(str).str.upper()
    for col in ["close", "volume", "ema200", "ath"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.drop_duplicates(subset=["ticker"], keep="first")


def evaluate_screener_prefilter(snapshot: pd.DataFrame) -> tuple[set[str], list[tuple[str, str]]]:
    """Apply the cheap live filters that map cleanly to screener fields."""
    candidates: set[str] = set()
    rejected: list[tuple[str, str]] = []

    for row in snapshot.itertuples(index=False):
        ticker = getattr(row, "ticker")
        close = getattr(row, "close")
        ema200_val = getattr(row, "ema200")
        ath = getattr(row, "ath")
        volume = getattr(row, "volume", np.nan)

        reasons = []
        if pd.isna(close) or not (PRICE_ABOVE <= close <= PRICE_BELOW):
            reasons.append(f"price {close} outside [{PRICE_ABOVE}, {PRICE_BELOW}]")

        if VOLUME_ABOVE > 0 and (pd.isna(volume) or volume <= VOLUME_ABOVE):
            reasons.append(f"volume {volume} <= {VOLUME_ABOVE}")

        if RETRACEMENT_ENABLED:
            if pd.isna(ath) or ath <= 0:
                reasons.append("ATH unavailable")
            else:
                retr_pct = (ath - close) / ath
                if retr_pct > RETRACEMENT_PCT:
                    reasons.append(f"retracement {retr_pct:.1%} > {RETRACEMENT_PCT:.0%} from ATH")

        if EMA_FILTER_ENABLED:
            if pd.isna(ema200_val):
                reasons.append(f"EMA{EMA_FILTER_LEN} unavailable")
            elif close <= ema200_val:
                reasons.append(f"close {close:.2f} not above EMA{EMA_FILTER_LEN} {ema200_val:.2f}")

        if reasons:
            rejected.append((ticker, "; ".join(reasons)))
        else:
            candidates.add(ticker)

    return candidates, rejected


def market_regime_allows_new_entries(index_df: pd.DataFrame) -> bool:
    """Returns False when the Market Trend Filter says 'Only Exit - Stop New Entry'."""
    if not MARKET_FILTER_ENABLED:
        return True
    close = index_df["close"].iloc[-1]
    e = ema(index_df["close"], MARKET_FILTER_LEN).iloc[-1]
    bullish = close > e
    if not bullish and MARKET_FILTER_ACTION == "only_exit_stop_new_entry":
        return False
    return True


def normalize_ticker_symbol(value: str) -> str:
    return str(value).strip().upper().replace("&", "_")


def parse_holdings(value: Optional[str]) -> list[str]:
    if not value:
        return []
    return [normalize_ticker_symbol(part) for part in value.split(",") if part.strip()]


def build_portfolio_actions(
    ranked_df: pd.DataFrame,
    current_holdings: list[str],
    new_entries_allowed: bool,
) -> dict:
    """Apply simulator-style Top 30 / Exit Rank 60 portfolio construction."""
    holdings = list(dict.fromkeys(normalize_ticker_symbol(t) for t in current_holdings))
    if ranked_df.empty:
        return {
            "hold": [],
            "outgoing": holdings,
            "incoming": [],
            "suppressed_incoming": [],
        }

    rank_by_ticker = dict(zip(ranked_df["ticker"].astype(str).str.upper(), ranked_df["rank"]))
    hold = [ticker for ticker in holdings if rank_by_ticker.get(ticker, EXIT_RANK + 1) <= EXIT_RANK]
    outgoing = [ticker for ticker in holdings if ticker not in hold]

    selected = set(hold)
    candidate_incoming = [
        ticker
        for ticker in ranked_df["ticker"].astype(str).str.upper().tolist()
        if ticker not in selected
    ]
    slots = max(TOP_N - len(hold), 0)
    incoming = candidate_incoming[:slots] if new_entries_allowed else []
    suppressed_incoming = candidate_incoming[:slots] if not new_entries_allowed else []

    return {
        "hold": hold,
        "outgoing": outgoing,
        "incoming": incoming,
        "suppressed_incoming": suppressed_incoming,
    }


# =========================================================================
# Main scan
# =========================================================================

def scan_one_ticker(ticker: str, exchange: str, bench_df, asof: str):
    """Runs entirely inside a worker thread using that thread's own TV session."""
    df = trim_to_asof(fetch_history(ticker, exchange), asof)
    data_bars = len(df["close"].dropna())

    reasons_failed = []

    ok_pv, r = passes_price_volume(df)
    reasons_failed += r

    ok_retr, ath, retr_pct, r = passes_retracement(df)
    reasons_failed += r

    ok_ema, ema200_val, r = passes_ema_filter(df)
    reasons_failed += r

    ok_rs, r = passes_relative_strength(df, bench_df) if bench_df is not None else (True, [])
    reasons_failed += r

    momentum_score, momentum_valid, r = compute_momentum_score_checked(df)
    if REQUIRE_VALID_MOMENTUM and not momentum_valid:
        reasons_failed += r

    sharpe = sharpe_return(df["close"], length=252)
    passed = ok_pv and ok_retr and ok_ema and ok_rs and (momentum_valid or not REQUIRE_VALID_MOMENTUM)

    return ScanResult(
        ticker=ticker,
        close=df["close"].iloc[-1],
        ath=ath,
        retracement_pct=retr_pct,
        ema200=ema200_val,
        above_ema200=ok_ema,
        rs_above_rs_ema200=ok_rs,
        momentum_score=momentum_score,
        momentum_valid=momentum_valid,
        sharpe=sharpe,
        passed=passed,
        data_bars=data_bars,
        reasons_failed=reasons_failed,
    )


def run_scan(
    asof: str = "live",
    max_workers: int = MAX_WORKERS,
    use_screener: bool = SCREENER_ENABLED,
    current_holdings: Optional[list[str]] = None,
    save_results: bool = True,
):
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise SystemExit("Set NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env.local")

    start_time = time.time()

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    universe = get_n750_tickers(supabase)
    universe_total = len(universe)

    log.info(
        "Running scan as-of: %s | workers: %d | live screener prefilter: %s",
        "YESTERDAY'S CLOSE (T-1)" if asof == "t-1" else "LIVE / LATEST CLOSE",
        max_workers,
        "ON" if use_screener and asof == "live" else "OFF",
    )

    # --- global market regime gate (also respects --yesterday) ---
    # Done on the main thread, sequentially, before the pool starts.
    index_df = trim_to_asof(fetch_history(MARKET_INDEX_SYMBOL, MARKET_INDEX_EXCHANGE), asof)
    new_entries_allowed = market_regime_allows_new_entries(index_df)
    market_date = index_df.index[-1]
    asof_date = pd.Timestamp(market_date).date().isoformat()
    log.info(
        "Market Trend Filter (as of %s): Nifty50 %s its %d-EMA -> new entries %s",
        market_date,
        "above" if index_df["close"].iloc[-1] > ema(index_df["close"], MARKET_FILTER_LEN).iloc[-1] else "below",
        MARKET_FILTER_LEN,
        "ALLOWED" if new_entries_allowed else "BLOCKED (exit-only mode)",
    )

    # --- benchmark for relative strength ---
    bench_df = None
    if RELATIVE_MOMENTUM_ENABLED:
        bench_df = trim_to_asof(fetch_history(BENCHMARK_SYMBOL, BENCHMARK_EXCHANGE), asof)

    scan_universe = universe
    screener_rejected: list[tuple[str, str]] = []
    screener_missing: set[str] = set()
    screener_used = False

    if asof == "live" and use_screener:
        snapshot = fetch_live_screener_snapshot(universe)
        if snapshot is not None:
            screener_candidates, screener_rejected = evaluate_screener_prefilter(snapshot)
            universe_tickers = {str(t).strip().upper() for t in universe["ticker"].dropna() if str(t).strip()}
            returned_tickers = set(snapshot["ticker"])
            screener_missing = universe_tickers - returned_tickers

            # Missing screener rows are scanned historically so symbol mismatches
            # or transient screener gaps do not silently drop valid candidates.
            scan_tickers = screener_candidates | screener_missing
            scan_universe = universe[universe["ticker"].astype(str).str.upper().isin(scan_tickers)].copy()
            screener_used = True

            log.info(
                "TradingView screener prefilter: %d rows returned | %d passed cheap filters | "
                "%d rejected | %d missing -> historical fallback | %d queued for tvdatafeed",
                len(snapshot),
                len(screener_candidates),
                len(screener_rejected),
                len(screener_missing),
                len(scan_universe),
            )
        else:
            log.info("Continuing with full tvdatafeed scan for all %d tickers", universe_total)
    elif asof == "t-1" and use_screener:
        log.info("--yesterday requested: skipping screener because it exposes live/current fields only")

    scan_total = len(scan_universe)
    results: list[ScanResult] = []
    skipped: list[tuple] = []  # (ticker, reason)
    progress_lock = threading.Lock()
    completed = 0

    def log_progress():
        elapsed = time.time() - start_time
        rate = elapsed / completed
        remaining = rate * (scan_total - completed)
        passed_so_far = sum(1 for r_ in results if r_.passed)
        log.info(
            "Progress: %d/%d (%.0f%%) | passed so far: %d | skipped so far: %d | "
            "elapsed: %.1fmin | ETA: %.1fmin",
            completed, scan_total, 100 * completed / scan_total, passed_so_far, len(skipped),
            elapsed / 60, remaining / 60,
        )

    if scan_total:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_ticker = {
                pool.submit(
                    scan_one_ticker,
                    row["ticker"],
                    row.get("exchange", "NSE") or "NSE",
                    bench_df,
                    asof,
                ): row["ticker"]
                for _, row in scan_universe.iterrows()
            }

            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    result = future.result()
                    with progress_lock:
                        results.append(result)
                except Exception as exc:
                    log.warning("Skipping %s after %d attempts: %s", ticker, RETRY_COUNT, exc)
                    with progress_lock:
                        skipped.append((ticker, str(exc)))

                with progress_lock:
                    completed += 1
                    if completed % PROGRESS_LOG_EVERY == 0 or completed == scan_total:
                        log_progress()
    else:
        log.info("No tickers queued for tvdatafeed after screener prefilter")

    result_df = pd.DataFrame([r.__dict__ for r in results])
    invalid_momentum_df = (
        result_df[~result_df["momentum_valid"]].copy()
        if not result_df.empty and "momentum_valid" in result_df.columns else pd.DataFrame()
    )
    passed_df = (
        result_df[
            result_df["passed"]
            & result_df["momentum_valid"]
            & result_df["momentum_score"].notna()
        ].copy()
        if not result_df.empty else result_df
    )

    # Rank criteria = Sharpe Return
    if not passed_df.empty:
        if RANK_CRITERIA == "sharpe":
            passed_df = passed_df.sort_values("sharpe", ascending=False)
        else:
            passed_df = passed_df.sort_values("momentum_score", ascending=False)
        passed_df["rank"] = range(1, len(passed_df) + 1)

    top_n_df = passed_df[passed_df["rank"] <= TOP_N] if not passed_df.empty else passed_df
    buffer_zone_df = (
        passed_df[(passed_df["rank"] > TOP_N) & (passed_df["rank"] <= EXIT_RANK)]
        if not passed_df.empty else passed_df
    )
    portfolio_actions = build_portfolio_actions(
        passed_df,
        current_holdings or [],
        new_entries_allowed,
    )
    saved_count = 0
    if save_results:
        saved_count = save_momentum_snapshot(
            supabase,
            passed_df,
            asof_date,
            new_entries_allowed,
        )

    total_elapsed = time.time() - start_time

    # --- final summary ---
    log.info("=" * 70)
    log.info("SCAN SUMMARY (as of: %s)", "T-1 / yesterday's close" if asof == "t-1" else "live / latest close")
    log.info("  Universe fetched from Supabase : %d", universe_total)
    if screener_used:
        log.info("  Rejected by live screener       : %d", len(screener_rejected))
        log.info("  Missing from screener fallback  : %d", len(screener_missing))
    log.info("  Queued for tvdatafeed           : %d", scan_total)
    log.info("  Successfully fetched from TV    : %d", scan_total - len(skipped))
    log.info("  Skipped (failed after retries) : %d", len(skipped))
    log.info("  Invalid momentum / no 252 ROC   : %d", len(invalid_momentum_df))
    log.info("  Passed all filters             : %d", len(passed_df))
    log.info("  Top-%d                          : %d", TOP_N, len(top_n_df))
    log.info("  Buffer zone (rank %d-%d)        : %d", TOP_N + 1, EXIT_RANK, len(buffer_zone_df))
    log.info("  Market Trend Filter             : %s", "new entries ALLOWED" if new_entries_allowed else "EXIT-ONLY (no new entries)")
    log.info("  Saved to Supabase %s.%s : %d", MOMENTUM_SCHEMA, MOMENTUM_TABLE, saved_count)
    log.info("  Total time                      : %.1f min", total_elapsed / 60)
    if skipped:
        preview = ", ".join(t for t, _ in skipped[:30])
        log.info("  Skipped tickers: %s%s", preview, " ..." if len(skipped) > 30 else "")
    if not invalid_momentum_df.empty:
        preview = ", ".join(invalid_momentum_df["ticker"].head(30).astype(str).tolist())
        log.info(
            "  Invalid momentum tickers: %s%s",
            preview,
            " ..." if len(invalid_momentum_df) > 30 else "",
        )
    if current_holdings:
        log.info("  Portfolio Hold                 : %s", ", ".join(portfolio_actions["hold"]) or "(none)")
        log.info("  Portfolio Outgoing             : %s", ", ".join(portfolio_actions["outgoing"]) or "(none)")
        if new_entries_allowed:
            log.info("  Portfolio Incoming             : %s", ", ".join(portfolio_actions["incoming"]) or "(none)")
        else:
            log.info(
                "  Portfolio Incoming             : (blocked by market trend filter; candidates: %s)",
                ", ".join(portfolio_actions["suppressed_incoming"]) or "(none)",
            )
    log.info("=" * 70)

    return {
        "all_results": result_df,
        "passed": passed_df,
        "top_n": top_n_df,
        "buffer_zone_60": buffer_zone_df,
        "new_entries_allowed": new_entries_allowed,
        "skipped": skipped,
        "invalid_momentum": invalid_momentum_df,
        "portfolio_actions": portfolio_actions,
        "saved_count": saved_count,
        "asof_date": asof_date,
        "screener_used": screener_used,
        "screener_rejected": screener_rejected,
        "screener_missing": sorted(screener_missing),
        "asof": asof,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Momentum Investing Scanner (N750)")
    parser.add_argument(
        "--yesterday", action="store_true",
        help="Anchor all calculations to yesterday's completed close (T-1) instead "
             "of the latest/live bar. Use this if you're running mid-session and want "
             "deterministic results unaffected by today's in-progress candle. This also "
             "applies to the Market Trend Filter (Nifty50 vs its 100-EMA).",
    )
    parser.add_argument(
        "--workers", type=int, default=MAX_WORKERS,
        help=f"Number of parallel TV sessions/threads to use (default: {MAX_WORKERS}). "
             "Raise for more throughput, lower if you see more connection errors.",
    )
    parser.add_argument(
        "--no-screener", action="store_true",
        help="Disable the live TradingView screener prefilter and use tvdatafeed for every ticker. "
             "This flag has no effect with --yesterday because T-1 scans already require tvdatafeed.",
    )
    parser.add_argument(
        "--holdings", default="",
        help="Comma-separated current portfolio holdings. When provided, the scanner prints "
             "Hold / Outgoing / Incoming using Top 30 and Exit Rank 60 simulator rules.",
    )
    parser.add_argument(
        "--no-save", action="store_true",
        help=f"Run the scan without refreshing the Supabase {MOMENTUM_SCHEMA}.{MOMENTUM_TABLE} table.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asof_mode = "t-1" if args.yesterday else "live"

    # Rebuild the semaphore to match a --workers override so the connection
    # cap and the thread pool size stay in sync.
    MAX_WORKERS = args.workers
    _connection_semaphore = threading.Semaphore(MAX_WORKERS)

    current_holdings = parse_holdings(args.holdings)
    out = run_scan(
        asof=asof_mode,
        max_workers=args.workers,
        use_screener=not args.no_screener,
        current_holdings=current_holdings,
        save_results=not args.no_save,
    )
    print("\n=== TOP", TOP_N, "===")
    if out["top_n"].empty:
        print("(no stocks passed all filters)")
    else:
        print(out["top_n"][["rank", "ticker", "close", "momentum_score", "sharpe", "data_bars"]].to_string(index=False))

    if not out["invalid_momentum"].empty:
        print("\n=== INVALID MOMENTUM / INSUFFICIENT 252-DAY ROC DATA ===")
        print(
            out["invalid_momentum"][["ticker", "data_bars", "momentum_score", "reasons_failed"]]
            .to_string(index=False)
        )

    if current_holdings:
        actions = out["portfolio_actions"]
        print("\n=== PORTFOLIO ACTIONS ===")
        print("Hold:", ", ".join(actions["hold"]) or "(none)")
        print("Outgoing:", ", ".join(actions["outgoing"]) or "(none)")
        if out["new_entries_allowed"]:
            print("Incoming:", ", ".join(actions["incoming"]) or "(none)")
        else:
            print("Incoming: (blocked by Market Trend Filter)")
            print("Suppressed incoming candidates:", ", ".join(actions["suppressed_incoming"]) or "(none)")

    print(f"\nNew entries allowed (Market Trend Filter): {out['new_entries_allowed']}")
    print(f"Saved to Supabase {MOMENTUM_SCHEMA}.{MOMENTUM_TABLE}: {out['saved_count']} rows")
    if out["skipped"]:
        print(f"Skipped {len(out['skipped'])} tickers (see log above for details)")
