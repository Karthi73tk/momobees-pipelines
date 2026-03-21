"""
market_pulse_global.py
──────────────────────
Fetches OHLCV + indicator data for global indices via tvDatafeed,
computes trend scores, and upserts results into Supabase.

Dependencies:
    pip install tvDatafeed pandas numpy supabase python-dotenv ta

Usage:
    python market_pulse_global.py                  # run once
    python market_pulse_global.py --dry-run        # print rows, no DB write
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib
import time
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from tvDatafeed import TvDatafeed, Interval

# ── Optional TA library for RSI (falls back to manual if not installed) ────────
try:
    from ta.momentum import RSIIndicator
    HAS_TA = True
except ImportError:
    HAS_TA = False

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

# Load .env.local first (Next.js convention), fall back to .env
load_dotenv(".env.local")
load_dotenv()  # fallback

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Markets definition ─────────────────────────────────────────────────────────
# ticker format: "EXCHANGE:SYMBOL"  (as used by TradingView / tvDatafeed)
MARKETS: list[dict] = [
    # Americas
    {"region": "Americas",     "country": "USA",         "index_name": "S&P 500",           "ticker": "SP:SPX"},
    {"region": "Americas",     "country": "USA",         "index_name": "NASDAQ 100",         "ticker": "TVC:NDX"},
    {"region": "Americas",     "country": "Canada",      "index_name": "S&P/TSX Composite",  "ticker": "TSX:TSX"},
    {"region": "Americas",     "country": "Brazil",      "index_name": "Bovespa",            "ticker": "BMFBOVESPA:IBOV"},
    # Asia-Pacific
    {"region": "Asia-Pacific", "country": "India",       "index_name": "Nifty 50",           "ticker": "NSE:NIFTY"},
    {"region": "Asia-Pacific", "country": "Japan",       "index_name": "Nikkei 225",         "ticker": "TVC:NI225"},
    {"region": "Asia-Pacific", "country": "Hong Kong",   "index_name": "Hang Seng",          "ticker": "TVC:HSI"},
    {"region": "Asia-Pacific", "country": "China",       "index_name": "Shanghai Composite", "ticker": "SSE:000001"},
    {"region": "Asia-Pacific", "country": "South Korea", "index_name": "KOSPI",              "ticker": "TVC:KOSPI"},
    {"region": "Asia-Pacific", "country": "Australia",   "index_name": "ASX 200",            "ticker": "ASX:XJO"},
    # Europe
    {"region": "Europe",       "country": "Germany",     "index_name": "DAX 40",             "ticker": "TVC:DAX"},
    {"region": "Europe",       "country": "UK",          "index_name": "FTSE 100",           "ticker": "FTSE:UKX"},
    {"region": "Europe",       "country": "France",      "index_name": "CAC 40",             "ticker": "EURONEXT:PX1"},
    {"region": "Europe",       "country": "Pan-European","index_name": "Euro Stoxx 50",      "ticker": "TVC:SX5E"},
]

# How many daily bars to fetch (need at least 252 for 52-week high/low + EMAs)
BARS = 300

# Fallback tickers — tried automatically if the primary returns 0 bars
TICKER_FALLBACKS: dict[str, list[str]] = {
    "TSX:TSX":       ["TVC:TSX"],      # Reliable TV-calculated version
    "SSE:000001":    ["TVC:SHCOMP"],   # Shanghai Composite fallback
    "ASX:XJO":       ["TVC:XJO"],      # ASX 200 fallback
    "STOXX:SX5E":    ["TVC:SX5E"],     # Euro Stoxx 50 fallback
    "FTSE:UKX":      ["TVC:UKX"],      # FTSE 100 fallback
}
# Retry settings for tvDatafeed
MAX_RETRIES: int    = 4       # 1 original + 3 retries
BACKOFF_BASE: float = 2.0     # exponential: 2s -> 4s -> 8s -> 16s
BACKOFF_MAX: float  = 60.0
MIN_BARS: int       = 20      # minimum bars to consider a fetch valid


# ══════════════════════════════════════════════════════════════════════════════
# INDICATOR HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def _sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period).mean()


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    if HAS_TA:
        return RSIIndicator(close=series, window=period).rsi()
    # Manual Wilder RSI
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _perf_pct(series: pd.Series, n: int) -> Optional[float]:
    """% change over last n bars. Returns None if not enough data."""
    if len(series) <= n:
        return None
    return round(float((series.iloc[-1] / series.iloc[-1 - n] - 1) * 100), 4)


def _ytd_pct(df: pd.DataFrame) -> Optional[float]:
    """% change from last trading day of previous year to latest close."""
    if df.empty:
        return None
    current_year = df.index[-1].year
    prev_year_df = df[df.index.year < current_year]
    if prev_year_df.empty:
        return None
    prev_close = float(prev_year_df["close"].iloc[-1])
    cur_close = float(df["close"].iloc[-1])
    return round((cur_close / prev_close - 1) * 100, 4)


def _trend_score(row: dict) -> float:
    """
    Compute a 0–100 trend score identical to SectorBreadthClient logic.

    Components (mirrors the weighted EMA breadth in the React component):
      30% — above EMA-20   (short-term)
      30% — above EMA-50   (mid-term)
      40% — above EMA-200  (long-term / structural)

    Each boolean is worth its weight × 100; averaged into 0-100.
    RSI nudge: ±5 pts based on RSI relative to 50 midline (clamped).
    """
    score = 0.0
    score += 30.0 if row.get("above_ema_20")  else 0.0
    score += 30.0 if row.get("above_ema_50")  else 0.0
    score += 40.0 if row.get("above_ema_200") else 0.0

    # RSI nudge — scales from -5 to +5
    rsi = row.get("rsi_14")
    if rsi is not None and not np.isnan(float(rsi)):
        rsi = float(rsi)
        nudge = (rsi - 50) / 50 * 5   # -5 .. +5
        score = max(0.0, min(100.0, score + nudge))

    return round(score, 2)


def _trend_label(score: float) -> str:
    if score >= 70: return "Bullish"
    if score >= 55: return "Leaning Bull"
    if score >= 40: return "Neutral"
    if score >= 25: return "Leaning Bear"
    return "Bearish"


# ══════════════════════════════════════════════════════════════════════════════
# DATA FETCH
# ══════════════════════════════════════════════════════════════════════════════

def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "429" in msg or "too many requests" in msg


def _try_fetch(tv: TvDatafeed, ticker: str) -> Optional[pd.DataFrame]:
    """
    Fetch a single ticker with exponential backoff retry.

    Retries on:
      - 429 Too Many Requests  (recreates TvDatafeed instance to drop poisoned connection)
      - Any other exception    (transient network / WebSocket errors)
      - Empty / insufficient data response  (also retried — TV can return partial data)

    Returns a DataFrame on success, None after all retries are exhausted.
    """
    exchange, symbol = ticker.split(":")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = tv.get_hist(
                symbol=symbol,
                exchange=exchange,
                interval=Interval.in_daily,
                n_bars=BARS,
            )

            if df is not None and not df.empty and len(df) >= MIN_BARS:
                return df

            # Insufficient data — retry in case TV returned a partial response
            bars_got = len(df) if df is not None else 0
            if attempt < MAX_RETRIES:
                wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
                log.warning(
                    "  [%s] insufficient data (%d bars) attempt %d/%d -- retrying in %.0fs ...",
                    ticker, bars_got, attempt, MAX_RETRIES, wait,
                )
                time.sleep(wait)
            else:
                log.error("  [%s] insufficient data (%d bars) after %d attempts.", ticker, bars_got, MAX_RETRIES)
                return None

        except Exception as exc:
            is_429 = _is_rate_limit_error(exc)

            if attempt < MAX_RETRIES:
                wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
                if is_429:
                    # Recreate the TvDatafeed instance — the WebSocket may be poisoned
                    try:
                        tv.__init__()
                    except Exception:
                        pass
                    log.warning(
                        "  [%s] 429 Too Many Requests attempt %d/%d -- backing off %.0fs ...",
                        ticker, attempt, MAX_RETRIES, wait,
                    )
                else:
                    log.warning(
                        "  [%s] %s: %s attempt %d/%d -- retrying in %.0fs ...",
                        ticker, type(exc).__name__, exc, attempt, MAX_RETRIES, wait,
                    )
                time.sleep(wait)
            else:
                log.error(
                    "  [%s] giving up after %d attempts. Last error: %s: %s",
                    ticker, MAX_RETRIES, type(exc).__name__, exc,
                )
                return None

    return None


def fetch_market_data(tv: TvDatafeed, market: dict) -> Optional[dict]:
    """Fetch OHLCV for one market, compute all indicators, return a flat dict."""
    ticker = market["ticker"]

    # Try primary ticker, then any registered fallbacks
    candidates = [ticker] + TICKER_FALLBACKS.get(ticker, [])
    df = None
    used_ticker = ticker
    for candidate in candidates:
        df = _try_fetch(tv, candidate)
        if df is not None:
            used_ticker = candidate
            if candidate != ticker:
                log.info(f"  [{ticker}] primary failed — using fallback {candidate}")
            break

    if df is None:
        log.warning(f"  [{ticker}] all candidates failed: {candidates}")
        return None

    close = df["close"].dropna()
    if len(close) < 20:
        log.warning(f"  [{used_ticker}] not enough non-null closes")
        return None

    # ── Moving averages ──
    ema20  = _ema(close, 20)
    ema50  = _ema(close, 50)
    ema200 = _ema(close, 200)
    sma50  = _sma(close, 50)
    sma200 = _sma(close, 200)
    rsi14  = _rsi(close, 14)

    cur      = float(close.iloc[-1])
    prev     = float(close.iloc[-2]) if len(close) >= 2 else None

    def last(s: pd.Series) -> Optional[float]:
        v = s.iloc[-1]
        return None if pd.isna(v) else round(float(v), 4)

    e20  = last(ema20)
    e50  = last(ema50)
    e200 = last(ema200)
    s50  = last(sma50)
    s200 = last(sma200)
    rsi  = last(rsi14)

    # ── 52-week high / low ──
    window_252 = close.iloc[-252:] if len(close) >= 252 else close
    high_52w = float(window_252.max())
    low_52w  = float(window_252.min())
    pct_from_high = round((cur / high_52w - 1) * 100, 4) if high_52w else None

    # ── Performance ──
    row: dict = {
        **market,
        "close":             round(cur, 4),
        "prev_close":        round(prev, 4) if prev else None,
        "change_pct_1d":     round((cur / prev - 1) * 100, 4) if prev else None,
        "change_pct_1w":     _perf_pct(close, 5),
        "change_pct_1m":     _perf_pct(close, 21),
        "change_pct_3m":     _perf_pct(close, 63),
        "change_pct_ytd":    _ytd_pct(df),
        "high_52w":          round(high_52w, 4),
        "low_52w":           round(low_52w, 4),
        "pct_from_52w_high": pct_from_high,
        "ema_20":            e20,
        "ema_50":            e50,
        "ema_200":           e200,
        "sma_50":            s50,
        "sma_200":           s200,
        "above_ema_20":      bool(cur > e20)  if e20  else None,
        "above_ema_50":      bool(cur > e50)  if e50  else None,
        "above_ema_200":     bool(cur > e200) if e200 else None,
        "above_sma_200":     bool(cur > s200) if s200 else None,
        "rsi_14":            rsi,
        "updated_at":        datetime.now(timezone.utc).isoformat(),
    }

    ts = _trend_score(row)
    row["trend_score"] = ts
    row["trend_label"] = _trend_label(ts)

    rsi_str = f"{rsi:.1f}" if rsi is not None else "N/A"
    log.info(
        f"  ✓ {used_ticker:<26}  close={cur:>10,.2f}  "
        f"EMA20={'↑' if row['above_ema_20'] else '↓'}  "
        f"EMA50={'↑' if row['above_ema_50'] else '↓'}  "
        f"EMA200={'↑' if row['above_ema_200'] else '↓'}  "
        f"RSI={rsi_str}  "
        f"Score={ts}  [{row['trend_label']}]"
    )
    return row


# ══════════════════════════════════════════════════════════════════════════════
# SUPABASE UPSERT
# ══════════════════════════════════════════════════════════════════════════════

def upsert_to_supabase(supabase: Client, rows: list[dict]) -> None:
    """Upsert all rows into global_market_breadth, conflict on ticker."""
    if not rows:
        log.warning("No rows to upsert.")
        return

    # Supabase / PostgREST wants plain Python types, not numpy
    clean = []
    for r in rows:
        cr = {}
        for k, v in r.items():
            if isinstance(v, (np.integer,)):
                cr[k] = int(v)
            elif isinstance(v, (np.floating,)):
                cr[k] = None if np.isnan(float(v)) else float(v)
            elif isinstance(v, (np.bool_,)):
                cr[k] = bool(v)
            else:
                cr[k] = v
        clean.append(cr)

    result = (
        supabase.table("global_market_breadth")
        .upsert(clean, on_conflict="ticker")
        .execute()
    )
    log.info(f"Upserted {len(clean)} rows → global_market_breadth")
    return result


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main(dry_run: bool = False) -> None:
    log.info("═" * 60)
    log.info("  Market Pulse Global — data updater")
    log.info("═" * 60)

    # ── Connect to TradingView (anonymous is fine for index data) ──────────────
    # For higher reliability / no rate limits, pass username + password:
    #   tv = TvDatafeed(username="your@email.com", password="yourpassword")
    tv = TvDatafeed()
    log.info("Connected to TvDatafeed (anonymous)")

    # ── Fetch all markets ─────────────────────────────────────────────────────
    rows: list[dict] = []
    for i, market in enumerate(MARKETS, 1):
        log.info(f"[{i:02}/{len(MARKETS)}] Fetching {market['index_name']} ({market['ticker']})")
        row = fetch_market_data(tv, market)
        if row:
            rows.append(row)
        # Small delay to be polite to the TV servers
        time.sleep(1.5)

    log.info(f"\nFetched {len(rows)}/{len(MARKETS)} markets successfully")

    if dry_run:
        log.info("\n── DRY RUN — printing rows, skipping DB write ──")
        df = pd.DataFrame(rows)
        cols = ["region", "country", "index_name", "close", "change_pct_1d",
                "trend_score", "trend_label", "above_ema_20", "above_ema_50", "above_ema_200"]
        print(df[[c for c in cols if c in df.columns]].to_string(index=False))
        return

    # ── Connect to Supabase ───────────────────────────────────────────────────
    # Supports both Next.js-style NEXT_PUBLIC_ keys and plain keys
    url = (
        os.environ.get("NEXT_PUBLIC_SUPABASE_URL") or
        os.environ.get("SUPABASE_URL")
    )
    key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or   # preferred for writes
        os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")  # fallback (anon key)
    )
    if not url or not key:
        raise EnvironmentError(
            "Could not find Supabase credentials."
            "Expected in .env.local: NEXT_PUBLIC_SUPABASE_URL and either"
            "  SUPABASE_SERVICE_ROLE_KEY (preferred) or NEXT_PUBLIC_SUPABASE_ANON_KEY"
        )
    supabase: Client = create_client(url, key)
    log.info(f"Connected to Supabase: {url}")

    upsert_to_supabase(supabase, rows)

    log.info("Done ✓")

    # ── Write result JSON for GitHub Actions summary ──────────────────────────
    failed = len(MARKETS) - len(rows)
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/market_pulse.json").write_text(json.dumps({
        "script":    "Market Pulse Global",
        "succeeded": len(rows),
        "failed":    failed,
        "skipped":   0,
        "total":     len(MARKETS),
        "errors":    [m["ticker"] for m in MARKETS
                      if m["ticker"] not in {r["ticker"] for r in rows}][:10],
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Market Pulse Global data updater")
    parser.add_argument("--dry-run", action="store_true", help="Fetch data but skip DB write")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
