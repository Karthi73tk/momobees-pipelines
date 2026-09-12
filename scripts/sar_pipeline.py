"""
13/34 EMA Stop-and-Reverse (NIFTY, 75-min) — forward-test pipeline  ->  sar.*
--------------------------------------------------------------------------------
An always-in-market trend follower on NIFTY, evaluated on 75-min candles that are
reconstructed from tvDatafeed 15-min bars of the NIFTY CONTINUOUS FUTURE (NIFTY1!).

Rules (Variant A, verbatim from the study handoff):
  • EMA_fast = EMA(13, close), EMA_slow = EMA(34, close); warm-up 34 bars.
  • The 13 EMA is the trailing stop; a NEW entry needs a close beyond BOTH EMAs.
      FLAT  : close>EMA13 and close>EMA34 -> LONG ; close<both -> SHORT
      LONG  : close<EMA13 -> exit; if also close<EMA34 -> reverse to SHORT (same bar)
      SHORT : close>EMA13 -> exit; if also close>EMA34 -> reverse to LONG  (same bar)
  • Fill at the CLOSE of the signal bar. No independent stop-loss (the 13 EMA is it).
  • Positional; carries overnight; last decision each day is the 14:15 bar (no 15:30).

Book / money model (per the integration brief):
  • ₹10L capital, start 2 lots, lot_size 65. Banded-ratchet sizing:
      +1 lot per +₹3L above the reference; −1 lot per −₹1.25L·lots drawdown
      from the reference (2 lots tolerate ₹2.5L before the first cut); floor 1.
      The reference moves with each add/cut; lots change only at a trade close.
  • ₹P&L per trade = net_points · lot_size · lots.
  • net_points = gross_points − cost_points, cost_points = 0.00013240·S + 1.45458
    (S = mid of entry/exit) — the study's canonical [Dec+Scaled@0.5] option-cost
    model, refit to <0.001 pt against the 2026 trade log.

Forward test:
  • Seeded with the 90 backtested 2026 trades (index spot), 10L start, compounded.
  • Book handed off FLAT at 2026-08-10 14:15 (the last backtest bar); from there the
    live futures state machine runs forward and records its own trades (source='live').

Idempotent: only 75-min bars newer than book.last_processed_ts are ever acted on,
so the SAME script serves the EOD run and the every-75-min intraday checks. The
in-progress (not-yet-closed) 75-min bar is always dropped.

Telegram: fires on every long/short SWITCH, every EXIT, and every fresh ENTRY.

Usage:
    python sar_pipeline.py --seed            # one-time: replay 2026 backtest trades
    python sar_pipeline.py                   # every-75-min check (guards on the clock)
    python sar_pipeline.py --eod             # authoritative daily close (stamps is_eod)
    python sar_pipeline.py --force           # ignore the market-hours clock guard
    python sar_pipeline.py --preview-only    # compute + print, no DB writes, no Telegram
Requires: tvDatafeed, supabase, pandas, python-dotenv.
"""
from __future__ import annotations
import os, sys, csv, json, math, argparse, logging, pathlib
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

ROOT = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env.local"); load_dotenv(ROOT / "scripts" / ".env.local")
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
                or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY"))
SCHEMA = "sar"
IST = ZoneInfo("Asia/Kolkata")

# ── strategy / book constants ────────────────────────────────────────────────
FEED_SYMBOL   = "NIFTY1!"      # tvDatafeed continuous front-month future
EXCHANGE      = "NSE"
EMA_FAST, EMA_SLOW = 13, 34
START_CAPITAL = 1_000_000.0    # ₹10L
START_LOTS    = 2              # start 2 lots on ₹10L
LOT_SIZE      = 65             # current NIFTY F&O lot
# ── banded-ratchet position sizing ───────────────────────────────────────────
# Add 1 lot for every +₹3L above the reference (compounding up); cut 1 lot when
# equity falls ₹1.25L·lots below the reference (so 2 lots tolerate a ₹2.5L
# drawdown before the first cut). The reference moves with each add/cut, and the
# drawdown is therefore measured from the level where the current lots were set
# (NOT the running peak). Floor at 1 lot. Lots change only at a trade close
# (realised equity); an open position holds its entry lots to the close.
ADD_STEP      = 300_000.0      # +₹3L profit → +1 lot
DD_PER_LOT    = 125_000.0      # −₹1.25L per lot of drawdown → −1 lot
MIN_LOTS      = 1
WARMUP        = 34
N_BARS_15M    = 6000           # ~240 sessions of 15-min bars (warmup + live window)
BOUNDARY_TS   = pd.Timestamp("2026-08-10 14:15:00")   # last backtest 75-min bar
# canonical option round-trip cost, points/lot, refit to the 2026 trade log:
COST_A, COST_B = 0.00013240, 1.45458     # cost = COST_A*S + COST_B
SEED_CSV = ROOT / "data" / "sar_seed_2026.csv"

# fixed 75-min session buckets (no 15:30 bar; last decision is 14:15)
BUCKETS = [dtime(9, 15), dtime(10, 30), dtime(11, 45), dtime(13, 0), dtime(14, 15)]
BAR_LEN = timedelta(minutes=75)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("sar")


def sb_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def apply_ladder(equity: float, lots: int, ref: float):
    """Banded ratchet: returns (lots, ref) after folding realised `equity`.
    Add on the way up (+₹3L → +1 lot); cut on the way down (−₹1.25L·lots → −1
    lot, floored at 1). The reference moves with each step (hysteresis band)."""
    while equity - ref >= ADD_STEP:
        lots += 1
        ref += ADD_STEP
    while lots > MIN_LOTS and (equity - ref) <= -(DD_PER_LOT * lots):
        ref -= DD_PER_LOT * lots
        lots -= 1
    return lots, ref


def cost_points(entry: float, exit_: float) -> float:
    return COST_A * (0.5 * (entry + exit_)) + COST_B


def market_open(now=None) -> bool:
    now = now or datetime.now(IST)
    return now.weekday() < 5 and dtime(9, 15) <= now.time() <= dtime(15, 40)


# ── 75-min reconstruction from tvDatafeed 15-min continuous future ───────────
def _bucket(t: dtime) -> dtime:
    b = BUCKETS[0]
    for s in BUCKETS:
        if t >= s:
            b = s
        else:
            break
    return b


def fetch_75min() -> pd.DataFrame:
    """tvDatafeed 15-min NIFTY1! -> reconstructed 75-min OHLC + EMA13/EMA34.
    Indexed by IST-naive start-of-candle ts. The in-progress bar is dropped."""
    from tvDatafeed import TvDatafeed, Interval
    raw = TvDatafeed().get_hist(FEED_SYMBOL, EXCHANGE, Interval.in_15_minute, n_bars=N_BARS_15M)
    if raw is None or raw.empty:
        raise RuntimeError("tvDatafeed returned no 15-min data for " + FEED_SYMBOL)
    m = raw.reset_index().rename(columns={"datetime": "dt"})
    m["dt"] = pd.to_datetime(m["dt"])
    m["date"] = m["dt"].dt.date
    m["bkt"] = m["dt"].dt.time.map(_bucket)
    agg = (m.groupby(["date", "bkt"])
             .agg(open=("open", "first"), high=("high", "max"),
                  low=("low", "min"), close=("close", "last"))
             .reset_index())
    agg["ts"] = agg.apply(lambda r: pd.Timestamp.combine(r["date"], r["bkt"]), axis=1)
    agg = agg.sort_values("ts").set_index("ts")[["open", "high", "low", "close"]]

    # drop the currently-forming 75-min bar (its bucket has not closed yet)
    now_ist = datetime.now(IST).replace(tzinfo=None)
    agg = agg[agg.index + BAR_LEN <= pd.Timestamp(now_ist)]

    agg["ema_fast"] = agg["close"].ewm(span=EMA_FAST, adjust=False).mean()
    agg["ema_slow"] = agg["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    return agg


# ── SAR state machine (Variant A) — advance the book across NEW bars ─────────
def advance(df: pd.DataFrame, start_state: str, entry_ts, entry_price, lots,
            equity: float, net_cum: float, after_ts, ref: float):
    """Walk bars with ts > after_ts, applying the SAR rules from `start_state`.
    `lots`/`ref` carry the banded-ratchet sizing state; lots change only at a
    close (via apply_ladder on the new realised equity). An open position holds
    its entry lots to the close.
    Returns (trades, final_state, entry_ts, entry_price, lots, equity, net_cum,
             equity_points, opens, ref)."""
    STATE = {"LONG": 1, "SHORT": -1, "FLAT": 0}
    NAME = {1: "LONG", -1: "SHORT", 0: "FLAT"}
    state = STATE[start_state]
    trades, eq_points, opens = [], [], []

    def do_close(ts, exit_price, reason):
        nonlocal equity, net_cum, lots, ref
        direction = "Long" if state == 1 else "Short"
        gross = (exit_price - entry_price) if state == 1 else (entry_price - exit_price)
        cp = cost_points(entry_price, exit_price)
        net = gross - cp
        net_cum += net
        pnl = net * LOT_SIZE * lots            # position held at its entry lots
        equity += pnl
        trades.append(dict(
            symbol="NIFTY", entry_ts=pd.Timestamp(entry_ts), direction=direction,
            entry_price=round(float(entry_price), 2), exit_ts=pd.Timestamp(ts),
            exit_price=round(float(exit_price), 2), exit_reason=reason,
            gross_points=round(float(gross), 3), cost_points=round(float(cp), 3),
            net_points=round(float(net), 3),
            lots=lots, lot_size=LOT_SIZE, pnl_inr=round(float(pnl), 2),
            win_lose="Win" if net > 0 else "Lose",
            cum_net_points=round(float(net_cum), 3), equity_after=round(float(equity), 2),
            source="live", year=pd.Timestamp(ts).year))
        lots, ref = apply_ladder(equity, lots, ref)     # resize for the NEXT entry

    def do_open(ts, price, new_state):
        nonlocal state, entry_ts, entry_price
        state = new_state
        entry_ts, entry_price = pd.Timestamp(ts), float(price)
        opens.append(dict(direction="Long" if new_state == 1 else "Short",
                          entry_ts=pd.Timestamp(ts), entry_price=round(float(price), 2), lots=int(lots)))

    fresh = df[df.index > pd.Timestamp(after_ts)] if after_ts is not None else df
    for ts, r in fresh.iterrows():
        c, ef, es = r["close"], r["ema_fast"], r["ema_slow"]
        if math.isnan(ef) or math.isnan(es):
            eq_points.append((ts, equity, NAME[state], c)); continue
        if state == 0:
            if c > ef and c > es:
                do_open(ts, c, 1)
            elif c < ef and c < es:
                do_open(ts, c, -1)
        elif state == 1:
            if c < ef:
                if c < es:
                    do_close(ts, c, "SAR-Reverse"); do_open(ts, c, -1)
                else:
                    do_close(ts, c, "Trail-Flat"); state = 0
        elif state == -1:
            if c > ef:
                if c > es:
                    do_close(ts, c, "SAR-Reverse"); do_open(ts, c, 1)
                else:
                    do_close(ts, c, "Trail-Flat"); state = 0
        eq_points.append((ts, equity, NAME[state], c))

    return (trades, NAME[state], entry_ts, entry_price, lots, equity, net_cum, eq_points, opens, ref)


# ── DB helpers ───────────────────────────────────────────────────────────────
def read_book(sb):
    rows = sb.schema(SCHEMA).table("book").select("*").eq("id", 1).execute().data
    if rows:
        return rows[0]
    return dict(id=1, state="FLAT", entry_ts=None, entry_price=None, lots=START_LOTS,
                last_price=None, last_processed_ts=None, equity=START_CAPITAL,
                net_points_cum=0.0, ema_fast=None, ema_slow=None, ladder_ref=START_CAPITAL)


def write_book(sb, **fields):
    fields["id"] = 1
    fields["updated_at"] = datetime.now(IST).isoformat()
    sb.schema(SCHEMA).table("book").upsert(fields, on_conflict="id").execute()


def _iso(ts):
    return None if ts is None else pd.Timestamp(ts).isoformat()


def _naive(ts):
    """Normalise a (possibly tz-aware) DB timestamp to tz-naive IST-wall-clock,
    matching the tz-naive df.index. Values are written naive and stored as
    timestamptz (UTC), so dropping the tz on read recovers the same wall time."""
    if ts is None:
        return None
    t = pd.Timestamp(ts)
    return t.tz_convert(None) if t.tz is not None else t


# ── Telegram ─────────────────────────────────────────────────────────────────
def alert(trades_opened, trades_closed, book, close_px):
    if not (trades_opened or trades_closed):
        return
    try:
        from notify import send_telegram
    except Exception:
        return
    lines = ["📟 <b>SAR · NIFTY 75m</b>"]
    for t in trades_closed:
        emoji = "🔻" if t["exit_reason"] == "SAR-Reverse" else "🚪"
        lines.append(f"{emoji} <b>EXIT {t['direction']}</b> @ {t['exit_price']} "
                     f"({t['exit_reason']}) · {t['net_points']:+.1f} pts · "
                     f"₹{t['pnl_inr']:+,.0f} ({t['lots']}×{LOT_SIZE})")
    for t in trades_opened:
        arrow = "🟢" if t["direction"] == "Long" else "🔴"
        lines.append(f"{arrow} <b>ENTER {t['direction'].upper()}</b> @ {t['entry_price']} "
                     f"· {t['lots']} lot(s)")
    lines.append(f"\nPosition: <b>{book['state']}</b> · equity <b>₹{book['equity']:,.0f}</b> "
                 f"· EMA13 {book['ema_fast']:.0f} / EMA34 {book['ema_slow']:.0f} · close {close_px:.0f}")
    send_telegram("\n".join(lines))


# ── mode: seed ───────────────────────────────────────────────────────────────
def run_seed(sb, preview):
    if not SEED_CSV.exists():
        log.error("seed file missing: %s", SEED_CSV); return
    rows = list(csv.DictReader(SEED_CSV.open()))
    equity, net_cum, s_no = START_CAPITAL, 0.0, 0
    lots, ref = START_LOTS, START_CAPITAL          # banded-ratchet sizing state
    trades, eq_points = [], []
    for r in rows:
        s_no += 1
        net = float(r["net_points"])
        pnl = net * LOT_SIZE * lots                # trade held at its entry lots
        net_cum += net; equity += pnl
        trades.append(dict(
            s_no=s_no, symbol="NIFTY", entry_ts=_iso(r["entry_ts"]), direction=r["direction"],
            entry_price=round(float(r["entry_price"]), 2), exit_ts=_iso(r["exit_ts"]),
            exit_price=round(float(r["exit_price"]), 2), exit_reason=r["exit_reason"],
            gross_points=round(float(r["gross_points"]), 3),
            cost_points=round(float(r["cost_points"]), 3), net_points=round(net, 3),
            bars_held=int(r["bars_held"]), lots=lots, lot_size=LOT_SIZE,
            pnl_inr=round(pnl, 2), win_lose="Win" if net > 0 else "Lose",
            cum_net_points=round(net_cum, 3), equity_after=round(equity, 2),
            source="seed", year=int(r["year"])))
        eq_points.append(dict(as_of=_iso(r["exit_ts"]), equity=round(equity, 2),
                              net_points_cum=round(net_cum, 3), state="FLAT",
                              close=round(float(r["exit_price"]), 2), is_eod=True, source="seed"))
        lots, ref = apply_ladder(equity, lots, ref)    # resize for the next trade
    log.info("SEED: %d trades · final equity ₹%s · net %.1f pts · lots %d · ref ₹%s",
             len(trades), f"{equity:,.0f}", net_cum, lots, f"{ref:,.0f}")
    if preview:
        return
    # a re-seed rebuilds the whole forward test: clear ALL trades + equity points
    # (seed AND any live rows the previous book produced), then insert fresh.
    sb.schema(SCHEMA).table("trades").delete().in_("source", ["seed", "live"]).execute()
    sb.schema(SCHEMA).table("equity_curve").delete().in_("source", ["seed", "live"]).execute()
    for i in range(0, len(trades), 100):
        sb.schema(SCHEMA).table("trades").insert(trades[i:i + 100]).execute()
    sb.schema(SCHEMA).table("equity_curve").upsert(eq_points, on_conflict="as_of").execute()
    # hand off FLAT at the boundary; the live machine takes over after this bar,
    # continuing the banded-ratchet sizing from the seed's final lots/ref.
    write_book(sb, state="FLAT", entry_ts=None, entry_price=None, lots=lots, ladder_ref=round(ref, 2),
               last_processed_ts=BOUNDARY_TS.isoformat(), equity=round(equity, 2),
               net_points_cum=round(net_cum, 3), last_price=round(float(rows[-1]["exit_price"]), 2))
    sb.schema(SCHEMA).table("runs").insert(dict(
        mode="seed", last_bar_ts=BOUNDARY_TS.isoformat(), bars_processed=len(trades),
        state="FLAT", action=f"seeded {len(trades)} 2026 trades, equity ₹{equity:,.0f}")).execute()


# ── mode: live (eod / intraday) ──────────────────────────────────────────────
def run_live(sb, mode, preview):
    df = fetch_75min()
    latest = df.iloc[-1]
    log.info("75-min bars: %d · latest %s close %.1f EMA13 %.1f EMA34 %.1f",
             len(df), df.index[-1], latest["close"], latest["ema_fast"], latest["ema_slow"])

    book = read_book(sb)
    after = _naive(book["last_processed_ts"]) if book.get("last_processed_ts") else BOUNDARY_TS
    ladder_ref = float(book.get("ladder_ref") if book.get("ladder_ref") is not None else START_CAPITAL)
    (closed, state, entry_ts, entry_price, lots, equity, net_cum,
     eq_points, opened, ladder_ref) = advance(df, book.get("state", "FLAT"), _naive(book.get("entry_ts")),
                          float(book["entry_price"]) if book.get("entry_price") else None,
                          int(book.get("lots") or START_LOTS), float(book["equity"]),
                          float(book["net_points_cum"]), after, ladder_ref)

    close_px = float(latest["close"])
    dist = close_px - float(latest["ema_fast"])
    n_new = len(df[df.index > pd.Timestamp(after)])
    log.info("%s: %d new bar(s) · %d close(s) · state %s · equity ₹%s",
             mode.upper(), n_new, len(closed), state, f"{equity:,.0f}")
    for t in closed:
        log.info("  CLOSE %s %s@%.1f -> %.1f %s  %+.1f pts  Rs %+.0f",
                 t["direction"], t["entry_ts"], t["entry_price"], t["exit_price"],
                 t["exit_reason"], t["net_points"], t["pnl_inr"])

    if preview:
        return

    is_eod = (mode == "eod")
    # 1. candles cache (chart + stop line) — upsert the tail we care about
    tail = df.tail(400).reset_index()
    sb.schema(SCHEMA).table("candles").upsert([dict(
        ts=_iso(r["ts"]), open=round(float(r["open"]), 2), high=round(float(r["high"]), 2),
        low=round(float(r["low"]), 2), close=round(float(r["close"]), 2),
        ema_fast=round(float(r["ema_fast"]), 2), ema_slow=round(float(r["ema_slow"]), 2),
        source="future") for _, r in tail.iterrows()], on_conflict="ts").execute()

    # 2. realised trades
    if closed:
        sb.schema(SCHEMA).table("trades").insert(
            [{**t, "entry_ts": _iso(t["entry_ts"]), "exit_ts": _iso(t["exit_ts"])}
             for t in closed]).execute()

    # 3. equity curve (one row per processed bar)
    if eq_points:
        sb.schema(SCHEMA).table("equity_curve").upsert([dict(
            as_of=_iso(ts), equity=round(float(eq), 2), state=st, close=round(float(cx), 2),
            net_points_cum=round(net_cum, 3), is_eod=is_eod, source="live")
            for (ts, eq, st, cx) in eq_points], on_conflict="as_of").execute()

    # 4. book — last_processed_ts only ever moves forward
    last_ts = max(pd.Timestamp(df.index[-1]), pd.Timestamp(after))
    write_book(sb, state=state, entry_ts=_iso(entry_ts), entry_price=(round(float(entry_price), 2)
               if entry_price is not None else None), lots=int(lots), last_price=round(close_px, 2),
               last_processed_ts=_iso(last_ts), equity=round(float(equity), 2),
               net_points_cum=round(float(net_cum), 3), ladder_ref=round(float(ladder_ref), 2),
               ema_fast=round(float(latest["ema_fast"]), 2), ema_slow=round(float(latest["ema_slow"]), 2))

    # 5. heartbeat
    book_now = dict(state=state, equity=round(float(equity)), ema_fast=float(latest["ema_fast"]),
                    ema_slow=float(latest["ema_slow"]))
    sb.schema(SCHEMA).table("runs").insert(dict(
        mode=mode, last_bar_ts=_iso(last_ts), bars_processed=n_new, state=state,
        close=round(close_px, 2), ema_fast=round(float(latest["ema_fast"]), 2),
        ema_slow=round(float(latest["ema_slow"]), 2), dist_to_stop_pts=round(float(dist), 2),
        action=(f"{len(closed)} close(s), now {state}" if closed else f"no change, {state}"))).execute()

    # 6. Telegram on any transition
    alert(opened, closed, book_now, close_px)

    # 7. result file for the consolidated daily summary
    pathlib.Path(ROOT / "results").mkdir(exist_ok=True)
    (ROOT / "results" / "sar.json").write_text(json.dumps(dict(
        script="SAR (NIFTY 75m)", succeeded=1, failed=0, skipped=0, total=1,
        errors=[], state=state, equity=round(float(equity)), closes=len(closed))))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", action="store_true", help="one-time: replay the 2026 backtest trades")
    ap.add_argument("--eod", action="store_true", help="authoritative daily close (stamps is_eod)")
    ap.add_argument("--force", action="store_true", help="ignore the market-hours clock guard")
    ap.add_argument("--preview-only", action="store_true", help="compute + print; no DB writes/Telegram")
    args = ap.parse_args()

    if not (SUPABASE_URL and SUPABASE_KEY):
        log.error("Supabase env not configured (NEXT_PUBLIC_SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY).")
        sys.exit(1)
    sb = sb_client()

    if args.seed:
        log.info("=== SAR SEED ===")
        run_seed(sb, args.preview_only); return

    mode = "eod" if args.eod else "intraday"
    if mode == "intraday" and not args.force and not market_open():
        log.info("market closed — nothing to do (use --force to run anyway)."); return
    log.info("=== SAR %s — %s ===", mode.upper(), datetime.now(IST).strftime("%Y-%m-%d %H:%M"))
    run_live(sb, mode, args.preview_only)
    log.info("done.")


if __name__ == "__main__":
    main()
