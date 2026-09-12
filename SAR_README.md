# 13/34 EMA Stop-and-Reverse (NIFTY 75-min) — MoMoBees integration

Forward test of the `13_34_ema_stop_and_reverse` study (verdict: conditional, **NIFTY only**),
wired into MoMoBees the same way as Blue Sky: a pipeline writes a Supabase schema, the Next app
reads it. Always-in-market trend follower; long/short switches and exits alert on Telegram.

## What runs

| Piece | Path |
|---|---|
| Schema (run once) | [`sql/sar_001_schema.sql`](sql/sar_001_schema.sql) |
| Pipeline (seed / EOD / intraday) | [`scripts/sar_pipeline.py`](scripts/sar_pipeline.py) |
| 2026 seed trades | [`data/sar_seed_2026.csv`](data/sar_seed_2026.csv) |
| Intraday CI (75-min closes) | [`.github/workflows/sar_intraday.yml`](.github/workflows/sar_intraday.yml) |
| EOD CI | step in [`.github/workflows/daily_sync.yml`](.github/workflows/daily_sync.yml) |
| App page | `MoMoBees_js/src/app/dashboard/ema-sar/page.tsx` → nav **Swing → SAR** |

## One-time setup

1. **Create the schema.** In the Supabase SQL editor, run `sql/sar_001_schema.sql`.
2. **Expose it.** Supabase Dashboard → Project Settings → API → *Exposed schemas* → add **`sar`**
   (same requirement as `bluesky`; the app + pipeline talk to it over PostgREST).
3. **Seed the 2026 forward test** (replays the 90 backtested 2026 trades, ₹10L start, compounded):
   ```bash
   python scripts/sar_pipeline.py --seed
   ```
   Hands the book off **FLAT** at 2026-08-10 14:15 (the last backtest bar); the live futures machine
   takes over after that.
4. GitHub secrets already used by the other jobs cover this one (`NEXT_PUBLIC_SUPABASE_URL`,
   `SUPABASE_SERVICE_ROLE_KEY`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`).

## How it runs after setup

- **Every 75-min close** (`sar_pipeline.py`, no flags): fetches NIFTY continuous-future 15-min bars
  from tvDatafeed, reconstructs 75-min OHLC, recomputes EMA13/EMA34, folds any newly-closed bar into the
  book, records trades, and alerts Telegram on any long/short switch or exit. Idempotent — only bars
  newer than `book.last_processed_ts` are ever acted on, so extra/late firings are cheap no-ops.
- **EOD** (`--eod`, inside `daily_sync.yml`): the authoritative daily close; same logic, stamps
  `equity_curve.is_eod=true`. Also runnable via `python3 run_pipeline.py --only sar`.

## Rules (Variant A, verbatim)

- `EMA_fast = EMA(13, close)`, `EMA_slow = EMA(34, close)`; warm-up 34 bars.
- **FLAT** → LONG if close > both EMAs; → SHORT if close < both.
- **LONG** → exit when close < EMA13; if also < EMA34, reverse to SHORT the same bar (SAR); else flat.
- **SHORT** → mirror. Fill at the **close** of the signal bar. No independent stop-loss / no target — the
  13 EMA *is* the stop, and clipping the tail would kill the (tail-driven) edge.
- 75-min bars: 09:15 10:30 11:45 13:00 14:15 (no 15:30 bar — last decision is the 14:15 close).

## Book / money model

- ₹10,00,000 capital, ₹5,00,000 per lot, lot size **65**.
- `lots = floor(equity / ₹5L)`, recomputed at every entry (**compounded**).
- `₹P&L = net_points · 65 · lots`; `net_points = gross − cost`,
  `cost = 0.00013240·S + 1.45458` pts (S = mid of entry/exit) — the study's canonical `[Dec+Scaled@0.5]`
  option round-trip cost, refit to < 0.001 pt against the 2026 trade log.

## Caveats carried from the study

- **NIFTY only** — BANKNIFTY fails the combined stress; not shipped.
- Live signal is on the **continuous future** (per the integration request); the 2026 seed is on index
  spot. Points-based P&L is unaffected; the one handoff is clean because the book is flat at the boundary.
- Tail-driven: median trade is negative; long strings of small losers punctuated by rare large winners.
  Do not add a per-trade target or an extra stop without re-testing.
