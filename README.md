# MoMoBees — Market Data Sync Pipelines

Automated daily and weekly data sync pipelines for NSE stocks, running on GitHub Actions with Telegram notifications.

---

## Repository Structure

```
├── scripts/
│   ├── data_sync_engine_nse_all_d.py   # Daily — NSE universe sync
│   ├── data_sync_engine_n750_d.py      # Daily — N750 RS ratio sync
│   ├── market_pulse_global_d.py        # Daily — Global indices
│   ├── pivot_analysis_d.py             # Daily — Pivot point analysis
│   ├── portfolio_rebalance_d.py        # Daily — Portfolio rebalance
│   ├── portfolio_nav_snapshot_d.py     # Daily — Portfolio NAV snapshot
│   ├── sync_universe.py                # Daily — N750 tier/F&O universe sync
│   ├── swing_professor/                # Weekly + Daily — Swing Professor pipeline (own subfolder, see below)
│   └── rrg_pipeline_w.py              # Weekly — RRG pipeline
├── notify.py                           # Shared Telegram notification helper
├── requirements.txt
└── .github/
    └── workflows/
        ├── daily_sync.yml              # Mon–Fri 4:30 PM IST
        └── weekly_sync.yml             # Friday 5:00 PM IST
```

`scripts/swing_professor/` is a self-contained subfolder, kept separate from
the unrelated daily/weekly sync scripts above:

```
scripts/swing_professor/
├── stage_analysis_pipeline_w.py    # Weekly — Weinstein stage classifier (must run first, see below)
├── weekly_watchlist_pipeline.py    # Weekly — orchestrator (Components B-G), writes swing_professor.watchlist
├── daily_rvol_scanner.py           # Daily — RVOL confirmation scan, watchlist tickers only (not the full universe)
├── clean_base_lib.py               # Clean Base evaluation (depth bands, VCP, weekly 10WMA + daily 50DMA respect)
├── stage2_checklist.py             # Stage 2 Checklist pre-filter (10w/20w SMA slope, purple-dot count, etc.)
├── composite_scoring.py            # Fixed-denominator composite ranking
├── dot_scoring.py                  # Purple/red-dot leg scoring + raw dot counting
├── lifecycle.py                    # ADD/KEEP/REMOVE/PROMOTE decision table
├── supabase_watchlist_store.py     # Supabase-backed persistence (swing_professor.watchlist)
├── stage2_data_lib.py              # Shared Supabase/tvDatafeed fetch helpers
├── momentum_scanner_tvdatafeed.py  # Component B momentum scan
├── stage1_to_stage2_screen_v3.py   # Component C young-breakout screen
└── weekly_report_writer.py         # Weekly markdown report generator
```

`weekly_watchlist_pipeline.py` must run after `stage_analysis_pipeline_w.py`
each week — it only *reads* `stage.weekly_stock_stages`, never writes it, and
works off stale data otherwise (see `The_Professor/Docs/HANDOFF.md` §2).
`daily_rvol_scanner.py` runs independently, any day, scoped only to whatever
is currently `ACTIVE` on `swing_professor.watchlist` — not the full NSE
universe — writing hits to `swing_professor.daily_confirmations`.

---

## Schedule

| Workflow | Schedule | Scripts |
|---|---|---|
| Daily Sync | Mon–Fri 4:30 PM IST | NSE All, Universe Sync, N750, Market Pulse, Pivot, Momentum, Portfolio Rebalance, Portfolio NAV Snapshot, Swing Professor RVOL Scanner |
| Weekly Sync | Friday 5:00 PM IST | Stage Analysis, Swing Professor Watchlist, RRG |

---

## Step-by-Step Setup

### 1. Create the GitHub Repository

1. Go to [github.com/new](https://github.com/new)
2. Name it something like `momobees-pipelines`
3. Set visibility to **Public** (free Actions minutes)
4. Click **Create repository**

### 2. Clone and set up locally

```bash
git clone https://github.com/YOUR_USERNAME/momobees-pipelines.git
cd momobees-pipelines
```

Create the folder structure:

```bash
mkdir -p scripts .github/workflows
```

Copy all your scripts into `scripts/` and `notify.py` into the root.

### 3. Add all files and push

```bash
git add .
git commit -m "Initial pipeline setup"
git push origin main
```

### 4. Add GitHub Secrets

Go to your repo → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Add each of these:

| Secret Name | Value |
|---|---|
| `NEXT_PUBLIC_SUPABASE_URL` | Your Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Your Supabase service role key |
| `TELEGRAM_BOT_TOKEN` | Your BotFather token e.g. `123456:ABCdef...` |
| `TELEGRAM_CHAT_ID` | Your chat/channel ID e.g. `-1001234567890` |

> **How to find your Telegram Chat ID:**
> Send a message to your bot, then open:
> `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`
> Look for `"chat":{"id": ...}` in the response.

### 5. Test the workflows manually

1. Go to your repo → **Actions** tab
2. Click **Daily Sync (Mon–Fri 4:23 PM IST)**
3. Click **Run workflow** → **Run workflow**
4. Watch the logs and check your Telegram for notifications

### 6. Verify the schedule

GitHub Actions cron uses UTC. Your schedules are:

```
Daily:    cron: '53 10 * * 1-5'       # 10:53 UTC Mon–Fri = 4:23 PM IST
Weekly:   cron: '23 11 * * 5'         # 11:23 UTC Friday  = 4:53 PM IST
RRG:      cron: '38 11 * * 5'         # 11:38 UTC Friday  = 5:08 PM IST
Backstop: cron: '23 4,5,6,7,8,9 * * 1-5'  # 09:53–14:53 IST, inside the session
Blue Sky: cron: '13,43 4-9 * * 1-5'   # 09:43–15:13 IST, every 30 min
SAR:      5 entries at ~10:38, 11:53, 13:08, 14:23, 15:38 IST (75-min bar closes)
```

> **Note — the scheduler runs late, by hours.** Measured across every workflow
> in this repo from 2026-09-08 to 2026-09-18, GitHub's hosted scheduler created
> runs **3h40m–5h40m after** their nominal cron, and dropped most firings of the
> high-frequency ones (Blue Sky landed 2 of ~16 expected runs per day). This is a
> GitHub-side queue, not a configuration error — the repo is public, so it is not
> a billing or minutes problem.
>
> Two consequences to design around:
>
> 1. **Treat the cron as "no earlier than", never as "at".** Every cron here uses
>    an off-peak minute (never :00 or :30) because round slots are the most
>    congested, but that is a mitigation, not a fix.
> 2. **Keep the scripts' clock guards wide.** A guard that only passes during the
>    live session turns a late run into a no-op. `sar_pipeline.py` is idempotent
>    (it only ever folds in bars newer than `book.last_processed_ts`), so its
>    intraday guard deliberately spans the whole weekday from 09:15 IST onward and
>    a late run simply catches up. `bluesky_intraday_poller.py` reads *live* LTP,
>    so it cannot do that — a late run there is genuinely useless and still no-ops.
>
> If reliable intraday timing ever becomes a hard requirement, GitHub's hosted
> scheduler is the wrong tool; move those jobs to a self-hosted runner or an
> external scheduler that calls `workflow_dispatch`.

---

## Local Development

Create a `.env.local` file in the root (never commit this):

```bash
NEXT_PUBLIC_SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_SERVICE_ROLE_KEY=eyJ...
TELEGRAM_BOT_TOKEN=123456:ABCdef...
TELEGRAM_CHAT_ID=-1001234567890
BENCHMARK_TICKER=NIFTY
BENCHMARK_EXCHANGE=NSE
STOCK_EXCHANGE=NSE
LOOKBACK_PERIODS=150
TAIL_PERIODS=12
```

Run any script locally:

```bash
pip install -r requirements.txt
python scripts/data_sync_engine_nse_all_d.py --preview-only
python scripts/swing_professor/stage_analysis_pipeline_w.py --preview-only
python scripts/swing_professor/weekly_watchlist_pipeline.py --store-backend supabase --reports-dir reports
python scripts/swing_professor/daily_rvol_scanner.py --preview-only
```

Swing Professor's `.env.local` can also live at `scripts/.env.local` (already
gitignored) if you're running its scripts directly from within that
directory — `load_dotenv(".env.local")` resolves relative to the current
working directory, not the script's own location, so match your `cd`/invocation
path to wherever the file actually is.

Make sure `.env.local` is in `.gitignore`:

```bash
echo ".env.local" >> .gitignore
```

---

## Telegram Notifications

Each script sends a notification on completion. The final step of each workflow sends a consolidated summary:

**Daily summary example:**
```
🗓 Daily Sync Summary

✅ NSE All Sync: success
✅ N750 Sync: success
✅ Market Pulse: success
❌ Pivot Analysis: failure
```

**Weekly summary example:**
```
📅 Weekly Sync Summary

✅ Stage Analysis: success
✅ RRG Pipeline: success
```
