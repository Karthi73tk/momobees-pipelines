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
│   ├── stage_analysis_pipeline_w.py    # Weekly — Weinstein stage analysis
│   └── rrg_pipeline_w.py              # Weekly — RRG pipeline
├── notify.py                           # Shared Telegram notification helper
├── requirements.txt
└── .github/
    └── workflows/
        ├── daily_sync.yml              # Mon–Fri 4:30 AM IST
        └── weekly_sync.yml             # Friday 5:00 PM IST
```

---

## Schedule

| Workflow | Schedule | Scripts |
|---|---|---|
| Daily Sync | Mon–Fri 4:30 AM IST | NSE All, N750, Market Pulse, Pivot |
| Weekly Sync | Friday 5:00 PM IST | Stage Analysis, RRG |

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
2. Click **Daily Sync (Mon–Fri 4:30 AM IST)**
3. Click **Run workflow** → **Run workflow**
4. Watch the logs and check your Telegram for notifications

### 6. Verify the schedule

GitHub Actions cron uses UTC. Your schedules are:

```
Daily:   cron: '0 23 * * 0-4'   # 23:00 UTC Sun–Thu = 4:30 AM IST Mon–Fri
Weekly:  cron: '30 11 * * 5'    # 11:30 UTC Friday  = 5:00 PM IST Friday
```

> **Note:** GitHub Actions cron can be delayed by up to 15 minutes during
> high-load periods. This is normal and not a configuration issue.

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
python scripts/stage_analysis_pipeline_w.py --preview-only
```

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
