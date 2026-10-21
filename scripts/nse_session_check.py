"""
nse_session_check.py — which NSE session does this daily_sync run belong to,
and was there one?

Several daily_sync steps stamped their rows with the wall clock instead of the
bar they read, which broke two ways (audit 2026-10-21):

  * Exchange holidays (14 Sep, 2 Oct, 20 Oct 2026): the run still fired and
    re-published the previous session under the holiday's date — duplicate RVOL
    confirmations, a copied Blue Sky candidate list and equity point, and a
    zero-return NAV row for every portfolio.
  * Late runs: GitHub delivers this job up to ~9 h late. Blue Sky stamped the
    IST date, so a run after 18:30 UTC landed on the NEXT day — 12 Oct 2026 has
    no Blue Sky row and 13 Oct was written twice.

This step reads NIFTY's latest daily bar and exports its date as
MARKET_SESSION_DATE (via $GITHUB_ENV) for the later steps to stamp. The run's
intended date is its UTC date: the cron fires at 10:53 UTC and even a 9-hour
delay stays on the same UTC day. If the latest bar is older than that, there
was no session today: trading_day=false is written to $GITHUB_OUTPUT, the
date-stamped steps are skipped, and their summary lines say why.

If TradingView cannot be reached it fails open (trading_day=true, nothing
exported), which is exactly the behaviour before this step existed.
"""
import json
import logging
import os
import pathlib
from datetime import datetime, timezone

from tvDatafeed import TvDatafeed, Interval

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("nse_session")

# Steps gated on trading_day in daily_sync.yml: result file -> summary label.
GATED_RESULTS = {
    "results/bluesky.json":           "Blue Sky Screener",
    "results/bluesky_portfolio.json": "Blue Sky Portfolio",
    "results/portfolio_nav.json":     "Portfolio NAV Snapshot",
    "results/rvol.json":              "Swing Professor RVOL Scan",
}


def latest_session_date():
    for attempt in range(1, 4):
        try:
            df = TvDatafeed().get_hist("NIFTY", "NSE", interval=Interval.in_daily, n_bars=5)
            if df is not None and not df.empty:
                # tvDatafeed stamps bars in the machine's zone; on a UTC runner the
                # 09:15 IST open is 03:45 UTC, still the same calendar date.
                return df.index[-1].date().isoformat()
        except Exception as exc:
            log.warning("attempt %d/3: %s", attempt, exc)
    return None


def _append(env_file, line):
    path = os.environ.get(env_file)
    if path:
        with open(path, "a") as f:
            f.write(line + "\n")


def main():
    intended = datetime.now(timezone.utc).date().isoformat()
    session = latest_session_date()

    if session is None:
        log.warning("Could not read NIFTY's latest bar -- assuming %s is a session (fail open).", intended)
        _append("GITHUB_OUTPUT", "trading_day=true")
        return

    _append("GITHUB_ENV", f"MARKET_SESSION_DATE={session}")
    _append("GITHUB_OUTPUT", f"session_date={session}")

    if session >= intended:
        log.info("NSE session %s -- date-stamped steps will use it.", session)
        _append("GITHUB_OUTPUT", "trading_day=true")
        return

    reason = f"no NSE session on {intended} (latest bar {session}) -- skipped"
    log.info("%s. Gated steps: %s", reason, ", ".join(GATED_RESULTS.values()))
    _append("GITHUB_OUTPUT", "trading_day=false")
    pathlib.Path("results").mkdir(exist_ok=True)
    for path, label in GATED_RESULTS.items():
        pathlib.Path(path).write_text(json.dumps({
            "script": label, "succeeded": 0, "failed": 0, "skipped": 1, "errors": [reason],
        }))


if __name__ == "__main__":
    main()
