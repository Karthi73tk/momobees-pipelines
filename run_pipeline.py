#!/usr/bin/env python3
"""
run_pipeline.py
----------------
Local orchestrator for the MoMoBees daily + weekly pipeline scripts.

Purpose: when GitHub Actions is down/broken, run everything locally in one
shot, from the repo root. Sets up a venv, installs requirements, runs every
script in the correct order, and if one fails, remembers exactly where it
stopped so the *next* run picks up from that script instead of the top.

Usage:
    python3 run_pipeline.py                 # run daily + weekly (default)
    python3 run_pipeline.py --daily         # daily only
    python3 run_pipeline.py --weekly        # weekly only
    python3 run_pipeline.py --reset         # ignore existing progress, start fresh
    python3 run_pipeline.py --only n750     # run a single script by id
    python3 run_pipeline.py --status        # show cache state, run nothing
    python3 run_pipeline.py --list          # list all script ids + order
    python3 run_pipeline.py --continue-on-error   # don't stop the batch on failure

Run this from anywhere; it locates the repo root from its own file location.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# --------------------------------------------------------------------------
# Repo layout
# --------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent
VENV_DIR = REPO_ROOT / "venv"
REQUIREMENTS_FILE = REPO_ROOT / "requirements.txt"
CACHE_FILE = REPO_ROOT / ".pipeline_cache.json"
RESULTS_DIR = REPO_ROOT / "results"
REPORTS_DIR = REPO_ROOT / "reports"

IS_WINDOWS = platform.system() == "Windows"
VENV_PYTHON = VENV_DIR / ("Scripts/python.exe" if IS_WINDOWS else "bin/python")

# Keep this many historical durations per script for the ETA average.
HISTORY_LEN = 5

# --------------------------------------------------------------------------
# Pipeline definition — mirrors .github/workflows/daily_sync.yml,
# weekly_sync.yml and rrg_sync.yml (in that dependency order). Update this
# list if the workflows change.
# --------------------------------------------------------------------------

DAILY_SCRIPTS = [
    {"id": "nse_all",                 "path": "scripts/data_sync_engine_nse_all_d.py"},
    {"id": "universe",                "path": "scripts/sync_universe.py"},
    {"id": "n750",                    "path": "scripts/data_sync_engine_n750_d.py"},
    {"id": "indices",                 "path": "scripts/data_sync_engine_indices.py"},
    {"id": "index_monthly_returns",   "path": "scripts/sync_index_monthly_returns.py"},
    {"id": "market_pulse",            "path": "scripts/market_pulse_global_d.py"},
    {"id": "pivot",                   "path": "scripts/pivot_analysis_d.py"},
    {"id": "momentum_scanner",        "path": "scripts/momentum_scanner.py"},
    {"id": "momentum_weekly_scanner", "path": "scripts/momentum_weekly_scanner.py"},
    {"id": "portfolio_rebalance",     "path": "scripts/portfolio_rebalance_d.py"},
    {"id": "portfolio_nav_snapshot",  "path": "scripts/portfolio_nav_snapshot_d.py"},
    {"id": "model_portfolio_runner",  "path": "scripts/model_portfolio_runner_d.py"},
    {"id": "daily_rvol_scanner",      "path": "scripts/swing_professor/daily_rvol_scanner.py"},
]

WEEKLY_SCRIPTS = [
    {"id": "stage_analysis",   "path": "scripts/swing_professor/stage_analysis_pipeline_w.py"},
    {"id": "weekly_watchlist", "path": "scripts/swing_professor/weekly_watchlist_pipeline.py",
     "args": ["--store-backend", "supabase", "--reports-dir", "reports"]},
    {"id": "rrg_pipeline",     "path": "scripts/rrg_pipeline_w.py"},
]

for _s in DAILY_SCRIPTS:
    _s["group"] = "daily"
    _s.setdefault("args", [])
    _s["key"] = f"daily/{_s['id']}"
for _s in WEEKLY_SCRIPTS:
    _s["group"] = "weekly"
    _s.setdefault("args", [])
    _s["key"] = f"weekly/{_s['id']}"

ALL_SCRIPTS = DAILY_SCRIPTS + WEEKLY_SCRIPTS
SCRIPTS_BY_KEY = {s["key"]: s for s in ALL_SCRIPTS}
SCRIPTS_BY_ID = {s["id"]: s for s in ALL_SCRIPTS}  # ids are unique across both groups

STATUS_PENDING = "PENDING"
STATUS_RUNNING = "RUNNING"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"

ICONS = {
    STATUS_PENDING: "⏳",
    STATUS_RUNNING: "🏃",
    STATUS_SUCCESS: "✅",
    STATUS_FAILED: "❌",
}


# --------------------------------------------------------------------------
# Small utilities
# --------------------------------------------------------------------------

def hr(char="-", width=70):
    print(char * width)


def fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "?"
    seconds = int(round(seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def requirements_hash() -> str:
    if not REQUIREMENTS_FILE.exists():
        return ""
    return hashlib.sha256(REQUIREMENTS_FILE.read_bytes()).hexdigest()


# --------------------------------------------------------------------------
# Cache
# --------------------------------------------------------------------------

def default_cache() -> dict:
    return {
        "requirements_hash": "",
        "history": {},       # id -> [durations...] across all runs, for ETA
        "current_run": None, # dict, see new_run()
    }


def load_cache() -> dict:
    if not CACHE_FILE.exists():
        return default_cache()
    try:
        data = json.loads(CACHE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        print(f"⚠️  Could not read {CACHE_FILE.name}, starting with a fresh cache.")
        return default_cache()
    data.setdefault("requirements_hash", "")
    data.setdefault("history", {})
    data.setdefault("current_run", None)
    return data


def save_cache(cache: dict) -> None:
    CACHE_FILE.write_text(json.dumps(cache, indent=2, default=str))


def new_run(cache: dict, keys: list[str], scope_label: str) -> dict:
    run = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "scope": scope_label,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "keys": keys,
        "scripts": {
            k: {"status": STATUS_PENDING, "duration": None, "error": None, "finished_at": None}
            for k in keys
        },
    }
    cache["current_run"] = run
    return run


def record_history(cache: dict, key: str, duration: float) -> None:
    hist = cache["history"].setdefault(key, [])
    hist.append(round(duration, 1))
    cache["history"][key] = hist[-HISTORY_LEN:]


def eta_for(cache: dict, key: str) -> float | None:
    hist = cache["history"].get(key)
    if not hist:
        return None
    return sum(hist) / len(hist)


# --------------------------------------------------------------------------
# venv + dependencies
# --------------------------------------------------------------------------

def ensure_venv() -> None:
    if VENV_PYTHON.exists():
        return
    print(f"📦 No venv found at {VENV_DIR} — creating one...")
    subprocess.run([sys.executable, "-m", "venv", str(VENV_DIR)], check=True)
    print("   venv created.")


def ensure_dependencies(cache: dict, force: bool = False) -> None:
    current_hash = requirements_hash()
    if not force and current_hash and cache.get("requirements_hash") == current_hash:
        print("📦 requirements.txt unchanged since last install — skipping pip install.")
        print("   (use --reinstall to force)")
        return
    if not REQUIREMENTS_FILE.exists():
        print(f"⚠️  {REQUIREMENTS_FILE} not found — skipping dependency install.")
        return
    print("📦 Installing dependencies from requirements.txt ...")
    subprocess.run(
        [str(VENV_PYTHON), "-m", "pip", "install", "--upgrade", "pip", "--quiet"],
        check=True,
    )
    subprocess.run(
        [str(VENV_PYTHON), "-m", "pip", "install", "-r", str(REQUIREMENTS_FILE)],
        check=True,
    )
    cache["requirements_hash"] = current_hash
    save_cache(cache)
    print("   dependencies installed.\n")


# --------------------------------------------------------------------------
# Execution
# --------------------------------------------------------------------------

def run_script(spec: dict) -> tuple[bool, float, str | None]:
    """Run one script with the venv python, streaming its output live.
    Returns (success, duration_seconds, error_message_or_None)."""
    script_path = REPO_ROOT / spec["path"]
    if not script_path.exists():
        return False, 0.0, f"script not found: {script_path}"

    cmd = [str(VENV_PYTHON), str(script_path)] + spec["args"]
    start = time.time()
    try:
        # Inherit stdout/stderr so the script's own progress/logging shows
        # live in the terminal. cwd=REPO_ROOT is required: every script does
        # load_dotenv(".env.local") relative to the current working dir.
        result = subprocess.run(cmd, cwd=REPO_ROOT, env=os.environ.copy())
    except KeyboardInterrupt:
        duration = time.time() - start
        raise
    duration = time.time() - start

    if result.returncode != 0:
        return False, duration, f"exit code {result.returncode}"
    return True, duration, None


def print_progress_header(idx: int, total: int, spec: dict, cache: dict) -> None:
    eta = eta_for(cache, spec["key"])
    eta_str = f"~{fmt_duration(eta)}" if eta else "no history yet"
    hr()
    print(f"[{idx}/{total}] {spec['group'].upper():6s} {spec['id']}  (typical: {eta_str})")
    print(f"        {spec['path']} {' '.join(spec['args'])}".rstrip())
    hr()


def remaining_eta(cache: dict, keys: list[str], run: dict) -> float:
    total = 0.0
    for k in keys:
        if run["scripts"][k]["status"] == STATUS_SUCCESS:
            continue
        e = eta_for(cache, k)
        if e:
            total += e
    return total


def execute(cache: dict, run: dict, keys: list[str], continue_on_error: bool) -> bool:
    total = len(keys)
    overall_start = time.time()
    all_ok = True

    for idx, key in enumerate(keys, start=1):
        spec = SCRIPTS_BY_KEY[key]
        entry = run["scripts"][key]

        if entry["status"] == STATUS_SUCCESS:
            print(f"[{idx}/{total}] {ICONS[STATUS_SUCCESS]} {spec['id']} — already succeeded this run, skipping.")
            continue

        print_progress_header(idx, total, spec, cache)
        remaining = remaining_eta(cache, keys, run)
        elapsed = time.time() - overall_start
        print(f"⏱  elapsed so far: {fmt_duration(elapsed)}   |   est. remaining: ~{fmt_duration(remaining)}\n")

        entry["status"] = STATUS_RUNNING
        save_cache(cache)

        try:
            ok, duration, err = run_script(spec)
        except KeyboardInterrupt:
            entry["status"] = STATUS_PENDING  # so it re-runs, not stuck as RUNNING
            save_cache(cache)
            print("\n\n🛑 Interrupted by user. Progress saved — re-run to resume from this script.")
            sys.exit(130)

        entry["duration"] = round(duration, 1)
        entry["finished_at"] = datetime.now().isoformat(timespec="seconds")

        if ok:
            entry["status"] = STATUS_SUCCESS
            entry["error"] = None
            record_history(cache, key, duration)
            print(f"\n{ICONS[STATUS_SUCCESS]} {spec['id']} succeeded in {fmt_duration(duration)}.")
        else:
            entry["status"] = STATUS_FAILED
            entry["error"] = err
            all_ok = False
            print(f"\n{ICONS[STATUS_FAILED]} {spec['id']} FAILED after {fmt_duration(duration)}: {err}")

        save_cache(cache)

        if not ok and not continue_on_error:
            hr("=")
            print(f"Stopped at '{spec['id']}' ({spec['group']}).")
            print(f"Fix the issue, then re-run:  python3 run_pipeline.py")
            print(f"It will resume from '{spec['id']}' — everything before it is cached as done.")
            hr("=")
            return False

    total_elapsed = time.time() - overall_start
    hr("=")
    if all_ok:
        print(f"🎉 All {total} script(s) completed successfully in {fmt_duration(total_elapsed)}.")
    else:
        print(f"⚠️  Finished with failures (continue-on-error mode). Took {fmt_duration(total_elapsed)}.")
    hr("=")
    return all_ok


# --------------------------------------------------------------------------
# Status / listing
# --------------------------------------------------------------------------

def print_list() -> None:
    print("Daily scripts (in run order):")
    for i, s in enumerate(DAILY_SCRIPTS, 1):
        args = " " + " ".join(s["args"]) if s["args"] else ""
        print(f"  {i:2d}. {s['id']:26s} {s['path']}{args}")
    print("\nWeekly scripts (in run order):")
    for i, s in enumerate(WEEKLY_SCRIPTS, 1):
        args = " " + " ".join(s["args"]) if s["args"] else ""
        print(f"  {i:2d}. {s['id']:26s} {s['path']}{args}")


def print_status(cache: dict) -> None:
    run = cache.get("current_run")
    if not run:
        print("No run recorded yet. Run `python3 run_pipeline.py` to start.")
        return
    print(f"Last run: {run['date']}  scope={run['scope']}  started={run['created_at']}")
    hr()
    for key in run["keys"]:
        spec = SCRIPTS_BY_KEY[key]
        entry = run["scripts"][key]
        icon = ICONS.get(entry["status"], "?")
        dur = fmt_duration(entry["duration"]) if entry["duration"] else "-"
        err = f"  ({entry['error']})" if entry.get("error") else ""
        print(f"  {icon} {spec['group']:6s} {spec['id']:26s} {entry['status']:8s} {dur:>8s}{err}")
    hr()
    n_success = sum(1 for e in run["scripts"].values() if e["status"] == STATUS_SUCCESS)
    n_failed = sum(1 for e in run["scripts"].values() if e["status"] == STATUS_FAILED)
    print(f"{n_success}/{len(run['keys'])} succeeded, {n_failed} failed.")


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def build_key_list(args) -> tuple[list[str], str]:
    if args.only:
        spec = SCRIPTS_BY_ID.get(args.only)
        if not spec:
            valid = ", ".join(sorted(SCRIPTS_BY_ID))
            print(f"Unknown script id '{args.only}'. Valid ids:\n  {valid}")
            sys.exit(2)
        return [spec["key"]], f"only:{args.only}"

    if args.daily and args.weekly:
        return [s["key"] for s in ALL_SCRIPTS], "all"
    if args.daily:
        return [s["key"] for s in DAILY_SCRIPTS], "daily"
    if args.weekly:
        return [s["key"] for s in WEEKLY_SCRIPTS], "weekly"
    return [s["key"] for s in ALL_SCRIPTS], "all"


def main():
    parser = argparse.ArgumentParser(description="Run MoMoBees pipeline scripts locally, with resume-on-failure.")
    parser.add_argument("--daily", action="store_true", help="run only the daily scripts")
    parser.add_argument("--weekly", action="store_true", help="run only the weekly scripts")
    parser.add_argument("--only", type=str, default=None, help="run a single script by id (see --list)")
    parser.add_argument("--reset", "--force-fresh", dest="reset", action="store_true",
                         help="ignore any saved progress and start this scope from the beginning")
    parser.add_argument("--reinstall", action="store_true", help="force reinstall of requirements.txt")
    parser.add_argument("--skip-install", action="store_true", help="skip venv/dependency setup entirely")
    parser.add_argument("--continue-on-error", action="store_true",
                         help="keep going after a script fails, instead of stopping the batch")
    parser.add_argument("--list", action="store_true", help="list all scripts and exit")
    parser.add_argument("--status", action="store_true", help="show cache/progress and exit")
    args = parser.parse_args()

    if args.list:
        print_list()
        return

    cache = load_cache()

    if args.status:
        print_status(cache)
        return

    keys, scope_label = build_key_list(args)

    if not args.skip_install:
        ensure_venv()
        ensure_dependencies(cache, force=args.reinstall)
    else:
        if not VENV_PYTHON.exists():
            print(f"❌ --skip-install given but no venv found at {VENV_DIR}. Run once without --skip-install first.")
            sys.exit(1)

    RESULTS_DIR.mkdir(exist_ok=True)
    REPORTS_DIR.mkdir(exist_ok=True)

    run = cache.get("current_run")
    resuming = False

    if run and not args.reset and run.get("keys") == keys:
        # Same scope as last time and not forced fresh -> resume.
        pending_left = any(run["scripts"][k]["status"] != STATUS_SUCCESS for k in keys)
        if pending_left:
            resuming = True
            if run["date"] != datetime.now().strftime("%Y-%m-%d"):
                print(f"⚠️  Cached run is from {run['date']}, resuming it anyway "
                      f"(pass --reset to discard it and start fresh for today).\n")
            else:
                print(f"↻ Resuming previous run from {run['created_at']} ({scope_label}).\n")
        else:
            print(f"✅ Last run ({run['date']}, {scope_label}) already completed all scripts.")
            print("   Pass --reset to run it again from scratch, or --only <id> to re-run one script.\n")
            run = new_run(cache, keys, scope_label)
    else:
        if run and run.get("keys") != keys and not args.reset:
            print(f"ℹ️  Requested scope ({scope_label}) differs from last cached scope "
                  f"({run.get('scope')}) — starting a new run for this scope.\n")
        run = new_run(cache, keys, scope_label)

    save_cache(cache)

    ok = execute(cache, run, keys, continue_on_error=args.continue_on_error)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
