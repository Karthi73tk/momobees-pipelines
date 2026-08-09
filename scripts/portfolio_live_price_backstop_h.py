#!/usr/bin/env python3
"""
Hourly live-price backstop (FEAT-03).

Refreshes holdings.current_price for REAL, Zerodha-linked portfolios only, so
that anyone NOT actively looking at the live-polling portfolio page (digests,
emails, a fresh page load) sees prices at most ~1 hour stale during market
hours instead of up to a full day.

Scope & invariants (mirrors the Next live-price API route):
  * broker = 'ZERODHA' AND status = 'real' portfolios only. Virtual and ZEBU
    portfolios are untouched — they keep today's EOD-only pricing.
  * Kite LTP is scoped to ONE user's authenticated session, so tickers are
    deduped WITHIN each user's real portfolios (not globally like the EOD
    TVDatafeed job, which prices off a shared feed).
  * A user whose daily Kite token is missing/expired is SKIPPED and logged —
    never fails the whole run.
  * This job ONLY runs `UPDATE public.holdings SET current_price = ...`. It does
    NOT touch nav_history, does NOT call any RPC, and does NOT compute TWR. The
    4:30 PM IST EOD job (portfolio_nav_snapshot_d.py) remains the sole author of
    nav_history and the authoritative end-of-day current_price. The hourly
    window (10:00–15:00 IST) closes before the EOD job runs, so the two never
    race and the EOD job always wins for the day.

Env: NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, ENCRYPTION_KEY
     (same base64 key the Next app uses), plus optional TELEGRAM_* for alerts.
"""
import os
import sys
import json
import time
import pathlib
import logging
import argparse
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from supabase import create_client
from dotenv import load_dotenv

try:
    load_dotenv(".env.local")
except Exception:
    pass

from kite_crypto import decrypt

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("live_price_backstop")

SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

STOCK_EXCHANGE = os.environ.get("STOCK_EXCHANGE", "NSE")
KITE_LTP_URL = "https://api.kite.trade/quote/ltp"
# Kite's quote/ltp endpoint accepts up to 500 instruments per call; stay well
# under that (and under URL length limits) with a conservative chunk size.
LTP_CHUNK = 200


class KiteAuthError(Exception):
    """Raised when Kite rejects the token (HTTP 403 / TokenException) — the user
    must re-login. We skip that user rather than aborting the run."""


def to_instrument(ticker: str) -> str:
    """'NSE:RELIANCE' stays as-is; a bare 'RELIANCE' defaults to NSE."""
    return ticker if ":" in ticker else f"{STOCK_EXCHANGE}:{ticker}"


def fetch_kite_ltp(api_key: str, access_token: str, instruments: list[str]) -> dict[str, float]:
    """Batch LTP for one user's authenticated Kite session. Returns
    {instrument: last_price}. Raises KiteAuthError on token rejection."""
    prices: dict[str, float] = {}
    headers = {
        "X-Kite-Version": "3",
        "Authorization": f"token {api_key}:{access_token}",
    }

    for start in range(0, len(instruments), LTP_CHUNK):
        chunk = instruments[start:start + LTP_CHUNK]
        query = urllib.parse.urlencode([("i", inst) for inst in chunk])
        req = urllib.request.Request(f"{KITE_LTP_URL}?{query}", headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            if exc.code == 403:
                raise KiteAuthError("Kite token rejected (403).") from exc
            detail = ""
            try:
                detail = exc.read().decode("utf-8")[:200]
            except Exception:
                pass
            raise RuntimeError(f"Kite LTP HTTP {exc.code}: {detail}") from exc

        for inst, entry in (body.get("data") or {}).items():
            lp = entry.get("last_price")
            if lp is not None:
                prices[inst] = float(lp)

        # Gentle pacing between chunks (well within Kite's per-key rate limit).
        if start + LTP_CHUNK < len(instruments):
            time.sleep(0.4)

    return prices


def run(dry_run: bool = False) -> dict:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.")
    # Fail fast if the key is misconfigured, before we hit the DB.
    _ = os.environ.get("ENCRYPTION_KEY") or _missing_key()

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # 1. Real, Zerodha-linked portfolios only.
    portfolios = (
        sb.table("portfolios")
        .select("id, user_id")
        .eq("broker", "ZERODHA")
        .eq("status", "real")
        .execute()
        .data
    )
    if not portfolios:
        log.info("No real Zerodha portfolios to price.")
        return {"succeeded": 0, "failed": 0, "skipped": 0, "total": 0,
                "holdings_updated": 0, "errors": []}

    portfolio_ids = [p["id"] for p in portfolios]
    portfolios_by_user: dict[str, list[str]] = {}
    for p in portfolios:
        portfolios_by_user.setdefault(p["user_id"], []).append(p["id"])

    # 2. Open holdings across those portfolios.
    holdings = (
        sb.table("holdings")
        .select("id, portfolio_id, ticker")
        .in_("portfolio_id", portfolio_ids)
        .eq("status", "open")
        .execute()
        .data
    )
    holdings_by_portfolio: dict[str, list[dict]] = {}
    for h in holdings:
        holdings_by_portfolio.setdefault(h["portfolio_id"], []).append(h)

    now = datetime.now(timezone.utc)
    succeeded = 0
    skipped = 0
    failed = 0
    holdings_updated = 0
    errors: list[str] = []

    for user_id, pids in portfolios_by_user.items():
        user_holdings = [h for pid in pids for h in holdings_by_portfolio.get(pid, [])]
        if not user_holdings:
            continue

        # 3. Resolve this user's Kite credentials (same columns the Next app uses).
        prof = (
            sb.table("kite_profiles")
            .select("kite_api_key, kite_access_token_enc, access_token_expires_at")
            .eq("user_id", user_id)
            .maybe_single()
            .execute()
            .data
        )
        if not prof or not prof.get("kite_api_key"):
            skipped += 1
            errors.append(f"{user_id}: no kite_profile — skipped")
            log.warning("[user %s] No kite_profile / api_key. Skipped.", user_id)
            continue

        expires_at = prof.get("access_token_expires_at")
        token_enc = prof.get("kite_access_token_enc")
        is_expired = (
            not token_enc
            or not expires_at
            or datetime.fromisoformat(expires_at.replace("Z", "+00:00")) <= now
        )
        if is_expired:
            skipped += 1
            errors.append(f"{user_id}: token missing/expired — skipped")
            log.warning("[user %s] Kite token missing/expired. Skipped.", user_id)
            continue

        try:
            access_token = decrypt(token_enc)
        except Exception as exc:
            skipped += 1
            errors.append(f"{user_id}: token decrypt failed — skipped")
            log.warning("[user %s] Access token decrypt failed (%s). Skipped.", user_id, exc)
            continue

        # 4. Dedupe tickers WITHIN this user's real portfolios.
        tickers = sorted({h["ticker"] for h in user_holdings})
        inst_to_ticker = {to_instrument(t): t for t in tickers}
        instruments = list(inst_to_ticker.keys())

        try:
            ltp = fetch_kite_ltp(prof["kite_api_key"], access_token, instruments)
        except KiteAuthError:
            skipped += 1
            errors.append(f"{user_id}: Kite rejected token (403) — skipped")
            log.warning("[user %s] Kite rejected token (403). Skipped.", user_id)
            continue
        except Exception as exc:
            failed += 1
            errors.append(f"{user_id}: LTP fetch failed: {exc}")
            log.exception("[user %s] LTP fetch failed.", user_id)
            continue

        ticker_price = {
            inst_to_ticker[inst]: price
            for inst, price in ltp.items()
            if inst in inst_to_ticker
        }

        # 5. UPDATE current_price only — nothing else, no nav_history, no RPC.
        updated_this_user = 0
        for h in user_holdings:
            price = ticker_price.get(h["ticker"])
            if price is None:
                continue
            if dry_run:
                updated_this_user += 1
                continue
            res = sb.table("holdings").update({"current_price": price}).eq("id", h["id"]).execute()
            if res.data:
                updated_this_user += 1

        holdings_updated += updated_this_user
        succeeded += 1
        log.info(
            "[user %s] %s %d/%d holdings across %d portfolio(s).",
            user_id,
            "Would update" if dry_run else "Updated",
            updated_this_user,
            len(user_holdings),
            len(pids),
        )

    return {
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
        "total": len(portfolios_by_user),
        "holdings_updated": holdings_updated,
        "errors": errors,
    }


def _missing_key():
    raise RuntimeError("Missing ENCRYPTION_KEY environment variable.")


def _write_result(res: dict) -> None:
    pathlib.Path("results").mkdir(exist_ok=True)
    pathlib.Path("results/portfolio_live_price_backstop.json").write_text(json.dumps({
        "script":           "Portfolio Live-Price Backstop (hourly)",
        "succeeded":        res["succeeded"],
        "failed":           res["failed"],
        "skipped":          res["skipped"],
        "total":            res["total"],
        "holdings_updated": res.get("holdings_updated", 0),
        "errors":           res["errors"],
    }))


def _maybe_notify(res: dict, dry_run: bool) -> None:
    """Only ping Telegram when there's something actionable (skips/failures) so
    the hourly cadence doesn't spam the channel on clean runs."""
    if dry_run:
        return
    if res["skipped"] == 0 and res["failed"] == 0:
        return
    try:
        from notify import notify_summary
        lines = [
            ("Users priced", res["succeeded"]),
            ("Holdings updated", res.get("holdings_updated", 0)),
            ("Users skipped", res["skipped"]),
            ("Failed", res["failed"]),
        ]
        notify_summary("Live-Price Backstop (hourly)", lines)
    except Exception as exc:
        log.warning("Telegram notify failed: %s", exc)


def main() -> int:
    parser = argparse.ArgumentParser(description="Hourly live-price backstop for real Zerodha portfolios")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch LTP and report what WOULD change, but write nothing to holdings.",
    )
    args = parser.parse_args()

    try:
        res = run(dry_run=args.dry_run)
        _write_result(res)
        _maybe_notify(res, args.dry_run)
        log.info(
            "Done (%s). users_priced=%d holdings_updated=%d skipped=%d failed=%d",
            "DRY-RUN" if args.dry_run else "live",
            res["succeeded"], res.get("holdings_updated", 0), res["skipped"], res["failed"],
        )
        return 1 if res["failed"] > 0 else 0
    except Exception as exc:
        log.exception("Live-price backstop run failed.")
        _write_result({"succeeded": 0, "failed": 1, "skipped": 0, "total": 0,
                       "holdings_updated": 0, "errors": [str(exc)]})
        try:
            from notify import notify_failure
            notify_failure("Live-Price Backstop (hourly)", str(exc))
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    sys.exit(main())
