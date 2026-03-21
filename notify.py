"""
notify.py
---------
Shared Telegram notification helper.
All pipeline scripts import send_telegram() from here.

Environment variables required:
    TELEGRAM_BOT_TOKEN  - BotFather token  (e.g. 123456:ABCdef...)
    TELEGRAM_CHAT_ID    - Chat/channel ID  (e.g. -1001234567890)

If either variable is missing, send_telegram() logs a warning and returns
silently so scripts still complete even if notifications are misconfigured.
"""

import os
import logging
import traceback
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

# Read once at import time — works both from .env.local (local) and
# GitHub Actions secrets (CI).
_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")


def send_telegram(
    message: str,
    parse_mode: str = "HTML",
    disable_preview: bool = True,
) -> bool:
    """
    Send a Telegram message via the Bot API.

    Returns True on success, False on failure.
    Never raises — safe to call from finally blocks.
    """
    if not _BOT_TOKEN or not _CHAT_ID:
        log.warning(
            "Telegram not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing). "
            "Skipping notification."
        )
        return False

    try:
        import urllib.request
        import urllib.parse
        import json

        url     = f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage"
        payload = json.dumps({
            "chat_id":                  _CHAT_ID,
            "text":                     message,
            "parse_mode":               parse_mode,
            "disable_web_page_preview": disable_preview,
        }).encode("utf-8")

        req = urllib.request.Request(
            url,
            data    = payload,
            headers = {"Content-Type": "application/json"},
            method  = "POST",
        )

        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
            if body.get("ok"):
                log.info("Telegram notification sent.")
                return True
            else:
                log.warning("Telegram API returned ok=false: %s", body)
                return False

    except Exception as exc:
        log.error("Failed to send Telegram notification: %s: %s", type(exc).__name__, exc)
        log.debug(traceback.format_exc())
        return False


# ── Convenience builders ──────────────────────────────────────────────────────

def _ts() -> str:
    """Current UTC time as a readable string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def notify_success(script: str, details: str = "") -> bool:
    """Send a green success notification."""
    msg = (
        f"✅ <b>{script}</b>\n"
        f"<i>{_ts()}</i>\n"
    )
    if details:
        msg += f"\n{details}"
    return send_telegram(msg)


def notify_failure(script: str, error: str = "") -> bool:
    """Send a red failure notification."""
    msg = (
        f"❌ <b>{script} FAILED</b>\n"
        f"<i>{_ts()}</i>\n"
    )
    if error:
        # Truncate long errors so Telegram doesn't reject the message
        msg += f"\n<pre>{error[:800]}</pre>"
    return send_telegram(msg)


def notify_summary(script: str, lines: list) -> bool:
    """
    Send a structured summary notification.

    lines: list of (label, value) tuples, e.g.:
        [("Succeeded", 1800), ("Skipped", 0), ("Failed", 3)]
    """
    body = "\n".join(f"  {label}: <b>{value}</b>" for label, value in lines)
    msg  = (
        f"📊 <b>{script}</b>\n"
        f"<i>{_ts()}</i>\n\n"
        f"{body}"
    )
    return send_telegram(msg)
