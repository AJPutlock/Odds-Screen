"""
Send a bet to the user's phone over Telegram, with a button that opens the
sportsbook's bet slip (the screen's Odds API bet-slip link; on a phone the
book's app should open if it supports the link).

Setup (the user does this; never handled here):
  1. Telegram app on the phone -> message @BotFather -> /newbot -> copy the token.
  2. Send the new bot any message from the phone.
  3. .env: TELEGRAM_BOT_TOKEN=<token>. GET /api/phone/status then lists the
     chat id(s) that messaged the bot; put yours in TELEGRAM_CHAT_ID.
.env is re-read on every call, so no restart is needed after editing it.
The same two settings switch on history_tracker's off-market alerts (those are
read at startup, so they need a restart).
"""
import os

import requests

try:
    from dotenv import load_dotenv
except ImportError:          # history_tracker reports the missing package
    load_dotenv = None

API = "https://api.telegram.org/bot{token}/{method}"


def _settings():
    if load_dotenv:
        load_dotenv(override=True)
    return os.environ.get("TELEGRAM_BOT_TOKEN", "").strip(), os.environ.get("TELEGRAM_CHAT_ID", "").strip()


def status() -> dict:
    """What's configured, plus the chat ids that have messaged the bot (to fill
    TELEGRAM_CHAT_ID). Never returns the token."""
    token, chat = _settings()
    out = {"token_set": bool(token), "chat_id_set": bool(chat), "chats": []}
    if token and not chat:
        try:
            r = requests.get(API.format(token=token, method="getUpdates"), timeout=10).json()
            seen = {}
            for u in r.get("result", []):
                c = (u.get("message") or u.get("my_chat_member") or {}).get("chat") or {}
                if c.get("id") is not None:
                    seen[c["id"]] = c.get("first_name") or c.get("title") or c.get("username") or ""
            out["chats"] = [{"id": k, "name": v} for k, v in seen.items()]
            if not r.get("ok"):
                out["error"] = r.get("description", "Telegram rejected the token")
        except requests.exceptions.RequestException as e:
            out["error"] = f"{type(e).__name__}: {e}"
    return out


def send(text: str, url: str = None, button: str = "Open bet slip") -> dict:
    """Send one message (plain text) with an optional link button. Returns
    {"ok": True} or {"ok": False, "error": ...}."""
    token, chat = _settings()
    if not token or not chat:
        return {"ok": False, "error": "Telegram isn't set up: add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to .env"}
    # Telegram caps a message at 4,096 characters: send long text in line-aligned
    # pieces, with the link button on the last one.
    if len(text) > 3800:
        lines, chunk, res = text.split("\n"), "", {"ok": True}
        for line in lines:
            if chunk and len(chunk) + len(line) + 1 > 3800:
                res = send(chunk, None)
                if not res["ok"]:
                    return res
                chunk = ""
            chunk = f"{chunk}\n{line}" if chunk else line[:3800]
        return send(chunk, url, button) if chunk else res
    body = {"chat_id": chat, "text": text, "disable_web_page_preview": True}
    if url and url.startswith(("https://", "http://")):
        body["reply_markup"] = {"inline_keyboard": [[{"text": button, "url": url}]]}
    try:
        r = requests.post(API.format(token=token, method="sendMessage"), json=body, timeout=10).json()
    except requests.exceptions.RequestException as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    if not r.get("ok"):
        return {"ok": False, "error": r.get("description", "Telegram refused the message")}
    return {"ok": True}
