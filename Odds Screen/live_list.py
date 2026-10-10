"""
Live bet list on the user's phone, in their Telegram chat with the bot.

The odds screen page computes +EV (scanEV, region, positions, Kelly) and
POSTs the bets that qualify to /api/live-list/sync about once a minute while
it's open. This module keeps the chat in step with that list:

  - a new bet       -> its own message: bet, price, book, EV, stake, warnings,
                       buttons [Open bet slip] [Placed ✓] [Skip]
  - price/EV moved  -> that message is edited in place
  - edge gone       -> marked GONE (struck through, buttons removed)
  - kickoff passed  -> message deleted
  - one pinned summary message lists every live bet with its link, and says
    when the odds screen stopped updating (page closed).

Placed ✓ logs the bet to the tracker (bets.db) at the listed price and stake;
Skip hides it until its price changes. Button taps arrive through Telegram's
getUpdates long poll (poll_loop, started with the app's other threads), so no
public server is needed. State (message ids) lives in data/live_list.json so a
restart can still edit and delete what it sent.
"""
import html
import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

import bets as bets_store
import bet_grader
import phone_notify

STATE_PATH = Path(__file__).parent / "data" / "live_list.json"
API = "https://api.telegram.org/bot{token}/{method}"
STALE_AFTER = 15 * 60          # no sync for this long -> summary says the screen isn't updating
MAX_OPS_PER_SYNC = 25          # Telegram allows ~1 message/s per chat; edits count
CARD_FORMAT = 3                # bump to re-render every live message once in a new layout

_lock = threading.Lock()
_state = {"items": {}, "summary_id": None, "last_sync": None, "offset": 0}


def _load():
    try:
        _state.update(json.loads(STATE_PATH.read_text(encoding="utf-8")))
    except (OSError, ValueError):
        pass


def _save():
    STATE_PATH.parent.mkdir(exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(_state), encoding="utf-8")
    tmp.replace(STATE_PATH)


_load()


# ── Message cards ────────────────────────────────────────────────────────────
# Each bet is a small card: the team's logo as a link-preview thumbnail (no
# visible URL), the bet in bold, one line of numbers, the sharp books' prices
# (C: Circa  BM: Bookmaker  BO: BetOnline), the matchup, then any
# position notes. Placed / skipped / gone collapse to one line.

ESPN_LOGO_SPORTS = {   # scoreboard paths (logos only — grading has its own list)
    "americanfootball_nfl": ("football/nfl", {}),
    "americanfootball_ncaaf": ("football/college-football", {"groups": "80", "limit": "500"}),
    "icehockey_nhl": ("hockey/nhl", {}),
    "basketball_nba": ("basketball/nba", {}),
    "basketball_ncaab": ("basketball/mens-college-basketball", {"groups": "50", "limit": "500"}),
    "baseball_mlb": ("baseball/mlb", {}),
}
_logo_boards: dict = {}    # (sport, yyyymmdd) -> (fetched_at, [(away, away_logo, home, home_logo)])
_logo_cache: dict = {}     # (sport, away, home) -> (away_logo, home_logo)


def _logos(card):
    """(away_logo, home_logo) URLs from ESPN's scoreboard for this game, or (None, None)."""
    sk, away, home = card.get("sport_key"), card.get("away"), card.get("home")
    if sk not in ESPN_LOGO_SPORTS or not away or not home:
        return None, None
    ck = (sk, away, home)
    if ck in _logo_cache:
        return _logo_cache[ck]
    try:
        k = datetime.fromisoformat(str(card.get("kickoff")).replace("Z", "+00:00")).astimezone(bet_grader.ET)
        day = k.strftime("%Y%m%d")
    except (TypeError, ValueError):
        day = datetime.now(bet_grader.ET).strftime("%Y%m%d")
    hit = _logo_boards.get((sk, day))
    if not hit or time.time() - hit[0] > 6 * 3600:
        path, extra = ESPN_LOGO_SPORTS[sk]
        games = []
        try:
            r = requests.get(bet_grader.ESPN.format(path=path), params={"dates": day, **extra}, timeout=15)
            r.raise_for_status()
            for e in r.json().get("events", []):
                comp = (e.get("competitions") or [{}])[0]
                t = {c.get("homeAway"): c.get("team") or {} for c in comp.get("competitors", [])}
                if "home" in t and "away" in t:
                    games.append((t["away"], t["home"]))
        except (requests.exceptions.RequestException, ValueError) as e:
            print(f"[live-list] logos {sk} {day}: {type(e).__name__}: {e}")
        hit = (time.time(), games)
        _logo_boards[(sk, day)] = hit
    best = None
    for ta, th in hit[1]:
        a = bet_grader._team_score(away, {"name": ta.get("displayName"), "location": ta.get("location"),
                                          "short_name": ta.get("shortDisplayName")})
        h = bet_grader._team_score(home, {"name": th.get("displayName"), "location": th.get("location"),
                                          "short_name": th.get("shortDisplayName")})
        if min(a, h) >= 0.6 and (not best or a + h > best[0]):
            best = (a + h, ta.get("logo"), th.get("logo"))
    out = (best[1], best[2]) if best else (None, None)
    _logo_cache[ck] = out
    return out


def _preview(item):
    """link_preview_options showing the bet team's logo small (totals: the home team's)."""
    card = item.get("card") or {}
    away_logo, home_logo = _logos(card) if card else (None, None)
    logo = away_logo if card.get("side") == "away" else home_logo
    if not logo:
        return {"is_disabled": True}
    return {"url": logo, "prefer_small_media": True, "show_above_text": False}


def _money(v):
    return f"${v:,.0f}" if v >= 20 else f"${v:,.2f}"


def _render(item):
    """The live message for one bet (HTML)."""
    c = item.get("card")
    if not c:
        return html.escape(item.get("text") or "")
    e = html.escape
    nums = [f"<b>+{c['ev'] * 100:.1f}% EV</b>"]
    if c.get("stake") is not None:
        nums.append(f"bet <b>{_money(c['stake'])}</b>")
    if c.get("fair"):
        nums.append(f"fair {e(str(c['fair']))}")
    lines = [f"{e(c.get('emoji') or '')} <b>{e(c['label'])}</b>  <code>{e(str(c['odds']))}</code>  {e(c['book'])}",
             " · ".join(nums),
             *([f"<code>{e(c['sharp'])}</code>"] if c.get("sharp") else []),
             f"<i>{e(c['matchup'])}{' · ' + e(c['when']) if c.get('when') else ''}</i>"]
    lines += [e(n) for n in (c.get("notes") or []) if n]
    return "\n".join(lines)


def _one_line(item):
    c = item.get("card")
    if c:
        return f"{c.get('emoji') or ''} {c['label']} {c['odds']} {c['book']}".strip()
    return (item.get("text") or "").split("\n")[0]


def _call(method, **body):
    token, chat = phone_notify._settings()
    if not token or not chat:
        return None
    if method not in ("getUpdates", "answerCallbackQuery"):
        body.setdefault("chat_id", chat)
    for _ in range(2):
        try:
            r = requests.post(API.format(token=token, method=method), json=body,
                              timeout=40 if method == "getUpdates" else 10).json()
        except requests.exceptions.RequestException as e:
            print(f"[live-list] {method}: {phone_notify.safe_error(e)}")
            return None
        if r.get("ok"):
            return r.get("result")
        retry = (r.get("parameters") or {}).get("retry_after")
        if retry:                                  # rate limited: wait once, retry once
            time.sleep(min(float(retry), 30))
            continue
        desc = r.get("description", "")
        if "message is not modified" not in desc and "message to delete not found" not in desc:
            print(f"[live-list] {method}: {desc}")
        return None
    return None


def _kb(item, live=True):
    rows = []
    if item.get("url"):
        rows.append([{"text": item.get("button") or "Open bet slip", "url": item["url"]}])
    if live:
        rows.append([{"text": "Placed ✓", "callback_data": f"p:{item['short']}"},
                     {"text": "Skip", "callback_data": f"s:{item['short']}"}])
    return {"inline_keyboard": rows}


def _short(key):
    # callback_data is capped at 64 bytes: a stable short id per bet key
    import hashlib
    return hashlib.sha1(key.encode()).hexdigest()[:12]


def _kicked_off(item, now):
    k = item.get("kickoff")
    if not k:
        return False
    try:
        return datetime.fromisoformat(k.replace("Z", "+00:00")) <= now
    except ValueError:
        return False


def sync(items: list) -> dict:
    """Make the chat match `items` (the page's current qualifying bets)."""
    token, chat = phone_notify._settings()
    if not token or not chat:
        return {"ok": False, "error": "Telegram isn't set up (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID in .env)"}
    now = datetime.now(timezone.utc)
    ops = sent = edited = gone = deleted = 0
    with _lock:
        st = _state["items"]
        incoming = {}
        for it in items:
            key = str(it.get("key") or "")
            if not key or not it.get("text") or _kicked_off(it, now):
                continue
            incoming[key] = it
        for key, it in incoming.items():
            if ops >= MAX_OPS_PER_SYNC:
                break
            cur = st.get(key)
            text = it["text"]
            if cur and cur.get("status") in ("placed",):
                continue
            if cur and cur.get("status") == "skipped":
                if cur.get("text") == text:
                    continue                           # skipped until the price changes
                cur["status"] = "live"
            if not cur or cur.get("status") == "gone" and not cur.get("message_id"):
                short = _short(key)
                item = {**it, "short": short, "status": "live", "fmt": CARD_FORMAT}
                res = _call("sendMessage", text=_render(item), parse_mode="HTML", link_preview_options=_preview(item),
                            reply_markup=_kb(item))
                ops += 1
                if res:
                    item["message_id"] = res["message_id"]
                    st[key] = item
                    sent += 1
                continue
            if cur.get("text") != text or cur.get("status") == "gone" or cur.get("fmt") != CARD_FORMAT:
                cur.update({k: v for k, v in it.items() if k != "short"})
                cur["status"] = "live"
                cur["fmt"] = CARD_FORMAT
                _call("editMessageText", message_id=cur["message_id"], text=_render(cur), parse_mode="HTML",
                      link_preview_options=_preview(cur), reply_markup=_kb(cur))
                ops += 1
                edited += 1
        # Bets no longer qualifying: mark GONE once; delete everything after kickoff.
        for key, cur in list(st.items()):
            if _kicked_off(cur, now):
                if cur.get("message_id"):
                    _call("deleteMessage", message_id=cur["message_id"])
                del st[key]
                deleted += 1
                continue
            if key not in incoming and cur.get("status") == "live" and ops < MAX_OPS_PER_SYNC:
                cur["status"] = "gone"
                _call("editMessageText", message_id=cur["message_id"],
                      text="⛔ <s>" + html.escape(_one_line(cur)) + "</s> · gone", parse_mode="HTML",
                      link_preview_options={"is_disabled": True}, reply_markup={"inline_keyboard": []})
                ops += 1
                gone += 1
        _state["last_sync"] = now.isoformat()
        _state["stale_flagged"] = False
        _update_summary(now)
        _save()
    return {"ok": True, "live": sum(1 for c in _state["items"].values() if c.get("status") == "live"),
            "sent": sent, "edited": edited, "gone": gone, "deleted": deleted}


def _update_summary(now, stale=False):
    live = [c for c in _state["items"].values() if c.get("status") == "live"]
    live.sort(key=lambda c: -(c.get("ev") or 0))
    stamp = now.astimezone().strftime("%a %I:%M %p").replace(" 0", " ")
    lines = [f"📋 <b>Live bets</b> ({len(live)}) · updated {stamp}"]
    if stale:
        lines = [f"⚠️ <b>Odds screen isn't updating</b> (last update {stamp}). These may be stale."] + lines[1:]
    groups = {}
    for c in live[:40]:
        card = c.get("card") or {}
        groups.setdefault(card.get("emoji") or "•", []).append(c)
    for emoji, cs in groups.items():
        lines.append("")
        for c in cs:
            card = c.get("card")
            if card:
                label = (f"<b>{html.escape(card['label'])}</b> {html.escape(str(card['odds']))} "
                         f"{html.escape(card['book'])} · +{card['ev'] * 100:.1f}%")
            else:
                label = html.escape(c.get("summary") or c.get("key"))
            label = f"<a href=\"{html.escape(c['url'])}\">{label}</a>" if c.get("url") else label
            lines.append(f"{emoji} {label}")
    if not live:
        lines.append("Nothing qualifies right now.")
    text = "\n".join(lines)
    if _state.get("summary_text") == text:
        return
    sid = _state.get("summary_id")
    ok = sid and _call("editMessageText", message_id=sid, text=text, parse_mode="HTML",
                       disable_web_page_preview=True)
    if not ok:
        res = _call("sendMessage", text=text, parse_mode="HTML", disable_web_page_preview=True,
                    disable_notification=True)
        if res:
            _state["summary_id"] = res["message_id"]
            _call("pinChatMessage", message_id=res["message_id"], disable_notification=True)
    _state["summary_text"] = text


def _handle_callback(cb):
    data = cb.get("data") or ""
    action, _, short = data.partition(":")
    with _lock:
        key, cur = next(((k, c) for k, c in _state["items"].items() if c.get("short") == short), (None, None))
        if not cur:
            _call("answerCallbackQuery", callback_query_id=cb["id"], text="That bet is no longer on the list")
            return
        if action == "p" and cur.get("status") == "live":
            try:
                bet = bets_store.add_bet({**(cur.get("bet") or {}), "source": "telegram"})
                note = f"✅ <b>PLACED</b> · {html.escape(_one_line(cur))} · {_money(float(bet.get('stake') or 0))} · tracker #{bet['id']}"
                cur["status"] = "placed"
            except (ValueError, TypeError) as e:
                note = html.escape(f"Couldn't log it: {e}") + "\n" + _render(cur)
            _call("editMessageText", message_id=cur["message_id"], text=note,
                  parse_mode="HTML", link_preview_options=_preview(cur), reply_markup=_kb(cur, live=False))
            _call("answerCallbackQuery", callback_query_id=cb["id"], text="Logged to the tracker")
        elif action == "s" and cur.get("status") == "live":
            cur["status"] = "skipped"
            _call("editMessageText", message_id=cur["message_id"], text="↷ <s>" + html.escape(_one_line(cur)) + "</s> · skipped",
                  parse_mode="HTML", link_preview_options={"is_disabled": True}, reply_markup={"inline_keyboard": []})
            _call("answerCallbackQuery", callback_query_id=cb["id"], text="Skipped until the price changes")
        else:
            _call("answerCallbackQuery", callback_query_id=cb["id"])
        _update_summary(datetime.now(timezone.utc))
        _save()


def poll_loop():
    """Button taps (getUpdates long poll) + kickoff cleanup + 'screen closed' notice."""
    while True:
        token, chat = phone_notify._settings()
        if not token or not chat:
            time.sleep(30)
            continue
        res = _call("getUpdates", offset=_state.get("offset", 0), timeout=30,
                    allowed_updates=["callback_query"])
        for u in res or []:
            _state["offset"] = u["update_id"] + 1
            if u.get("callback_query"):
                try:
                    _handle_callback(u["callback_query"])
                except Exception as e:
                    print(f"[live-list] button: {type(e).__name__}: {e}")
        now = datetime.now(timezone.utc)
        with _lock:
            changed = False
            for key, cur in list(_state["items"].items()):
                if _kicked_off(cur, now):
                    if cur.get("message_id"):
                        _call("deleteMessage", message_id=cur["message_id"])
                    del _state["items"][key]
                    changed = True
            last = _state.get("last_sync")
            stale = bool(last) and (now - datetime.fromisoformat(last)).total_seconds() > STALE_AFTER
            if changed or (stale and not _state.get("stale_flagged")):
                _update_summary(datetime.fromisoformat(last) if stale else now, stale=stale)
                _state["stale_flagged"] = stale
                changed = True
            if changed or res:
                _save()
        if res is None:
            time.sleep(5)
