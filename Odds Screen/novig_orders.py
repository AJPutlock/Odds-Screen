"""
NoVig order placement: one explicit user click -> one FOK order, with a confirm
step in between. Nothing else in the app may call place(): not the +EV scan,
not the Telegram live list, not a schedule (house rule, 2026-10-12). Auto-
betting would be a separate, explicit opt-in.

Flow
    preview(ref, stake, limit_price, context)  ->  confirm card + one-shot token
    place(token)                               ->  the order, then its real outcome

    `ref` is the board's "marketId|outcomeId" for the side clicked. preview
    re-derives EVERYTHING that matters for money -- which side, the line, the
    price, the size, the fee, the depth -- from NoVig's own catalog and the live
    book. The page's `context` is used only to label the tracker row, and if it
    disagrees with what NoVig says that outcome is, the order is refused.

Price rule: FOK at the price the user confirmed, or better, never worse. The
limit IS the confirmed price; NoVig fills a taker at each resting order's price,
so a book that improved fills better automatically, and a book that got worse
refuses before anything is sent and shows the new price for a fresh confirm.

Money rules (all read from .env on every call; nothing on the page can set them)
    NOVIG_TRADING_ENABLED=1      kill switch, OFF unless exactly "1"
    NOVIG_MAX_ORDER_USD=25       per-order cap
    NOVIG_MAX_DAY_USD=100        per-day cap (ET day; filled + pending orders)

Things NoVig's API does that this module is built around
  * A resent order ALWAYS places again -- clientId is a label, not an
    idempotency key. So place() sends exactly once and never retries. If the
    answer is lost it reconciles by clientId on the private stream / REST, and
    tells the user not to resend.
  * 201 means "queued", not filled. A FOK that can't fill arrives later as a
    `reject` event with no HTTP error, so place() waits on the private orders
    channel (scrapers/novig_ws.py) for the real outcome.
  * qty is contracts that pay 1c each: cost = price x qty x $0.01.
  * Taker fee = coefficient x P(1-P) x qty x 1c at the FILL price, charged only
    live for WHEN_LIVE markets (NCAAF/MLB/NHL live coefficient 0.06 since Oct 8).
  * Prices must sit on NoVig's grid: .001-.050 by .001, .055-.945 by .005,
    .950-.999 by .001.

Every attempt is in bets.db (source 'novig_api', book_key 'novig'): a row is
written BEFORE the order goes out, then becomes the filled bet (stake = cost +
fee, odds = the effective price) or 'void' with the reason it didn't fill. An
order whose outcome can't be established is never voided: after 5 minutes it
stays OPEN as "UNVERIFIED - check NoVig app" (see reconcile()).

The trading key is loaded only here (novig_client "trade" scope) and never
logged or returned. The websocket uses the read key.
"""
import logging
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import ROUND_FLOOR, Decimal

import requests
from dotenv import dotenv_values

import bets
import novig_client as nc
from scrapers import novig_catalog as cat
from scrapers import novig_ws as ws

logger = logging.getLogger(__name__)

SOURCE, BOOK_KEY, BOOK = "novig_api", "novig", "NoVig"
CONFIRM_TTL = 30.0          # seconds a confirm card stays placeable
FILL_WAIT = 8.0             # seconds to wait on the private stream after a 201
UNKNOWN_GIVE_UP = 300.0     # a pending row unresolved this long becomes UNVERIFIED (still OPEN)
SPORT_OF = {"NFL": "americanfootball_nfl", "NCAAF": "americanfootball_ncaaf"}
_ET = timezone(timedelta(hours=-4))

_tokens: dict = {}          # token -> confirmed order (one-shot)
_tok_lock = threading.Lock()
_place_lock = threading.Lock()      # one order in flight at a time
_reconciler = None


class OrderError(Exception):
    """Refused before anything was sent. `info` carries what the page should show."""

    def __init__(self, message, **info):
        super().__init__(message)
        self.info = {"error": message, **info}


# ── settings ──────────────────────────────────────────────────────────────────

def settings() -> dict:
    cfg = dotenv_values(nc._ENV_FILE)

    def num(k, default):
        try:
            return float(cfg.get(k) or default)
        except ValueError:
            return float(default)
    return {"enabled": (cfg.get("NOVIG_TRADING_ENABLED") or "").strip() == "1",
            "max_order": num("NOVIG_MAX_ORDER_USD", 25), "max_day": num("NOVIG_MAX_DAY_USD", 100),
            "env": (cfg.get("NOVIG_ENV") or "paper").strip().lower(),
            "has_trade_key": bool((cfg.get("NOVIG_TRADE_ID") or "").strip())}


def _today_spent() -> float:
    """Dollars committed today (ET) through the API: filled and still-pending orders."""
    start = datetime.now(_ET).replace(hour=0, minute=0, second=0, microsecond=0)
    total = 0.0
    for b in bets.list_bets():
        if b.get("source") != SOURCE or b.get("status") == "void":
            continue
        try:
            placed = datetime.fromisoformat(str(b.get("placed_at")).replace("Z", "+00:00"))
        except ValueError:
            continue
        if placed.tzinfo is None:
            placed = placed.replace(tzinfo=timezone.utc)
        if placed >= start:
            total += float(b.get("stake") or 0)
    return round(total, 2)


# ── prices ────────────────────────────────────────────────────────────────────

def on_grid(p: Decimal) -> bool:
    mils = p * 1000
    if mils != mils.to_integral_value():
        return False
    m = int(mils)
    return 1 <= m <= 50 or (55 <= m <= 945 and m % 5 == 0) or 950 <= m <= 999


def american(prob: float) -> int:
    return int(cat._american(1 / prob))


def _fee(coef: float, price: float, qty: int) -> float:
    return coef * price * (1 - price) * qty * 0.01


# ── what is this outcome, according to NoVig ─────────────────────────────────

def _market_and_event(market_id: str):
    """Market + its event + sibling markets, from the board's catalog cache when
    it has them, else from NoVig's public catalog (through the shared 2/s budget)."""
    for _, (_, events, markets) in list(cat._catalog.items()):
        for m in markets:
            if m.get("marketId") == market_id and m.get("eventId") in events:
                sibs = [x for x in markets if x.get("eventId") == m["eventId"]]
                return m, events[m["eventId"]], sibs
    m = cat._get(f"/catalog/markets/{market_id}")
    if not m:
        raise OrderError("NoVig doesn't know that market (or the catalog is throttled) - try again shortly")
    ev = cat._get(f"/catalog/events/{m['eventId']}") or {}
    sibs = (cat._get("/catalog/markets", event=m["eventId"], marketType="SPREAD,TOTAL", limit=200) or {}).get("items") or []
    return m, ev, sibs + [m]


def describe(ref: str) -> dict:
    try:
        market_id, outcome_id = (ref or "").split("|")
    except ValueError:
        raise OrderError("bad NoVig reference")
    m, ev, sibs = _market_and_event(market_id)
    mk = cat.TYPES.get(m.get("marketType"))
    if not mk:
        raise OrderError(f"NoVig {m.get('marketType')} markets aren't supported for orders")
    parts = (ev.get("description") or "").split(" @ ")
    if len(parts) != 2:
        raise OrderError("couldn't read the teams for this NoVig event")
    away, home = (cat._NAME_FIX.get(p.strip(), p.strip()) for p in parts)
    away_o, home_o = cat._sides(m, *cat._abbrevs(sibs))
    if not (away_o and home_o):
        raise OrderError("couldn't tell which NoVig outcome is which side")
    if outcome_id not in (away_o["outcomeId"], home_o["outcomeId"]):
        raise OrderError("that outcome isn't part of this market")
    is_home = outcome_id == home_o["outcomeId"]
    totals = mk.startswith("totals")
    strike = float(m["strike"]) if m.get("strike") is not None else None
    if totals:
        side, point, team = ("over" if is_home else "under"), strike, None     # board: home = Over
    elif mk.startswith("spreads"):
        side, team = ("home", home) if is_home else ("away", away)
        point = strike if is_home else -strike                                 # strike is home's handicap
    else:
        side, team, point = ("home", home, None) if is_home else ("away", away, None)
    mine, other = (home_o, away_o) if is_home else (away_o, home_o)
    fee = m.get("fee") or {}
    return {"market_id": market_id, "outcome_id": outcome_id, "other_outcome_id": other["outcomeId"],
            "outcome_name": mine.get("name"), "market": mk, "side": side, "team": team, "point": point,
            "away": away, "home": home, "event_id": m.get("eventId"),
            "league": ev.get("league"), "sport_key": SPORT_OF.get(ev.get("league")),
            "commence_time": (datetime.fromtimestamp(ev["startsTs"] / 1000, timezone.utc).isoformat()
                              if ev.get("startsTs") else None),
            "event_status": ev.get("status"), "market_status": m.get("status"),
            "fee_coef": float(fee.get("coefficient") or 0), "fee_charged": fee.get("charged") or "WHEN_LIVE"}


def _check_context(d: dict, ctx: dict) -> None:
    """The page's labels must match NoVig's own description of the outcome."""
    if not ctx:
        return
    ctx = dict(ctx)
    if d["side"] in ("over", "under") and ctx.get("side") in ("home", "away"):
        ctx["side"] = {"home": "over", "away": "under"}[ctx["side"]]    # the board stores Over as home
    want = {"market": d["market"], "side": d["side"]}
    if d["point"] is not None:
        want["point"] = d["point"]
    for k, v in want.items():
        got = ctx.get(k)
        if got in (None, ""):
            continue
        same = abs(float(got) - float(v)) < 1e-9 if k == "point" else str(got) == str(v)
        if not same:
            raise OrderError(f"the board says {k}={got} but NoVig says this outcome is {k}={v} - refusing",
                             mismatch=k)


# ── the live book ────────────────────────────────────────────────────────────

def _book(d: dict):
    lv = ws.levels(d["market_id"])
    if lv is not None:
        return lv, "stream"
    lv = cat._book(d["market_id"])
    if lv is None:
        raise OrderError("couldn't read NoVig's order book right now - try again shortly")
    return lv, "rest"


def _is_live(d: dict) -> bool:
    streamed = ws.market_live(d["market_id"])
    if streamed is not None:
        return streamed
    return str(d.get("event_status") or "").endswith("INGAME")


def _walk(d: dict, levels: dict, limit: Decimal, qty: int, live: bool) -> dict:
    """Simulate a FOK buy of `qty` at `limit` against the OTHER outcome's bids
    (complementary books: a bid at q on the other side is an ask at 1-q here)."""
    asks = sorted(((round(1 - p, 3), n) for p, n in levels.get(d["other_outcome_id"]) or []), key=lambda a: a[0])
    coef = d["fee_coef"] if (d["fee_charged"] == "ALWAYS" or (d["fee_charged"] == "WHEN_LIVE" and live)) else 0.0
    best = asks[0][0] if asks else None
    fills, need, depth_at_limit = [], qty, 0
    for a, n in asks:
        if a > float(limit):
            break
        depth_at_limit += n
        if need > 0:
            take = min(n, need)
            fills.append((a, take))
            need -= take
    nxt = next(((a, n) for a, n in asks if a > float(limit)), None)
    cost = sum(a * n * 0.01 for a, n in fills)
    fee = sum(_fee(coef, a, n) for a, n in fills)
    return {"best_ask": best, "fills": fills, "filled": qty - need, "short": need,
            "depth_contracts": depth_at_limit, "depth_usd": round(sum(a * n * 0.01 for a, n in asks if a <= float(limit)), 2),
            "next_price": nxt[0] if nxt else None, "next_depth_usd": round(nxt[0] * nxt[1] * 0.01, 2) if nxt else None,
            "cost": round(cost, 5), "fee": round(fee, 5), "fee_coef": coef}


def _trade_client():
    return nc.NovigClient("trade")


def _balance() -> float:
    c = _trade_client()
    return float(c.request("GET", f"/v3/account/subaccounts/{c.key_id}/balance")["balance"])


# ── preview / place ──────────────────────────────────────────────────────────

def _assess(ref: str, stake: float, limit_price, context: dict) -> dict:
    s = settings()
    if s["env"] != "prod":
        raise OrderError("NOVIG_ENV isn't prod - the board's NoVig prices are production markets")
    if not s["has_trade_key"]:
        raise OrderError("no NoVig trading key in .env - run: python novig_setup.py open-subaccount")
    try:
        limit = Decimal(str(limit_price)).quantize(Decimal("0.001"))
        stake = float(stake)
    except Exception:
        raise OrderError("bad price or stake")
    if not on_grid(limit):
        raise OrderError(f"{limit} isn't a price NoVig accepts")
    if stake <= 0:
        raise OrderError("stake must be positive")
    d = describe(ref)
    _check_context(d, context or {})
    if d["market_status"] != "OPEN" or ws.market_status(d["market_id"]) in ("CLOSED", "SETTLED"):
        raise OrderError("this NoVig market is closed")
    qty = int((Decimal(str(stake)) / (limit * Decimal("0.01"))).to_integral_value(rounding=ROUND_FLOOR))
    if qty < 1:
        raise OrderError("stake too small for one contract at this price")
    levels, src = _book(d)
    live = _is_live(d)
    w = _walk(d, levels, limit, qty, live)
    spent = _today_spent()
    problems = []
    if not s["enabled"]:
        problems.append("trading is OFF - set NOVIG_TRADING_ENABLED=1 in .env to allow orders")
    if w["best_ask"] is None:
        problems.append("nobody is offering this side right now")
    elif w["best_ask"] > float(limit):
        problems.append(f"price moved: best is now {w['best_ask'] * 100:.1f}% ({american(w['best_ask']):+d}), "
                        f"not {float(limit) * 100:.1f}% - re-confirm at the new price")
    elif w["short"] > 0:
        problems.append(f"only ${w['depth_usd']:.2f} available at {float(limit) * 100:.1f}% or better"
                        + (f"; next price {w['next_price'] * 100:.1f}% with ${w['next_depth_usd']:.2f}"
                           if w["next_price"] else ""))
    if stake > s["max_order"]:
        problems.append(f"over the per-order cap (${s['max_order']:.0f}, NOVIG_MAX_ORDER_USD)")
    if spent + stake > s["max_day"]:
        problems.append(f"over the daily cap: ${spent:.2f} used of ${s['max_day']:.0f} (NOVIG_MAX_DAY_USD)")
    if not ws.private_ready():
        ws.start()
        problems.append("the live NoVig connection isn't ready yet - try again in a few seconds")
    if _pending():
        # A lost answer means an order may exist that we haven't seen yet; a new
        # order now could be a duplicate. Hold until reconcile resolves it.
        problems.append("an earlier NoVig order is still being confirmed - wait for it (up to a few minutes)")
    total = w["cost"] + w["fee"]
    try:
        bal = _balance()
        if total > bal + 1e-9:
            problems.append(f"NoVig trading wallet has ${bal:.2f}; this needs ${total:.2f}")
    except nc.NovigError as e:
        bal = None
        problems.append(f"couldn't read the NoVig wallet ({e.code})")
    payout = qty * 0.01
    eff = total / payout if payout else None
    return {"desc": d, "limit": limit, "qty": qty, "stake_req": stake, "walk": w, "live": live,
            "book_source": src, "spent_today": spent, "settings": s, "balance": bal,
            "problems": problems, "total": round(total, 2), "payout": round(payout, 2),
            "eff_prob": eff}


def _card(a: dict) -> dict:
    d, w, s = a["desc"], a["walk"], a["settings"]
    line = (f"{'Over' if d['side'] == 'over' else 'Under'} {d['point']:g}" if d["side"] in ("over", "under")
            else f"{d['team']} {d['point']:+g}" if d["point"] is not None else f"{d['team']} ML")
    avg = (sum(p * n for p, n in w["fills"]) / w["filled"]) if w["filled"] else None
    return {"game": f"{d['away']} @ {d['home']}", "market": d["market"], "line": line,
            "novig_outcome": d["outcome_name"], "side": d["side"], "point": d["point"],
            "price_pct": round(float(a["limit"]) * 100, 1), "price_american": american(float(a["limit"])),
            "expected_avg_pct": round(avg * 100, 2) if avg else None,
            "contracts": a["qty"], "cost": round(w["cost"], 2), "fee": round(w["fee"], 2),
            "fee_note": ("live taker fee" if w["fee_coef"] else "no fee (pregame)"),
            "total": a["total"], "to_win": round(a["payout"] - a["total"], 2), "payout": a["payout"],
            "effective_american": american(a["eff_prob"]) if a["eff_prob"] and a["eff_prob"] < 1 else None,
            "depth_usd_at_price": w["depth_usd"], "best_pct_now": round(w["best_ask"] * 100, 1) if w["best_ask"] else None,
            "book_source": a["book_source"], "live": a["live"],
            "today_used": a["spent_today"], "day_cap": s["max_day"], "order_cap": s["max_order"],
            "wallet": a["balance"], "trading_enabled": s["enabled"], "tif": "FOK",
            "problems": a["problems"], "placeable": not a["problems"]}


def preview(ref: str, stake, limit_price, context: dict = None) -> dict:
    """The confirm card. Returns a one-shot `token` only when the order would be
    placeable right now; the token pins the exact body that place() will send."""
    _ensure_reconciler()
    try:
        a = _assess(ref, stake, limit_price, context)
    except OrderError as e:
        return {"placeable": False, **e.info}
    card = _card(a)
    if card["placeable"]:
        tok = uuid.uuid4().hex
        with _tok_lock:
            now = time.time()
            for k in [k for k, v in _tokens.items() if v["exp"] < now]:
                _tokens.pop(k, None)
            _tokens[tok] = {"exp": now + CONFIRM_TTL, "ref": ref, "stake": a["stake_req"],
                            "limit": str(a["limit"]), "context": context or {}, "card": card}
        card["token"] = tok
        card["expires_in"] = CONFIRM_TTL
    return card


def place(token: str) -> dict:
    """Send the confirmed order exactly once and report what actually happened."""
    with _tok_lock:
        t = _tokens.pop(token or "", None)             # one-shot: a double click can't send twice
    if not t or t["exp"] < time.time():
        return {"ok": False, "state": "EXPIRED", "error": "that confirm expired - check the price again"}
    if not settings()["enabled"]:
        return {"ok": False, "state": "DISABLED", "error": "trading is OFF (NOVIG_TRADING_ENABLED)"}
    with _place_lock:
        try:                                            # prices move: re-check against the live book NOW
            a = _assess(t["ref"], t["stake"], t["limit"], t["context"])
        except OrderError as e:
            return {"ok": False, "state": "REFUSED", **e.info}
        if a["problems"]:
            return {"ok": False, "state": "REFUSED", "error": a["problems"][0], "card": _card(a)}
        if a["qty"] != t["card"]["contracts"]:
            return {"ok": False, "state": "REFUSED", "error": "order size changed since the confirm - re-check"}
        return _send(a, t["context"])


def _send(a: dict, ctx: dict) -> dict:
    d = a["desc"]
    client_id = str(uuid.uuid4())
    body = {"outcomeId": d["outcome_id"], "price": f"{a['limit']:.3f}", "qty": a["qty"],
            "tif": "FOK", "clientId": client_id}
    row = bets.add_bet({
        "sport_key": d["sport_key"], "event_id": ctx.get("event_id") or d["event_id"],
        "away": d["away"], "home": d["home"], "commence_time": ctx.get("commence_time") or d["commence_time"],
        "market": d["market"], "side": d["side"], "team": d["team"], "point": d["point"],
        "odds": american(a["eff_prob"]), "book_key": BOOK_KEY, "book": BOOK, "stake": a["total"],
        "fair_prob": ctx.get("fair_prob"), "edge": ctx.get("edge"), "ev": ctx.get("ev"),
        "kelly_rec": ctx.get("kelly_rec"), "is_alt": bool(ctx.get("is_alt")), "source": SOURCE,
        "notes": f"NOVIG PENDING clientId={client_id} outcome={d['outcome_id']} "
                 f"limit={body['price']} qty={a['qty']} FOK",
    })
    sent_at = time.time()
    try:
        r = _trade_client().request("POST", "/v3/orders", body=body, timeout=10)
    except nc.NovigError as e:
        why = e.code
        if e.status == 451:
            why += " - open the NoVig app on your phone so it re-checks your location"
        bets.update_bet(row["id"], {"status": "void", "notes": f"NOT SENT/REFUSED by NoVig: {e.status} {why} "
                                                                f"(clientId={client_id}) - not a bet"})
        return {"ok": False, "state": "REJECTED", "error": f"NoVig refused the order: {why}", "bet_id": row["id"]}
    except (requests.Timeout, requests.ConnectionError) as e:
        # The order may or may not exist. NEVER resend: look for it by clientId.
        oid, o = _find_by_client(client_id, wait=FILL_WAIT)
        if o is None:
            bets.update_bet(row["id"], {"notes": row["notes"] + f" | NO RESPONSE ({type(e).__name__}) at "
                                                               f"{datetime.now(_ET):%H:%M:%S} - reconciling"})
            return {"ok": False, "state": "UNKNOWN", "bet_id": row["id"],
                    "error": "no answer from NoVig - the order may still have gone through. Do NOT resend; "
                             "it's being reconciled automatically (check the NoVig app)."}
        return _finish(row, oid, o, a, client_id)
    oid = (r or {}).get("orderId")
    o = ws.wait_order(oid, timeout=FILL_WAIT) if oid else None
    if o is None or o.get("state") not in ("FILLED", "REJECTED", "CANCELED"):
        bets.update_bet(row["id"], {"notes": row["notes"] + f" orderId={oid} | queued, no outcome after "
                                                           f"{FILL_WAIT:g}s - reconciling"})
        return {"ok": False, "state": "UNKNOWN", "bet_id": row["id"], "order_id": oid,
                "error": "NoVig queued the order but hasn't reported the result yet - reconciling. Do NOT resend."}
    return _finish(row, oid, o, a, client_id)


def _finish(row: dict, oid: str, o: dict, a: dict, client_id: str) -> dict:
    """Turn a terminal order into the tracker row."""
    d = a["desc"]
    if o["state"] == "FILLED" and o.get("filled"):
        filled = int(o["filled"])
        fills = [(float(ev["price"]), int(ev["qty"])) for ev in o["events"] if ev.get("kind") == "fill"]
        cost = sum(p * n * 0.01 for p, n in fills)
        coef = a["walk"]["fee_coef"]
        fee = sum(_fee(coef, p, n) for p, n in fills)
        payout = filled * 0.01
        stake = round(cost + fee, 2)
        avg = cost / payout
        bets.update_bet(row["id"], {
            "status": "open", "stake": stake, "odds": american((cost + fee) / payout),
            "notes": f"NoVig API order {oid} clientId={client_id}: {filled} contracts @ avg {avg:.4f} "
                     f"(limit {a['limit']}), cost ${cost:.2f}" + (f" + fee ${fee:.2f}" if fee else "") +
                     f", pays ${payout:.2f}"})
        return {"ok": True, "state": "FILLED", "bet_id": row["id"], "order_id": oid,
                "contracts": filled, "avg_pct": round(avg * 100, 2), "cost": round(cost, 2),
                "fee": round(fee, 2), "stake": stake, "to_win": round(payout - stake, 2),
                "odds": american((cost + fee) / payout), "line": _card(a)["line"], "game": f"{d['away']} @ {d['home']}"}
    why = {"REJECTED": "NOT FILLED - FOK found too little at the price (the book moved)",
           "CANCELED": f"CANCELED by NoVig ({o.get('reason') or 'no reason given'})"}.get(o["state"], o["state"])
    bets.update_bet(row["id"], {"status": "void", "notes": f"{why}: order {oid} clientId={client_id} - not a bet"})
    return {"ok": False, "state": o["state"], "bet_id": row["id"], "order_id": oid, "error": why}


# ── reconciliation (lost answers, fills seen while the page wasn't waiting) ───

def _pending() -> list:
    """Tracker rows for API orders whose outcome isn't known yet."""
    return [b for b in bets.list_bets(status="open")
            if b.get("source") == SOURCE and (b.get("notes") or "").startswith("NOVIG PENDING")]


def _find_by_client(client_id: str, wait: float = 0.0):
    end = time.time() + wait
    while True:
        oid, o = ws.order_by_client(client_id)
        if o and o.get("state") in ("FILLED", "REJECTED", "CANCELED"):
            return oid, o
        if time.time() >= end:
            return oid, o
        time.sleep(0.25)


def reconcile() -> int:
    """Resolve tracker rows still marked PENDING or UNVERIFIED. Returns how many changed.

    A PENDING row unresolved after UNKNOWN_GIVE_UP is NOT voided: if it really
    filled, a void would hide live exposure from the tracker and from correlated
    Kelly -- the wrong way to be wrong with real money (lead's rule, 10/12). It
    stays OPEN as "UNVERIFIED - check NoVig app", gets one Telegram line, new
    orders are allowed again, and it keeps being reconciled here until a fill or
    cancel turns up or the user settles it with the tracker's W/L/V buttons."""
    changed = 0
    for b in bets.list_bets(status="open"):
        notes = b.get("notes") or ""
        if b.get("source") != SOURCE or not notes.startswith(("NOVIG PENDING", "UNVERIFIED")):
            continue
        kv = dict(p.split("=", 1) for p in notes.replace("|", " ").split() if "=" in p)
        client_id, oid = kv.get("clientId"), kv.get("orderId")
        o = ws.order(oid) if oid else None
        if o is None:
            oid2, o = ws.order_by_client(client_id) if client_id else (None, None)
            oid = oid or oid2
        if o is None and oid:
            try:                                      # REST: the order's own record
                rec = _trade_client().request("GET", f"/v3/orders/{oid}")
                st = (rec or {}).get("status")
                if st in ("REJECTED", "CANCELED"):
                    o = {"state": st, "events": []}
                elif st == "FILLED":
                    fl = _trade_client().request("GET", "/v3/portfolio/fills", query=f"order={oid}") or {}
                    items = fl.get("items") if isinstance(fl, dict) else fl
                    evs = [{"kind": "fill", "price": x.get("price"), "qty": x.get("qty")} for x in items or []]
                    o = {"state": "FILLED", "events": evs, "filled": sum(int(e["qty"] or 0) for e in evs)}
            except nc.NovigError:
                pass
        try:
            placed = datetime.fromisoformat(str(b.get("placed_at")).replace("Z", "+00:00"))
            if placed.tzinfo is None:
                placed = placed.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - placed).total_seconds()
        except ValueError:
            age = 0
        if o is None or o.get("state") not in ("FILLED", "REJECTED", "CANCELED"):
            if age > UNKNOWN_GIVE_UP and notes.startswith("NOVIG PENDING"):
                bets.update_bet(b["id"], {"notes": f"UNVERIFIED — check NoVig app (clientId {client_id}) | {notes}"})
                _alert(f"NoVig order unverified — check the app ({b.get('away')} @ {b.get('home')}, "
                       f"{b.get('market')} {b.get('side')}{'' if b.get('point') is None else ' ' + str(b.get('point'))})")
                changed += 1
            continue
        if o["state"] == "FILLED":
            fills = [(float(e["price"]), int(e["qty"])) for e in o["events"] if e.get("kind") == "fill" and e.get("price")]
            filled = sum(n for _, n in fills)
            if not filled:
                continue
            cost = sum(p * n * 0.01 for p, n in fills)
            payout = filled * 0.01
            bets.update_bet(b["id"], {"stake": round(cost, 2), "odds": american(cost / payout),
                                      "notes": f"NoVig API order {oid} clientId={client_id}: {filled} contracts, "
                                               f"cost ${cost:.2f}, pays ${payout:.2f} (reconciled; any live fee "
                                               f"is in NoVig's ledger)"})
        else:
            bets.update_bet(b["id"], {"status": "void", "notes": f"NOT FILLED ({o['state']}): order {oid} "
                                                                 f"clientId={client_id} - not a bet (reconciled)"})
        changed += 1
    return changed


def _alert(text: str) -> None:
    """One Telegram line; never let a notification failure break reconciliation."""
    try:
        import phone_notify
        phone_notify.send(text)
    except Exception as e:
        logger.warning(f"novig-orders: telegram alert failed ({type(e).__name__})")


def _ensure_reconciler() -> None:
    global _reconciler
    if _reconciler and _reconciler.is_alive():
        return

    def loop():
        while True:
            time.sleep(30)
            try:
                n = reconcile()
                if n:
                    logger.info(f"novig-orders: reconciled {n} pending order(s)")
            except Exception as e:
                logger.warning(f"novig-orders: reconcile: {type(e).__name__}: {e}")
    _reconciler = threading.Thread(target=loop, daemon=True, name="novig-reconcile")
    _reconciler.start()


def status() -> dict:
    s = settings()
    return {"trading_enabled": s["enabled"], "env": s["env"], "has_trade_key": s["has_trade_key"],
            "max_order": s["max_order"], "max_day": s["max_day"], "today_used": _today_spent(),
            "private_stream_ready": ws.private_ready()}
