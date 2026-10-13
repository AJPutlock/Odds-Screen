"""
NoVig live order books (and the user's own order fills) over NoVig's v3
websocket, so the odds board and the order path see every price as it moves
instead of whatever the 2-request-a-second public REST poller last reached.

Keys: ONLY trading::read keys sign these connections (NOVIG_READ_ID/_KEY, plus
optional NOVIG_READ_ID_2/_KEY_2 ... _5). A read key cannot place orders or move
money, and its reads tolerate a stale phone geolocation. The trading key never
touches this module.

What the live exchange actually does (probed 2026-10-12; several differ from
docs.novig.com, which is why each is spelled out):

  * Limits come from GET /v3/limits, and this key's are far below the docs:
    maxWatchedMarkets 2048 per connection (docs: 8,192) and a `stream` bucket of
    512 refilling at 4/s (docs: 32/s), so a full refill takes ~2 minutes.
  * Subscribing to an EVENT pulls in every prop (one NFL game was 786 markets),
    so this subscribes to individual game-line MARKETS, most important first.
    The board's slate is ~4,600 game-line markets (902 NFL + 3,677 NCAAF), so
    one key covers the top ~2,000 and the REST poller keeps the tail.
    Each extra read key adds its own connection, its own 2,048 cap and its own
    bucket -- "split your work across keys" is NoVig's documented scaling path.
  * A `book` subscribe costs 16 tokens a market, but a request bigger than the
    bucket is charged only the full bucket -- and passes ONLY when the bucket
    is completely full. The 32-token websocket upgrade means that never holds
    straight after connecting, so the big subscribe waits for the refill.
    1,900 markets went in one 89 KB frame; the snapshot came back in 0.2 s.
  * Book entries use `orderId` (docs say `order`), and there is an undocumented
    `update` delta, {orderId, remaining}: a partial fill shrinks the resting
    order in place instead of remove + add.
  * ~47 book deltas a second across 1,900 markets; deflate halves the bytes.

Books are COMPLEMENTARY bid books, exactly as the REST /book route returns
them: levels(mid) -> {outcomeId: [(price, qty), ...]}, qty in cents of payout.
scrapers/novig_catalog.py reads those and keeps every price check it already
applies (_fill's $10 depth walk, _monotone, _plausible_main), so a streamed
book is held to the same standard as a polled one.

A market is only served while its connection is up and its seq chain is
unbroken. Any gap re-snapshots that market; until then levels() returns None
and the catalog falls back to its REST copy.

Private stream: the first connection also carries the subaccount's `orders`
and `positions` channels. novig_orders.py waits on those for an order's real
outcome, because NoVig's 201 only means "queued": a FOK that can't fill comes
back as a `reject` event with no HTTP error at all.
"""
import json
import logging
import threading
import time
import zlib

from dotenv import dotenv_values

logger = logging.getLogger(__name__)

BOOK_WEIGHT = 16             # stream tokens per market on the `book` channel
UPGRADE_COST = 32            # stream tokens the websocket upgrade itself spends
HEADROOM = 48                # markets held back under the cap for late openers
RESYNC_MIN_GAP = 2.0         # seconds between subscription syncs on one connection
RESEND_AFTER = 30.0          # re-send a subscribe/snapshot whose reply never came
MAX_READ_KEYS = 5

_lock = threading.RLock()
_order_cv = threading.Condition(_lock)
_books: dict = {}            # marketId -> _Book
_orders: dict = {}           # orderId -> {"events": [...], "state": str, "filled": int, "cost_c": int}
_by_client: dict = {}        # clientId -> orderId
_positions: dict = {}        # outcomeId -> {"marketId", "qty", "cost"}
_private = {"orders_seq": None, "positions_seq": None, "ready": False}
_conns: list = []
_wanted: list = []           # every market the catalog wants watched, priority order


class _Book:
    __slots__ = ("orders", "seq", "lseq", "status", "live", "conn", "fresh", "ts")

    def __init__(self, conn):
        self.orders = {}     # orderId -> [outcomeId, price, qty]
        self.seq = self.lseq = None
        self.status, self.live, self.conn, self.fresh, self.ts = None, None, conn, False, 0.0

    def levels(self) -> dict:
        out: dict = {}
        for oid, price, qty in self.orders.values():
            if qty > 0:
                out.setdefault(oid, []).append((price, qty))
        return out


# ── credentials ───────────────────────────────────────────────────────────────

def _read_keys() -> list:
    """[(key_id, private_key)] for every configured trading::read key."""
    import novig_client as nc
    cfg = dotenv_values(nc._ENV_FILE)
    out = []
    for n in range(1, MAX_READ_KEYS + 1):
        sfx = "" if n == 1 else f"_{n}"
        kid, raw = (cfg.get(f"NOVIG_READ_ID{sfx}") or "").strip(), cfg.get(f"NOVIG_READ_KEY{sfx}") or ""
        if kid and raw.strip():
            try:
                out.append((kid, nc.load_private_key(raw)))
            except Exception as e:
                logger.warning(f"novig-ws: NOVIG_READ_KEY{sfx} unusable ({type(e).__name__})")
    return out


def _env():
    import novig_client as nc
    return (dotenv_values(nc._ENV_FILE).get("NOVIG_ENV") or "paper").strip().lower()


def available() -> bool:
    """Read key present AND pointed at production -- the board's catalog and
    market ids come from production, so a paper connection would match nothing."""
    return _env() == "prod" and bool(_read_keys())


# ── one connection ────────────────────────────────────────────────────────────

class _Conn:
    def __init__(self, idx: int, key_id: str, key, private: bool):
        self.idx, self.key_id, self._key, self.private = idx, key_id, key, private
        self.cap_markets = 2048 - HEADROOM
        self.bucket_cap, self.refill = 512.0, 4.0
        self.tokens, self.tokens_ts = 0.0, time.time()
        self.assigned: list = []       # markets this connection should watch
        self.subscribed: set = set()   # confirmed by a snapshot
        self.requested: dict = {}      # marketId -> when the subscribe went out
        self.resnap: dict = {}         # marketId with a seq gap -> when its snapshot went out (0 = not yet)
        self.private_on = False        # orders/positions snapshot received
        self.private_sent_at = 0.0
        self.last_request = None       # (kind, ids) of the newest request, undone on a 429
        self.ws = None
        self.connected = False
        self.gen = 0                   # bumps per connection, so a stale sync loop exits
        self.nonce = 0
        self._send_lock = threading.Lock()
        self.last_msg = 0.0
        self.last_sync = 0.0
        self.errors: dict = {}
        self.thread = threading.Thread(target=self._run, daemon=True, name=f"novig-ws-{idx}")
        self._wake = threading.Event()

    # token bucket, modelled client-side (the server is the authority; 429s reset it)
    def _avail(self) -> float:
        now = time.time()
        self.tokens = min(self.bucket_cap, self.tokens + (now - self.tokens_ts) * self.refill)
        self.tokens_ts = now
        return self.tokens

    def _spend(self, cost: float) -> bool:
        """Spend if the server would accept it. A request costing more than the
        bucket passes only when the bucket is FULL, and then empties it."""
        have = self._avail()
        if cost > self.bucket_cap:
            if have < self.bucket_cap - 0.5:
                return False
            self.tokens = 0.0
            return True
        if have < cost:
            return False
        self.tokens -= cost
        return True

    def _send(self, payload: dict, kind: str = "", ids=()) -> int:
        """Nonces must rise strictly per connection, and two threads send here
        (the sync loop and the socket thread), so number and send under one lock."""
        with self._send_lock:
            self.nonce += 1
            self.ws.send(json.dumps({"nonce": self.nonce, **payload}, separators=(",", ":")))
            self.last_request = (kind, tuple(ids))
            return self.nonce

    def _headers(self) -> list:
        import novig_client as nc
        ts = int(time.time() * 1000)
        sig = nc.sign(self._key, nc.string_to_sign(ts, "GET", "/v3/ws", "", b""))
        return [f"Novig-Key-Id: {self.key_id}", f"Novig-Timestamp: {ts}",
                f"Novig-Signature: {sig}", "X-Novig-WS-Compress: deflate"]

    def _limits(self) -> None:
        """Read this key's real caps instead of trusting the docs' numbers."""
        import novig_client as nc
        try:
            c = nc.NovigClient(key_id=self.key_id, key=self._key, env="prod")
            lim = c.request("GET", "/v3/limits")
            st = lim.get("stream") or {}
            self.bucket_cap = float(st.get("capacity") or 512)
            self.refill = float(st.get("refillPerSec") or 4)
            self.cap_markets = max(100, int(lim.get("maxWatchedMarkets") or 2048) - HEADROOM)
        except Exception as e:
            logger.warning(f"novig-ws[{self.idx}]: /v3/limits failed ({e}); using defaults")

    # lifecycle
    def _run(self) -> None:
        import websocket
        self._limits()
        # Assume the server's bucket is full at startup -- it is unless this key
        # subscribed in the last ~2 min. If not, the first big subscribe gets a
        # 429, the model drops to 0 and it waits out one refill. Starting at 0
        # instead would cost every app start a 2-minute blind wait.
        self.tokens, self.tokens_ts = self.bucket_cap, time.time()
        backoff = 2.0
        while True:
            try:
                self.ws = websocket.WebSocketApp(
                    "wss://api.novig.com/v3/ws", header=self._headers(),
                    on_open=self._on_open, on_message=self._on_message,
                    on_error=self._on_error, on_close=self._on_close)
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                logger.warning(f"novig-ws[{self.idx}]: {type(e).__name__}: {e}")
            self._drop_all()
            time.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
            if self.last_msg and time.time() - self.last_msg < 120:
                backoff = 2.0

    def _on_open(self, ws) -> None:
        with _lock:
            self.gen += 1
            self.connected = True
            self.last_msg = time.time()
            self.tokens = max(0.0, self._avail() - UPGRADE_COST)
            self.subscribed, self.requested, self.resnap = set(), {}, {}
            self.private_on, self.private_sent_at, self.nonce = False, 0.0, 0
        logger.info(f"novig-ws[{self.idx}]: connected")
        threading.Thread(target=self._sync_loop, args=(self.gen,), daemon=True,
                         name=f"novig-ws-sync-{self.idx}").start()

    def _on_error(self, ws, e) -> None:
        logger.warning(f"novig-ws[{self.idx}]: {type(e).__name__}: {e}")

    def _on_close(self, ws, code, reason) -> None:
        self.connected = False
        logger.info(f"novig-ws[{self.idx}]: closed {code} {reason or ''}".rstrip())
        if reason and "GEOLOCATION" in str(reason):
            self.errors["geolocation"] = str(reason)

    def _drop_all(self) -> None:
        self.connected = False
        with _lock:
            for b in _books.values():
                if b.conn is self:
                    b.fresh = False
            if self.private:
                _private["ready"] = False

    # subscriptions
    def _sync_loop(self, gen: int) -> None:
        """Keep the subscription equal to `assigned`, inside the token budget.
        Exits when its connection is gone or a newer connection replaced it."""
        while self.connected and gen == self.gen:
            try:
                self._sync_once()
            except Exception as e:
                logger.warning(f"novig-ws[{self.idx}]: sync: {type(e).__name__}: {e}")
            self._wake.wait(timeout=5.0)
            self._wake.clear()

    def _sync_once(self) -> None:
        now = time.time()
        if not self.connected or now - self.last_sync < RESYNC_MIN_GAP:
            return
        # Decide under the lock (the socket thread edits these sets), send outside it.
        with _lock:
            want = set(self.assigned)
            drop = sorted(self.subscribed - want)
            resnap = sorted(m for m, t in self.resnap.items()
                            if m in self.subscribed and now - t > RESEND_AFTER)
            add = [m for m in self.assigned if m not in self.subscribed
                   and now - self.requested.get(m, 0.0) > RESEND_AFTER]
            need_private = (self.private and not self.private_on
                            and now - self.private_sent_at > RESEND_AFTER)
        if need_private and self._spend(2):
            self._send({"subscribe": {"private": ["orders", "positions"]}}, "private")
            self.private_sent_at = now
        if drop and self._spend(len(drop)):
            self._send({"unsubscribe": [f"market:{m}" for m in drop]}, "unsubscribe", drop)
            with _lock:
                self.subscribed -= set(drop)
                for m in drop:
                    b = _books.get(m)
                    if b is not None and b.conn is self:
                        b.fresh = False
        if resnap and self._spend(BOOK_WEIGHT * len(resnap)):
            self._send({"snapshot": {"markets": {m: "book" for m in resnap}}}, "resnap", resnap)
            with _lock:
                for m in resnap:
                    self.resnap[m] = now
        if add and self._spend(BOOK_WEIGHT * len(add)):
            self._send({"subscribe": {"markets": {m: "book" for m in add}}}, "subscribe", add)
            with _lock:
                for m in add:
                    self.requested[m] = now
        self.last_sync = time.time()

    def _undo_last(self) -> None:
        """A 429 carries no nonce, but sends are serialized and the throttle
        answers at once, so the newest request is the one refused: clear its
        bookkeeping so the next sync sends it again."""
        kind, ids = self.last_request or ("", ())
        with _lock:
            if kind == "subscribe":
                for m in ids:
                    self.requested.pop(m, None)
            elif kind == "resnap":
                for m in ids:
                    if m in self.resnap:
                        self.resnap[m] = 0.0
            elif kind == "private":
                self.private_sent_at = 0.0

    # messages
    def _on_message(self, ws, msg) -> None:
        self.last_msg = time.time()
        if isinstance(msg, (bytes, bytearray)):
            msg = zlib.decompressobj(-15).decompress(msg)
        try:
            j = json.loads(msg)
        except ValueError:
            return
        code = j.get("code")
        if code:
            self.errors[code] = self.errors.get(code, 0) + 1
            if code == "RATE_LIMIT_EXCEEDED":
                self.tokens = 0.0                       # the server says empty; believe it
                self._undo_last()
            elif code == "SUBSCRIPTION_LIMIT_EXCEEDED":
                self.cap_markets = max(100, int(self.cap_markets * 0.9))
                self._undo_last()
                _rebalance()
            else:
                logger.warning(f"novig-ws[{self.idx}]: {code}: {j.get('message', '')[:120]}")
            return
        snap = j.get("snapshot")
        if snap:
            self._apply_snapshot(snap)
        delta = j.get("delta")
        if delta:
            self._apply_delta(delta)
        if self.private:
            self._apply_private(j)

    def _apply_snapshot(self, snap: dict) -> None:
        now = time.time()
        with _lock:
            for mid, d in snap.items():
                b = _books.get(mid)
                if b is None or b.conn is not self:
                    b = _books[mid] = _Book(self)
                book = d.get("book") or {}
                b.orders = {o["orderId"]: [oid, float(o["price"]), int(o["qty"])]
                            for oid, lst in (book.get("orders") or {}).items() for o in lst or []
                            if o.get("orderId")}
                b.seq = book.get("seq")
                lc = d.get("lifecycle") or {}
                b.lseq, b.status = lc.get("seq"), lc.get("status") or b.status
                b.fresh, b.ts = True, now
                self.subscribed.add(mid)
                self.requested.pop(mid, None)
                self.resnap.pop(mid, None)

    def _apply_delta(self, delta: dict) -> None:
        now = time.time()
        with _lock:
            for mid, d in delta.items():
                b = _books.get(mid)
                if b is None or b.conn is not self:
                    continue
                book = d.get("book")
                if book is not None:
                    seq = book.get("seq")
                    if b.seq is not None and seq is not None and seq != b.seq + 1:
                        if seq > b.seq + 1:                    # missed a message
                            b.fresh = False
                            self.resnap.setdefault(mid, 0.0)
                            self._wake.set()
                        continue                               # older than our state: ignore
                    for x in book.get("deltas") or []:
                        kind = x.get("kind")
                        if kind == "add" and x.get("orderId"):
                            b.orders[x["orderId"]] = [x.get("outcomeId"), float(x["price"]), int(x["qty"])]
                        elif kind == "remove":
                            b.orders.pop(x.get("orderId"), None)
                        elif kind == "update":                 # undocumented: partial fill in place
                            o = b.orders.get(x.get("orderId"))
                            if o is not None and x.get("remaining") is not None:
                                o[2] = int(x["remaining"])
                    b.seq, b.ts = seq, now
                lc = d.get("lifecycle")
                if lc is not None:
                    for t in lc.get("deltas") or []:
                        if t == "GOLIVE":
                            b.live = True
                        elif t == "UNLIVE":
                            b.live = False
                        elif t in ("OPEN", "CLOSE", "GRADE"):
                            b.status = {"OPEN": "OPEN", "CLOSE": "CLOSED", "GRADE": "SETTLED"}[t]
                    b.lseq = lc.get("seq", b.lseq)

    def _apply_private(self, j: dict) -> None:
        o, p, hb = j.get("orders"), j.get("positions"), j.get("heartbeat")
        with _order_cv:
            if o is not None:
                if "open" in o:                                  # snapshot
                    for row in o.get("open") or []:
                        _note_order(row.get("orderId"), dict(row, kind="open"))
                    _private["orders_seq"] = o.get("seq")
                    self.private_on = True
                else:
                    seq, last = o.get("seq"), _private["orders_seq"]
                    if last is not None and seq is not None and seq > last + 1:
                        self._resnap_private()
                    for ev in o.get("deltas") or []:
                        _note_order(ev.get("orderId"), ev)
                    _private["orders_seq"] = seq
                _private["ready"] = True
                _order_cv.notify_all()
            if p is not None:
                rows = p.get("positions") if "positions" in p else p.get("deltas")
                for row in rows or []:
                    if row.get("outcomeId"):
                        if int(row.get("qty") or 0) == 0:
                            _positions.pop(row["outcomeId"], None)
                        else:
                            _positions[row["outcomeId"]] = row
                _private["positions_seq"] = p.get("seq")
            if hb and _private["orders_seq"] is not None and (hb.get("orders") or 0) > _private["orders_seq"]:
                self._resnap_private()

    def _resnap_private(self) -> None:
        try:
            self._send({"snapshot": {"private": ["orders", "positions"]}})
        except Exception:
            pass


def _note_order(order_id, ev: dict) -> None:
    """Fold one private order event into _orders. Caller holds _lock."""
    if not order_id:
        return
    o = _orders.setdefault(order_id, {"events": [], "state": "PENDING", "filled": 0, "cost_c": 0.0})
    o["events"].append(dict(ev, _ts=time.time()))
    if ev.get("clientId"):
        _by_client[ev["clientId"]] = order_id
        o["clientId"] = ev["clientId"]
    kind = ev.get("kind")
    if kind == "fill":
        q = int(ev.get("qty") or 0)
        o["filled"] += q
        o["cost_c"] += q * float(ev.get("price") or 0)            # contracts x price, in cents
        if int(ev.get("remaining") or 0) == 0:
            o["state"] = "FILLED"
    elif kind == "open" and o["state"] == "PENDING":
        o["state"] = "OPEN"
    elif kind == "cancel":
        o["state"] = "CANCELED"
        o["reason"] = ev.get("reason")
    elif kind == "reject":
        o["state"] = "REJECTED"


# ── manager ───────────────────────────────────────────────────────────────────

def _ensure_conns() -> None:
    if _conns or not available():
        return
    for i, (kid, key) in enumerate(_read_keys()):
        c = _Conn(i, kid, key, private=(i == 0))
        _conns.append(c)
        c.thread.start()
    logger.info(f"novig-ws: {len(_conns)} connection(s) starting")


def _rebalance() -> None:
    """Deal the wanted markets out to connections, highest priority first."""
    with _lock:
        rest = list(_wanted)
        for c in _conns:
            c.assigned, rest = rest[:c.cap_markets], rest[c.cap_markets:]
    for c in _conns:
        c._wake.set()


def start() -> None:
    """Open the connection(s) without changing what they watch -- enough for the
    private orders stream that novig_orders waits on."""
    if available():
        _ensure_conns()


def watch(market_ids) -> None:
    """Called by novig_catalog on every build with every game-line market id it
    can use, MOST IMPORTANT FIRST. Starts the connections on first call."""
    global _wanted
    if not available():
        return
    _ensure_conns()
    ids = list(dict.fromkeys(market_ids))
    with _lock:
        changed = ids != _wanted
        _wanted = ids
    if changed:
        _rebalance()


def levels(market_id: str):
    """{outcomeId: [(price, qty), ...]} of resting bids, or None if this market
    isn't streaming right now (not watched, disconnected, or mid-gap)."""
    with _lock:
        b = _books.get(market_id)
        if b is None or not b.fresh or not (b.conn and b.conn.connected):
            return None
        return b.levels()


def fresh(market_id: str) -> bool:
    with _lock:
        b = _books.get(market_id)
        return bool(b and b.fresh and b.conn and b.conn.connected)


def market_live(market_id: str):
    """True/False once a GOLIVE/UNLIVE has streamed for this market, else None."""
    with _lock:
        b = _books.get(market_id)
        return b.live if b is not None else None


def market_status(market_id: str):
    with _lock:
        b = _books.get(market_id)
        return b.status if b is not None else None


# ── private stream (used by novig_orders) ────────────────────────────────────

def private_ready() -> bool:
    with _lock:
        return bool(_private["ready"] and _conns and _conns[0].connected)


def order(order_id: str):
    with _lock:
        o = _orders.get(order_id)
        return json.loads(json.dumps(o)) if o else None


def order_by_client(client_id: str):
    with _lock:
        oid = _by_client.get(client_id)
    return (oid, order(oid)) if oid else (None, None)


def wait_order(order_id: str, timeout: float = 8.0):
    """Block until the order is terminal (FILLED / CANCELED / REJECTED) or the
    timeout passes. Returns the order dict, or None if nothing arrived."""
    end = time.time() + timeout
    with _order_cv:
        while True:
            o = _orders.get(order_id)
            if o and o["state"] in ("FILLED", "CANCELED", "REJECTED"):
                return json.loads(json.dumps(o))
            left = end - time.time()
            if left <= 0:
                return json.loads(json.dumps(o)) if o else None
            _order_cv.wait(timeout=left)


def positions() -> dict:
    with _lock:
        return json.loads(json.dumps(_positions))


def status() -> dict:
    with _lock:
        now = time.time()
        conns = [{"idx": c.idx, "connected": c.connected, "assigned": len(c.assigned),
                  "streaming": len(c.subscribed), "requested": len(c.requested),
                  "gaps_pending": len(c.resnap), "cap": c.cap_markets,
                  "tokens": round(c._avail(), 1), "refill_per_s": c.refill,
                  "last_msg_s": round(now - c.last_msg, 1) if c.last_msg else None,
                  "errors": dict(c.errors)} for c in _conns]
        live = sum(1 for b in _books.values() if b.fresh and b.conn and b.conn.connected)
        return {"available": available(), "wanted": len(_wanted), "streaming_markets": live,
                "read_keys": len(_conns), "private_ready": bool(_private["ready"]),
                "orders_seq": _private["orders_seq"], "connections": conns}
