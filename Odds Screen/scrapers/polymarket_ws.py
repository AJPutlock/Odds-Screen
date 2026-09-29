"""
Polymarket US live order books over the authenticated markets WebSocket
(wss://api.polymarket.us/v1/ws/markets), so liquidity is known for EVERY
Polymarket price on the board instead of the few the public REST endpoint's
~18-books-a-minute limit allows.

Credentials come only from the environment (.env, loaded by history_tracker):
    POLYMARKET_KEY_ID      the key's id
    POLYMARKET_SECRET_KEY  the base64 Ed25519 secret
Without both, nothing starts and the REST path in polymarket.depth() is used.
The key is only ever used to subscribe to market data — Polymarket US keys
carry no read-only scope, so this module never calls a trading endpoint.

Handshake (docs.polymarket.us/api-reference/authentication): headers
X-PM-Access-Key, X-PM-Timestamp (ms), X-PM-Signature = base64 Ed25519
signature of f"{timestamp}GET{path}". Subscriptions take at most 100 market
slugs each and a connection at most 10 subscriptions ("max subscriptions per
connection reached", 2026-09-28), so markets are spread over as many
connections as needed, 1,000 each. Full-book updates arrive as {marketSlug,
bids, offers}.
"""
import base64
import json
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

WS_URL = "wss://api.polymarket.us/v1/ws/markets"
WS_PATH = "/v1/ws/markets"
MAX_PER_SUB = 100
MAX_SUBS_PER_CONN = 10
MAX_PER_CONN = MAX_PER_SUB * MAX_SUBS_PER_CONN
BOOK_MAX_AGE = 300          # seconds a streamed book stays usable without an update

_books: dict = {}           # slug -> (epoch, {"bids": [...], "offers": [...]})
_lock = threading.Lock()
_conns: list = []           # one _Conn per 1,000 markets
_assigned: set = set()      # slugs owned by some connection


def credentials():
    kid = os.environ.get("POLYMARKET_KEY_ID", "").strip()
    secret = os.environ.get("POLYMARKET_SECRET_KEY", "").strip()
    return (kid, secret) if kid and secret else None


def available() -> bool:
    return credentials() is not None


def _auth_headers() -> list:
    from cryptography.hazmat.primitives.asymmetric import ed25519
    kid, secret = credentials()
    raw = base64.b64decode(secret)
    key = ed25519.Ed25519PrivateKey.from_private_bytes(raw[:32])   # 64-byte secrets = seed + public key
    ts = str(int(time.time() * 1000))
    sig = base64.b64encode(key.sign(f"{ts}GET{WS_PATH}".encode())).decode()
    return [f"X-PM-Access-Key: {kid}", f"X-PM-Timestamp: {ts}", f"X-PM-Signature: {sig}"]


def book(slug: str):
    """The streamed order book for a slug, or None if not (freshly) streamed."""
    with _lock:
        hit = _books.get(slug)
    if hit and time.time() - hit[0] < BOOK_MAX_AGE:
        return hit[1]
    return None


def status() -> dict:
    with _lock:
        n = len(_books)
    errors = [c.error for c in _conns if c.error]
    return {"available": available(), "connections": len(_conns),
            "connected": sum(1 for c in _conns if c.connected), "books": n,
            "wanted": len(_assigned), "error": errors[0] if errors else None}


def watch(slugs) -> None:
    """Stream these markets' books (adds to what's already wanted), opening
    another connection whenever the open ones are full."""
    if not available():
        return
    new = sorted(set(slugs) - _assigned)
    while new:
        # The cap is 10 SUBSCRIPTIONS per connection, not 1,000 markets — a
        # later batch starts a new subscription even if the last one isn't full.
        conn = next((c for c in _conns if c.subs_used < MAX_SUBS_PER_CONN), None)
        if conn is None:
            conn = _Conn(len(_conns))
            _conns.append(conn)
        room = (MAX_SUBS_PER_CONN - conn.subs_used) * MAX_PER_SUB
        take, new = new[:room], new[room:]
        _assigned.update(take)
        conn.add(take)


def _find_books(obj, out: list) -> None:
    """Collect every {marketSlug, bids|offers} dict in a message (shape-tolerant)."""
    if isinstance(obj, dict):
        if "marketSlug" in obj and ("bids" in obj or "offers" in obj):
            out.append(obj)
        for v in obj.values():
            _find_books(v, out)
    elif isinstance(obj, list):
        for v in obj:
            _find_books(v, out)


class _Conn:
    """One WebSocket connection carrying up to MAX_PER_CONN markets."""

    def __init__(self, idx: int):
        self.idx, self.slugs, self.sent, self.subs_used = idx, [], set(), 0
        self.ws, self.connected, self.error, self.req = None, False, None, 0
        self.thread = threading.Thread(target=self._run, daemon=True, name=f"polymarket-ws-{idx}")
        self.thread.start()

    def add(self, slugs: list) -> None:
        self.slugs += slugs
        self.subs_used += -(-len(slugs) // MAX_PER_SUB)
        if self.connected:
            self._subscribe(slugs)

    def _subscribe(self, slugs: list) -> None:
        for i in range(0, len(slugs), MAX_PER_SUB):
            chunk = [s for s in slugs[i:i + MAX_PER_SUB] if s not in self.sent]
            if not chunk or self.ws is None:
                continue
            self.req += 1
            self.ws.send(json.dumps({"subscribe": {
                "requestId": f"md-{self.idx}-{self.req}",
                "subscriptionType": "SUBSCRIPTION_TYPE_MARKET_DATA",
                "marketSlugs": chunk,
                "responsesDebounced": True,
            }}))
            self.sent.update(chunk)

    def _on_message(self, ws, msg) -> None:
        try:
            data = json.loads(msg)
        except ValueError:
            return
        found = []
        _find_books(data, found)
        now = time.time()
        with _lock:
            for b in found:
                _books[b["marketSlug"]] = (now, {"bids": b.get("bids") or [], "offers": b.get("offers") or []})
        if not found and isinstance(data, dict) and ("error" in data or "errors" in data):
            self.error = str(data)[:300]
            logger.warning(f"polymarket ws[{self.idx}]: {self.error}")

    def _on_open(self, ws) -> None:
        self.connected, self.error = True, None
        self.sent.clear()
        self.subs_used = -(-len(self.slugs) // MAX_PER_SUB)   # a fresh connection packs full subscriptions
        logger.info(f"polymarket ws[{self.idx}]: connected, subscribing {len(self.slugs)} markets")
        self._subscribe(list(self.slugs))

    def _on_close(self, ws, code, reason) -> None:
        self.connected = False
        logger.info(f"polymarket ws[{self.idx}]: closed ({code} {reason})")

    def _on_error(self, ws, err) -> None:
        self.error = f"{type(err).__name__}: {err}"[:300]
        logger.warning(f"polymarket ws[{self.idx}]: {self.error}")

    def _run(self) -> None:
        import websocket
        delay = 5
        while available():
            try:
                self.ws = websocket.WebSocketApp(WS_URL, header=_auth_headers(), on_open=self._on_open,
                                                 on_message=self._on_message, on_close=self._on_close,
                                                 on_error=self._on_error)
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                self.error = f"{type(e).__name__}: {e}"[:300]
                logger.warning(f"polymarket ws[{self.idx}]: {self.error}")
            self.connected, self.ws = False, None
            time.sleep(delay)
            delay = min(delay * 2, 300)
