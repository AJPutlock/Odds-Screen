"""
Signed NoVig v3 API client (NOVIG-V3 request signing).

The public catalog/book reads in scrapers/novig_catalog.py need no key. This is
for everything that does: account/subaccount management and, later, orders.

Signing (docs.novig.com/api/signing), verified 30/30 byte-identical against
NoVig's published test vectors (see check_signing_vectors()):

    string_to_sign = "\n".join([
        "NOVIG-V3", "<unix ms>", "<METHOD>", "<path>",
        "<canonical query>", "<lowercase hex sha256 of the exact body bytes>"])

    headers: Novig-Key-Id, Novig-Timestamp (same ms), Novig-Signature
             (standard padded base64 of the Ed25519 / P-256-DER signature)

Two things that silently break a signature if they drift:
  * the query is signed in CANONICAL form but sent as-is, so this module builds
    the query string itself and never lets `requests` re-encode params;
  * the body hash covers the exact bytes sent, and a POST without
    Content-Type: application/json is hashed by the server as ZERO bytes.

Keys come only from .env and are never logged or echoed. A key value may be the
.pem path, an inline PEM, or just the base64 body line of the PEM (which is how
NOVIG_API_MANAGE_KEY is stored).

    NOVIG_ENV=paper|prod          which exchange (default paper)
    NOVIG_API_MANAGE_ID / _KEY    management key: opens/funds subaccounts, mints
                                  keys, CANNOT place orders. Keep it out of any
                                  long-running process once setup is done.
    NOVIG_TRADE_ID / _KEY         one subaccount's trading key: places/cancels
                                  orders, CANNOT move money.
"""
import base64
import hashlib
import json
import os
import time
from urllib.parse import unquote_to_bytes

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519
from dotenv import dotenv_values

HOSTS = {"paper": "https://api.paper.novig.com", "prod": "https://api.novig.com"}
SCHEME = "NOVIG-V3"
_ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
_SCOPES = {"manage": ("NOVIG_API_MANAGE_ID", "NOVIG_API_MANAGE_KEY"),
           "trade": ("NOVIG_TRADE_ID", "NOVIG_TRADE_KEY")}


class NovigError(Exception):
    """A non-2xx answer. `code` is NoVig's machine code (branch on it, not the text)."""

    def __init__(self, status, code, message, body=None):
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code, self.body = status, code, body


# ── signing ────────────────────────────────────────────────────────────────────

_UNRESERVED = frozenset(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")


def _pct(raw: bytes) -> str:
    return "".join(chr(b) if b in _UNRESERVED else f"%{b:02X}" for b in raw)


def canonical_query(query: str) -> str:
    """Split on &, split each pair at the FIRST =, URI-decode (+ stays +),
    re-encode everything but A-Za-z0-9-._~, sort bytewise by name then value."""
    if not query:
        return ""
    pairs = []
    for tok in query.split("&"):
        name, _, val = tok.partition("=")
        pairs.append((_pct(unquote_to_bytes(name)), _pct(unquote_to_bytes(val))))
    pairs.sort(key=lambda p: (p[0].encode(), p[1].encode()))
    return "&".join(f"{n}={v}" for n, v in pairs)


def string_to_sign(ts_ms: int, method: str, path: str, query: str, body: bytes) -> str:
    return "\n".join([SCHEME, str(ts_ms), method.upper(), path,
                      canonical_query(query), hashlib.sha256(body).hexdigest()])


def sign(key, sts: str) -> str:
    msg = sts.encode()
    if isinstance(key, ed25519.Ed25519PrivateKey):
        sig = key.sign(msg)
    elif isinstance(key, ec.EllipticCurvePrivateKey):
        sig = key.sign(msg, ec.ECDSA(hashes.SHA256()))       # DER, as NoVig requires
    else:
        raise TypeError(f"unsupported key type {type(key).__name__}")
    return base64.b64encode(sig).decode()


def load_private_key(value: str):
    """A .pem path, an inline PEM, or the bare base64 body of a PKCS#8 PEM."""
    v = (value or "").strip().strip('"').strip("'")
    if not v:
        raise ValueError("empty key")
    if "BEGIN" not in v and os.path.isfile(v):
        with open(v, "rb") as f:
            return serialization.load_pem_private_key(f.read(), password=None)
    if "BEGIN" in v:
        return serialization.load_pem_private_key(v.replace("\\n", "\n").encode(), password=None)
    return serialization.load_der_private_key(base64.b64decode(v), password=None)


def public_key_pem(key) -> str:
    return key.public_key().public_bytes(serialization.Encoding.PEM,
                                         serialization.PublicFormat.SubjectPublicKeyInfo).decode()


def fingerprint(key) -> str:
    """SHA-256 of the public key's SPKI DER — what NoVig reports. Not a secret."""
    der = key.public_key().public_bytes(serialization.Encoding.DER,
                                        serialization.PublicFormat.SubjectPublicKeyInfo)
    return hashlib.sha256(der).hexdigest()


def algorithm(key) -> str:
    return "Ed25519" if isinstance(key, ed25519.Ed25519PrivateKey) else "P-256"


# ── client ───────────────────────────────────────────────────────────────────

class NovigClient:
    def __init__(self, scope: str = "manage", env: str | None = None,
                 key_id: str | None = None, key=None):
        cfg = dotenv_values(_ENV_FILE)
        self.env = (env or cfg.get("NOVIG_ENV") or "paper").strip().lower()
        if self.env not in HOSTS:
            raise ValueError(f"NOVIG_ENV must be paper or prod, not {self.env!r}")
        self.host = HOSTS[self.env]
        if key is None:
            id_var, key_var = _SCOPES[scope]
            key_id, raw = (cfg.get(id_var) or "").strip(), cfg.get(key_var) or ""
            if not key_id or not raw.strip():
                raise RuntimeError(f"{id_var} / {key_var} not set in .env")
            key = load_private_key(raw)
        self.key_id, self._key, self.scope = key_id, key, scope
        self._http = requests.Session()

    def request(self, method: str, path: str, query: str = "", body=None, timeout: float = 20):
        """Signed call. `query` is the raw query string, sent exactly as signed.
        `body` is a dict (JSON-encoded once, and those exact bytes are signed)."""
        raw = b"" if body is None else json.dumps(body, separators=(",", ":")).encode()
        ts = int(time.time() * 1000)
        headers = {"Novig-Key-Id": self.key_id, "Novig-Timestamp": str(ts),
                   "Novig-Signature": sign(self._key, string_to_sign(ts, method, path, query, raw))}
        if body is not None:
            headers["Content-Type"] = "application/json"
        url = f"{self.host}{path}" + (f"?{query}" if query else "")
        r = self._http.request(method.upper(), url, data=raw if body is not None else None,
                               headers=headers, timeout=timeout)
        if r.status_code >= 400:
            try:
                j = r.json()
            except ValueError:
                j = {}
            raise NovigError(r.status_code, j.get("code") or "HTTP", j.get("message") or r.text[:200], j)
        if r.status_code == 204 or not r.content:
            return None
        try:
            return r.json()
        except ValueError:
            return r.text

    # convenience
    def echo(self, payload=None):
        return self.request("POST", "/v3/echo", body=payload or {"ping": "novig_client"})

    def subaccounts(self):
        return self.request("GET", "/v3/account/subaccounts")

    def keys(self):
        return self.request("GET", "/v3/keys")


def check_signing_vectors(path: str) -> tuple[int, int]:
    """Run NoVig's published signing-vectors.json through this module.
    Returns (passed, total). Ed25519 must match byte-for-byte (it is
    deterministic); P-256 is checked by verifying, since ECDSA is randomised."""
    with open(path, encoding="utf-8") as f:
        v = json.load(f)
    ok = 0
    for vec in v["vectors"]:
        i = vec["input"]
        sts = string_to_sign(i["timestamp"], i["method"], i["path"], i["query"], i["body"].encode())
        if sts != vec["string_to_sign"]:
            continue
        kp = v["keypairs"][vec["keypair_id"]]
        priv = serialization.load_pem_private_key(kp["private_key_pkcs8_pem"].encode(), password=None)
        pub = serialization.load_pem_public_key(kp["public_key_spki_pem"].encode())
        raw = base64.b64decode(vec["signature"])
        try:
            if vec["algorithm"] == "ed25519":
                pub.verify(raw, sts.encode())
                if sign(priv, sts) != vec["signature"]:
                    continue
            else:
                pub.verify(raw, sts.encode(), ec.ECDSA(hashes.SHA256()))
                pub.verify(base64.b64decode(sign(priv, sts)), sts.encode(), ec.ECDSA(hashes.SHA256()))
            ok += 1
        except Exception:
            continue
    return ok, len(v["vectors"])
