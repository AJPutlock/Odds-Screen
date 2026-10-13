"""
One-time NoVig API setup. Run each step yourself, in order:

    python novig_setup.py status             # read-only: what exists, balances
    python novig_setup.py open-subaccount    # makes the trading wallet + its key
    python novig_setup.py read-key           # read-only key for the websocket feed
    python novig_setup.py fund 50            # cash wallet -> trading wallet ($)
    python novig_setup.py defund 50          # trading wallet -> cash wallet ($)

Why the steps exist: on NoVig no key can both move money and place orders.
Your management key (NOVIG_API_MANAGE_ID/_KEY, made on the website) can open
and fund subaccounts but can never trade. Each subaccount is its own wallet
with one `trading` key that can trade but can never move money out. The odds
screen only ever needs the trading key, so a leak of it risks that one wallet's
balance and nothing else.

Every private key is generated on this machine and written straight into .env
(gitignored). Only the PUBLIC half is sent to NoVig, and nothing here prints a
private key. Uses NOVIG_ENV from .env (paper|prod).
"""
import argparse
import base64
import os
import re
import sys
import time
from decimal import Decimal, InvalidOperation

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
from dotenv import dotenv_values, set_key, unset_key

import novig_client as nc

ENV_FILE = nc._ENV_FILE


def _env() -> dict:
    return dotenv_values(ENV_FILE)


def _write(var: str, value: str) -> None:
    set_key(ENV_FILE, var, value, quote_mode="never")


def _new_key():
    """Fresh Ed25519 keypair -> (key object, private base64 body, public PEM)."""
    key = ed25519.Ed25519PrivateKey.generate()
    der = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption())
    return key, base64.b64encode(der).decode(), nc.public_key_pem(key)


def _manage(env):
    return nc.NovigClient("manage", env=env)


_ACTIVE_ENV = "paper"     # set in main() once --env / NOVIG_ENV is resolved


def _fail(e: Exception) -> None:
    if isinstance(e, nc.NovigError):
        if e.code == "SIGNATURE_REJECTED" and "not found" in str(e).lower():
            other = "prod" if _ACTIVE_ENV == "paper" else "paper"
            site = "novig.com" if other == "prod" else "paper.novig.com"
            sys.exit(f"NoVig {_ACTIVE_ENV} has never heard of this key id.\n"
                     f"  -> keys don't cross environments. If the key was made on {site},\n"
                     f"     add NOVIG_ENV={other} to .env, or pass --env {other}.")
        hint = {
            "SIGNATURE_REJECTED": "key id/key mismatch, a revoked key, or the PC clock is off by >30 s",
            "KYC_REQUIRED": "finish identity verification on novig.com first",
            "ANONYMIZED_NETWORK": "turn off any VPN/proxy on this PC",
        }.get(e.code, "")
        if e.status == 451 and not hint:
            hint = "location check: open the NoVig app on your phone so it geolocates, then retry"
        sys.exit(f"NoVig said {e.status} {e.code}: {e}" + (f"\n  -> {hint}" if hint else ""))
    sys.exit(f"{type(e).__name__}: {e}")


# ── commands ──────────────────────────────────────────────────────────────────

def cmd_status(a):
    c = _manage(a.env)
    print(f"environment : {c.env} ({c.host})")
    c.echo({"ping": "status"})
    print("management  : OK (signed echo accepted)")
    print(f"cash wallet : ${Decimal(c.request('GET', '/v3/account/balance')['balance']):,.2f}")
    # NoVig lists one row PER KEY on a wallet (a trading::read key shows up as a
    # second row with the same balance), so count wallets by their trading key.
    keys = c.keys() or []
    scope_of = {k.get("keyId"): k.get("scope") for k in keys}
    subs = c.subaccounts() or []
    trade_id = (_env().get("NOVIG_TRADE_ID") or "").strip()
    wallets = [s for s in subs if scope_of.get(s.get("keyId")) == "trading"]
    if not subs:
        print("subaccounts : none yet -> run: python novig_setup.py open-subaccount")
    for s in wallets:
        mine = "  <- NOVIG_TRADE_ID" if s.get("keyId") == trade_id else ""
        print(f"wallet      : {s.get('label')!r:16} ${Decimal(s.get('balance') or 0):,.2f}  "
              f"id {s.get('keyId')}{mine}")
    for k in keys:
        print(f"key         : {k.get('scope'):18} {k.get('name')!r:18} last used {k.get('lastUsedAt') or 'never'}")
    if trade_id:
        try:
            t = nc.NovigClient("trade", env=a.env)
            t.echo({"ping": "status"})
            print("trading key : OK (signed echo accepted)")
        except Exception as e:
            print(f"trading key : FAILED - {e}")


def cmd_open_subaccount(a):
    cfg = _env()
    if (cfg.get("NOVIG_TRADE_ID") or "").strip():
        sys.exit("NOVIG_TRADE_ID is already set in .env. Opening another subaccount would leave\n"
                 "that one's key orphaned. Remove both NOVIG_TRADE_* lines first if you mean it.")
    c = _manage(a.env)
    key, priv_b64, pub_pem = _new_key()
    # Private half goes into .env BEFORE the account exists, so a crash after the
    # POST can never leave a registered key whose private half was thrown away.
    _write("NOVIG_TRADE_KEY", priv_b64)
    try:
        r = c.request("POST", "/v3/account/subaccounts",
                      body={"label": a.label, "publicKey": pub_pem, "algorithm": "Ed25519"})
    except Exception as e:
        unset_key(ENV_FILE, "NOVIG_TRADE_KEY")
        _fail(e)
    _write("NOVIG_TRADE_ID", r["keyId"])
    print(f"opened subaccount {r.get('label')!r} (id {r['keyId']}), balance ${Decimal(r.get('balance') or 0):,.2f}")
    print(f"trading key fingerprint {nc.fingerprint(key)}")
    print("wrote NOVIG_TRADE_ID and NOVIG_TRADE_KEY to .env")
    try:
        nc.NovigClient("trade", env=a.env).echo({"ping": "new-trading-key"})
        print("trading key verified: signed echo accepted")
    except Exception as e:
        print(f"WARNING: the new trading key did not verify yet ({e}). NoVig can take up to ~60 s;"
              f" re-run `status` in a minute.")
    print("\nnext: python novig_setup.py fund <dollars>")


def cmd_read_key(a):
    trade_id = (_env().get("NOVIG_TRADE_ID") or "").strip()
    if not trade_id:
        sys.exit("open the subaccount first: python novig_setup.py open-subaccount")
    if (_env().get("NOVIG_READ_ID") or "").strip():
        sys.exit("NOVIG_READ_ID is already set in .env.")
    c = _manage(a.env)
    key, priv_b64, pub_pem = _new_key()
    _write("NOVIG_READ_KEY", priv_b64)
    try:
        r = c.request("POST", f"/v3/account/subaccounts/{trade_id}/keys",
                      body={"name": a.name, "publicKey": pub_pem, "algorithm": "Ed25519",
                            "scope": "trading::read"})
    except Exception as e:
        unset_key(ENV_FILE, "NOVIG_READ_KEY")
        _fail(e)
    _write("NOVIG_READ_ID", r["keyId"])
    print(f"minted trading::read key {r['keyId']} (fingerprint {r.get('fingerprint')})")
    print("wrote NOVIG_READ_ID and NOVIG_READ_KEY to .env - it can stream books but cannot\n"
          "place orders or move money, and its reads tolerate a stale phone geolocation.")


def _amount(text: str) -> str:
    try:
        d = Decimal(text.replace("$", "").replace(",", ""))
    except InvalidOperation:
        sys.exit(f"not an amount: {text!r}")
    if d <= 0 or d != d.quantize(Decimal("0.00001")):
        sys.exit("amount must be positive with at most 5 decimal places")
    return f"{d:.5f}"


def _transfer(a, direction: str):
    trade_id = (_env().get("NOVIG_TRADE_ID") or "").strip()
    if not trade_id:
        sys.exit("open the subaccount first: python novig_setup.py open-subaccount")
    amount = _amount(a.amount)
    c = _manage(a.env)
    cash = Decimal(c.request("GET", "/v3/account/balance")["balance"])
    sub = Decimal(c.request("GET", f"/v3/account/subaccounts/{trade_id}/balance")["balance"])
    src = cash if direction == "fund" else sub
    if Decimal(amount) > src:
        sys.exit(f"only ${src:,.2f} available to {direction}")
    arrow = "cash wallet -> trading subaccount" if direction == "fund" else "trading subaccount -> cash wallet"
    print(f"{c.env.upper()}: move ${Decimal(amount):,.2f}  ({arrow})")
    print(f"  before: cash ${cash:,.2f} | subaccount ${sub:,.2f}")
    word = direction.upper()
    if input(f"  type {word} to confirm: ").strip() != word:
        sys.exit("cancelled, nothing moved")
    tid = f"odds-screen-{direction}-{int(time.time())}"
    try:
        r = c.request("POST", f"/v3/account/subaccounts/{trade_id}/transfer",
                      body={"direction": direction, "amount": amount, "clientTransferId": tid})
    except Exception as e:
        _fail(e)
    status = r.get("status")
    for _ in range(15):
        if status in ("Applied", "Rejected"):
            break
        time.sleep(2)
        r = c.request("GET", f"/v3/account/subaccounts/{trade_id}/transfers/{r['transferId']}")
        status = r.get("status")
    print(f"  transfer {r.get('transferId')}: {status}")
    if status == "Requested":
        print("  still Requested after 30 s. Do NOT retry under a new id - check `status`"
              " later, and contact NoVig support if it never settles.")
    sub2 = Decimal(c.request("GET", f"/v3/account/subaccounts/{trade_id}/balance")["balance"])
    print(f"  subaccount now ${sub2:,.2f}")


def main():
    # --env works before or after the command name
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--env", choices=("paper", "prod"), default=argparse.SUPPRESS,
                        help="override NOVIG_ENV")
    p = argparse.ArgumentParser(description=__doc__, parents=[common],
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status", parents=[common]).set_defaults(fn=cmd_status)
    s = sub.add_parser("open-subaccount", parents=[common])
    s.add_argument("--label", default="odds-screen")
    s.set_defaults(fn=cmd_open_subaccount)
    s = sub.add_parser("read-key", parents=[common])
    s.add_argument("--name", default="odds-screen-feed")
    s.set_defaults(fn=cmd_read_key)
    for d in ("fund", "defund"):
        s = sub.add_parser(d, parents=[common])
        s.add_argument("amount")
        s.set_defaults(fn=lambda a, d=d: _transfer(a, d))
    a = p.parse_args()
    global _ACTIVE_ENV
    a.env = getattr(a, "env", None)
    _ACTIVE_ENV = (a.env or _env().get("NOVIG_ENV") or "paper").strip().lower()
    try:
        a.fn(a)
    except SystemExit:
        raise
    except Exception as e:
        _fail(e)


if __name__ == "__main__":
    main()
