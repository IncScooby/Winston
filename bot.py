"""
Winston -- 5-Minute BTC Up/Down desk. 24/7 cloud engine (FastAPI + asyncio).

Runs without a browser open. Serves index.html at "/" and a JSON API for it.

Modes
  PAPER (default): simulated fills at the quoted contract price, TP / SL / expiry.
  LIVE: real orders through the polymarket-us SDK. Always starts OFF after every
        restart, needs a typed confirmation, and a daily-loss circuit breaker
        flips it back to PAPER automatically.

Signal sources
  terrycalls : Winston follows Terry's call/lock for the 5m market, then double-checks
               it against BTC's actual move before deciding (the "veto").
  momentum   : Winston's own rule (BTC % move from the window open + timing + a
               simple probability model).

Odds sources (best first): Polymarket live midpoint -> Terry prices -> Polymarket
event prices -> a model estimate (flagged ESTIMATED, never used for live orders).

Environment variables
  ADMIN_TOKEN            password for the dashboard/API (REQUIRED for live mode)
  TERRY_API_KEY          terrycalls.com API key (free plan is fine)
  POLYMARKET_KEY_ID      Polymarket US API key id        (live mode only)
  POLYMARKET_SECRET_KEY  Polymarket US secret key        (live mode only)
  BOT_NAME               name shown on the dashboard (default Winston)
  DATA_DIR               where to keep desk_state.json (use a Railway volume to
                         keep trade history across redeploys)
  TZ_NAME                timezone for "PnL today" (default America/Los_Angeles)
  Optional live tuning: LIVE_SLIPPAGE, LIVE_TIF, LIVE_SLUG_TEMPLATE,
  LIVE_UP_OPEN_INTENT, LIVE_UP_CLOSE_INTENT, LIVE_DOWN_OPEN_INTENT,
  LIVE_DOWN_CLOSE_INTENT, EST_VOL_PCT, TERRY_POLL_SEC
"""

import asyncio
import hmac
import json
import math
import os
import re
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import requests
import websockets
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

try:  # live trading SDK is optional until you flip the switch
    from polymarket_us import PolymarketUS
    POLY_SDK = True
except Exception:  # noqa: BLE001
    PolymarketUS = None
    POLY_SDK = False

# ----------------------------------------------------------------- SETTINGS --
WINDOW = 300
BOT_NAME = os.environ.get("BOT_NAME", "Winston").strip() or "Winston"
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", str(BASE_DIR)))
STATE_FILE = DATA_DIR / "desk_state.json"
TZ = ZoneInfo(os.environ.get("TZ_NAME", "America/Los_Angeles"))

EST_VOL_PCT = float(os.environ.get("EST_VOL_PCT", "0.10"))      # typical 5-min BTC stdev, in %
TERRY_POLL_SEC = max(3.0, float(os.environ.get("TERRY_POLL_SEC", "5")))  # 5s = 17,280/day (< 20,000 free cap)
LIVE_SLIPPAGE = float(os.environ.get("LIVE_SLIPPAGE", "0.02"))
LIVE_TIF = os.environ.get("LIVE_TIF", "TIME_IN_FORCE_GOOD_TILL_CANCEL")
LIVE_SLUG_TEMPLATE = os.environ.get("LIVE_SLUG_TEMPLATE", "btc-updown-5m-{ts}")
LIVE_INTENTS = {
    ("UP", "open"): os.environ.get("LIVE_UP_OPEN_INTENT", "ORDER_INTENT_BUY_LONG"),
    ("UP", "close"): os.environ.get("LIVE_UP_CLOSE_INTENT", "ORDER_INTENT_SELL_LONG"),
    ("DOWN", "open"): os.environ.get("LIVE_DOWN_OPEN_INTENT", "ORDER_INTENT_BUY_SHORT"),
    ("DOWN", "close"): os.environ.get("LIVE_DOWN_CLOSE_INTENT", "ORDER_INTENT_SELL_SHORT"),
}
# How old (seconds) a price may be before the next source down the list is used.
FRESH_CLOB, FRESH_TERRY, FRESH_GAMMA = 6.0, 8.0, 15.0

ADMIN_TOKEN_ENV = os.environ.get("ADMIN_TOKEN", "").strip()
ADMIN_TOKEN = ADMIN_TOKEN_ENV or secrets.token_urlsafe(18)

DEFAULT_CONFIG: Dict[str, Any] = {
    "stake_usd": 10.0,
    "max_entry_price": 0.78,
    "min_entry_price": 0.30,
    "min_confidence": 65.0,
    "take_profit": 0.08,
    "stop_loss": 0.06,
    "min_btc_move": 0.04,
    "entry_min_sec": 45,
    "entry_max_sec": 200,
    "force_exit_sec": 250,
    "fee_pct": 2.0,
    "max_daily_loss": 30.0,
    "max_live_stake": 25.0,
    "signal_source": "terrycalls" if os.environ.get("TERRY_API_KEY", "").strip() else "momentum",
    "winston_veto": True,
    "live_trading": False,
    "paused": False,
}
RANGES = {
    "stake_usd": (1.0, 1000.0), "max_entry_price": (0.05, 0.99), "min_entry_price": (0.01, 0.95),
    "min_confidence": (50.0, 99.0), "take_profit": (0.01, 0.50), "stop_loss": (0.01, 0.50),
    "min_btc_move": (0.0, 1.0), "entry_min_sec": (0, 290), "entry_max_sec": (10, 295),
    "force_exit_sec": (30, 295), "fee_pct": (0.0, 10.0), "max_daily_loss": (1.0, 100000.0),
    "max_live_stake": (1.0, 10000.0),
}
INT_KEYS = {"entry_min_sec", "entry_max_sec", "force_exit_sec"}
BOOL_KEYS = ("live_trading", "paused", "winston_veto")

cfg: Dict[str, Any] = dict(DEFAULT_CONFIG)

st: Dict[str, Any] = {
    "btc": None, "btc_ts": 0.0, "feed_source": "starting", "history": [], "odds_hist": [],
    "window_start": None, "window_open": None, "open_source": "", "partial": False,
    "window_last_btc": None, "traded_window": None,
    "terry": None, "terry_status": "starting", "terry_key": None,
    "gamma": {}, "gamma_status": "starting", "clob": None, "clob_status": "starting",
    "last_quote": None, "last_move": 0.0, "signal": None,
    "position": None, "trades": [], "chat": [], "logs": [], "now_thinking": "Starting up…",
    "client": None, "live_error": "", "close_fail_ts": 0.0, "last_kind": {}, "lock": None,
}


# ------------------------------------------------------------------ HELPERS --
def add_log(msg: str) -> None:
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    st["logs"].insert(0, line)
    del st["logs"][80:]
    print(line, flush=True)


def say(who: str, kind: str, text: str, tone: str = "wait") -> None:
    """Add a line to the conversation. Repeats of the same 'kind' by the same speaker are skipped."""
    if who == "winston":
        st["now_thinking"] = text  # the live 'typing' line always shows the latest thought
    if st["last_kind"].get(who) == kind:
        return
    st["last_kind"][who] = kind
    st["chat"].append({"ts": time.time(), "who": who, "text": text, "tone": tone})
    del st["chat"][:-80]


def think(kind: str, text: str, tone: str = "wait") -> None:
    say("winston", kind, text, tone)


def pc(p: float) -> str:
    return f"{p * 100:.0f}%"


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def est_probs(move_pct: float, left: float):
    """P(window finishes Up) given the current move and time left (random-walk model)."""
    sigma = EST_VOL_PCT * math.sqrt(max(left, 3.0) / WINDOW)
    p = min(0.98, max(0.02, norm_cdf(move_pct / sigma)))
    return p, 1.0 - p


def norm_price(v: Any) -> Optional[float]:
    try:
        p = float(v)
    except (TypeError, ValueError):
        return None
    if p > 1.5:
        p /= 100.0
    return p if 0.0 < p < 1.0 else None


def norm_side(v: Any) -> Optional[str]:
    if isinstance(v, dict):
        v = v.get("side")
    s = str(v or "").strip().upper()
    if s.startswith("UP") or s in ("YES", "LONG", "BULLISH"):
        return "UP"
    if s.startswith("DOWN") or s in ("NO", "SHORT", "BEARISH"):
        return "DOWN"
    return None


def norm_conf(v: Any) -> Optional[float]:
    try:
        c = float(v)
    except (TypeError, ValueError):
        return None
    if c <= 1.0:
        c *= 100.0
    return round(c, 1)


def price_class(source: str) -> str:
    return "estimated" if source == "estimated" else "real"


def trade_date(t: Dict[str, Any]):
    return datetime.fromtimestamp(t["closed_at"], TZ).date()


def clean_value(key: str, value: Any) -> Any:
    if key in RANGES:
        lo, hi = RANGES[key]
        v = min(hi, max(lo, float(value)))
        return int(round(v)) if key in INT_KEYS else v
    if key == "signal_source":
        if value not in ("terrycalls", "momentum"):
            raise ValueError("signal_source must be 'terrycalls' or 'momentum'")
        return value
    if key in BOOL_KEYS:
        return bool(value)
    raise ValueError(f"unknown setting {key}")


def save_state() -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        payload = {"trades": st["trades"][-2000:], "config": {**cfg, "live_trading": False}}
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload))
        tmp.replace(STATE_FILE)
    except Exception as e:  # noqa: BLE001
        add_log(f"Could not save state: {e}")


def load_state() -> None:
    try:
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text())
            st["trades"] = list(data.get("trades", []))[-2000:]
            for k, v in (data.get("config") or {}).items():
                if k in cfg and k != "live_trading":
                    try:
                        cfg[k] = clean_value(k, v)
                    except Exception:  # noqa: BLE001
                        pass
            add_log(f"Loaded {len(st['trades'])} past trades from disk.")
    except Exception as e:  # noqa: BLE001
        add_log(f"Could not load saved state: {e}")
    cfg["live_trading"] = False  # always boot into paper mode


def live_ready():
    if not POLY_SDK:
        return False, "polymarket-us SDK is not installed"
    if not os.environ.get("POLYMARKET_KEY_ID") or not os.environ.get("POLYMARKET_SECRET_KEY"):
        return False, "POLYMARKET_KEY_ID / POLYMARKET_SECRET_KEY are not set"
    if not ADMIN_TOKEN_ENV:
        return False, "set an ADMIN_TOKEN environment variable first"
    return True, "ready"


def live_pnl_today() -> float:
    today = datetime.now(TZ).date()
    return sum(t["pnl"] for t in st["trades"] if t.get("mode") == "live" and trade_date(t) == today)


def check_breaker() -> None:
    if cfg["live_trading"] and live_pnl_today() <= -cfg["max_daily_loss"]:
        cfg["live_trading"] = False
        save_state()
        add_log("CIRCUIT BREAKER: daily live loss limit hit. Switched back to PAPER mode.")
        think("breaker", "🛑 We hit today's loss limit, so I've switched live trading OFF. "
                         "I'm back in paper mode and I'll keep watching.", "loss")


def scoreboard(mode: str) -> Dict[str, Any]:
    today = datetime.now(TZ).date()
    ts = [t for t in st["trades"] if t.get("mode") == mode]
    total = sum(t["pnl"] for t in ts)
    day = sum(t["pnl"] for t in ts if trade_date(t) == today)
    wins = sum(1 for t in ts if t["pnl"] > 0)
    n = len(ts)
    return {"mode": mode, "pnl_today": round(day, 2), "pnl_total": round(total, 2), "trades": n,
            "wins": wins, "win_rate": round(wins / n * 100, 1) if n else 0.0,
            "avg_trade": round(total / n, 2) if n else 0.0}


# ------------------------------------------------------------------- QUOTES --
def terry_matches(t: Dict[str, Any], left: float) -> bool:
    sl = t["item"].get("seconds_left")
    if sl is None:
        return True
    try:
        expected = float(sl) - (time.time() - t["fetched"])
    except (TypeError, ValueError):
        return True
    return abs(expected - left) <= 25


def terry_fresh() -> Optional[Dict[str, Any]]:
    t = st["terry"]
    if t and time.time() - t["fetched"] <= 20 and not t.get("stale"):
        return t
    return None


def get_quote(left: float, move_pct: float) -> Dict[str, Any]:
    """Best available contract prices for the CURRENT window, with where they came from."""
    now = time.time()
    slug = f"btc-updown-5m-{st['window_start']}"
    c = st["clob"]
    if c and c["slug"] == slug and now - c["fetched"] <= FRESH_CLOB:
        return {"up": c["up"], "down": c["down"], "source": "polymarket-live", "age": round(now - c["fetched"], 1)}
    t = terry_fresh()
    if t and now - t["fetched"] <= FRESH_TERRY and terry_matches(t, left):
        pr = t["item"].get("prices") or {}
        up, dn = norm_price(pr.get("up")), norm_price(pr.get("down"))
        if up is not None and dn is not None:
            return {"up": up, "down": dn, "source": "terrycalls", "age": round(now - t["fetched"], 1)}
    g = st["gamma"].get(slug)
    if g and now - g["fetched"] <= FRESH_GAMMA and g["up"] is not None and g["down"] is not None:
        return {"up": g["up"], "down"
                : g["down"], "source": "polymarket", "age": round(now - g["fetched"], 1)}
    up, dn = est_probs(move_pct, left)
    return {"up": round(up, 3), "down": round(dn, 3), "source": "estimated", "age": 0.0}


def signal_snapshot(move: float, left: float) -> Dict[str, Any]:
    src = cfg["signal_source"]
    if src == "momentum":
        p_up, p_dn = est_probs(move, left)
        lean = "UP" if move >= 0 else "DOWN"
        conf = round(max(p_up, p_dn) * 100, 1)
        strong = abs(move) >= cfg["min_btc_move"]
        if strong:
            detail = f"BTC is {move:+.3f}% from the target. My model gives {lean} a {conf:.0f}% chance."
        else:
            detail = (f"BTC is only {move:+.3f}% from the target. I need at least "
                      f"±{cfg['min_btc_move']:.2f}% before I consider a trade.")
        return {"source": "momentum", "side": lean if strong else None, "lean": lean,
                "confidence": conf, "status": "ready" if strong else "waiting", "detail": detail}

    def none(status: str, detail: str, lean: Optional[str] = None) -> Dict[str, Any]:
        return {"source": "terrycalls", "side": None, "lean": lean, "confidence": None,
                "status": status, "detail": detail}

    if not os.environ.get("TERRY_API_KEY", "").strip():
        return none("no_key", "TERRY_API_KEY isn't set, so I can't hear Terry. Add the key or switch me to Momentum.")
    t = st["terry"]
    if not t or time.time() - t["fetched"] > 20:
        return none("unavailable", f"I can't reach Terry right now ({st['terry_status']}).")
    if t.get("stale"):
        return none("stale", "Terry's data is stale (over 30s old), so I'm not following it.")
    if not terry_matches(t, left):
        return none("waiting", "I'm waiting for Terry to publish this window.")
    item = t["item"]
    status = str(item.get("status") or "waiting").lower()
    call = item.get("call") if isinstance(item.get("call"), dict) else None
    lock = item.get("lock") if isinstance(item.get("lock"), dict) else None
    ch = lock or call
    lean = norm_side(item.get("leaning"))
    if status in ("called", "locked") and ch:
        side = norm_side(ch.get("side"))
        conf = norm_conf(ch.get("chance") if ch.get("chance") is not None else ch.get("confidence"))
        if side and conf is not None:
            word = "locked" if lock else "called"
            return {"source": "terrycalls", "side": side, "lean": side, "confidence": conf,
                    "status": status, "detail": f"Terry {word} {side} at {conf:.0f}% confidence."}
        return none(status, "Terry made a call but I couldn't read its side or confidence, so I'm skipping it.", lean)
    return none(status, f"Terry is {status}" + (f" and leaning {lean}" if lean else "")
                + ", with no call to follow yet.", lean)


def terry_announce() -> None:
    """Put Terry's latest real call/lock/lean into the conversation (built only from his API fields)."""
    t = st["terry"]
    ws = st["window_start"]
    if not t or ws is None:
        return
    left = WINDOW - (time.time() - ws)
    if not terry_matches(t, left):
        return
    item = t["item"]
    status = str(item.get("status") or "waiting").lower()
    call = item.get("call") if isinstance(item.get("call"), dict) else None
    lock = item.get("lock") if isinstance(item.get("lock"), dict) else None
    ch = lock or call
    side = norm_side(ch.get("side")) if ch else None
    conf = norm_conf((ch or {}).get("chance") if (ch or {}).get("chance") is not None else (ch or {}).get("confidence"))
    lean = norm_side(item.get("leaning"))
    key = (ws, status, side or lean, None if conf is None else round(conf / 5))
    if key == st["terry_key"]:
        return
    st["terry_key"] = key
    if status == "locked" and side:
        text = f"Locked {side}" + (f" · {conf:.0f}% confidence" if conf is not None else "")
    elif status == "called" and side:
        text = f"Calling {side}" + (f" · {conf:.0f}% confidence" if conf is not None else "")
    elif lean:
        text = f"Leaning {lean}. No call yet."
    else:
        text = "No call yet. Watching this window."
    say("terry", str(key), text, "terry")


# --------------------------------------------------------------- LIVE (SDK) --
def get_client():
    if st["client"] is None:
        st["client"] = PolymarketUS(key_id=os.environ["POLYMARKET_KEY_ID"],
                                    secret_key=os.environ["POLYMARKET_SECRET_KEY"])
    return st["client"]


def live_slug() -> str:
    ws = st["window_start"] or int(time.time()) // WINDOW * WINDOW
    t = terry_fresh()
    if t:
        m = t["item"].get("market")
        if isinstance(m, str) and re.fullmatch(r"[a-z0-9][a-z0-9\-_]{5,100}", m):
            return m
    return LIVE_SLUG_TEMPLATE.format(ts=ws)


def order_id(resp: Any) -> Optional[str]:
    if isinstance(resp, dict):
        for k in ("id", "orderId", "order_id"):
            if resp.get(k):
                return str(resp[k])
        inner = resp.get("order")
        if isinstance(inner, dict):
            return order_id(inner)
    return None


def live_buy(slug: str, side: str, price: float, qty: int) -> Dict[str, Any]:
    client = get_client()
    client.markets.retrieve_by_slug(slug)  # refuse to trade a market the exchange can't find
    limit = min(0.99, round(price + LIVE_SLIPPAGE, 2))
    resp = client.orders.create({
        "marketSlug": slug,
        "intent": LIVE_INTENTS[(side, "open")],
        "type": "ORDER_TYPE_LIMIT",
        "price": {"value": f"{limit:.2f}", "currency": "USD"},
        "quantity": int(qty),
        "tif": LIVE_TIF,
    })
    return {"order_id": order_id(resp), "slug": slug, "limit": limit}


def live_sell(pos: Dict[str, Any], price: float) -> Dict[str, Any]:
    client = get_client()
    limit = max(0.01, round(price - LIVE_SLIPPAGE, 2))
    resp = client.orders.create({
        "marketSlug": pos["slug"],
        "intent": LIVE_INTENTS[(pos["side"], "close")],
        "type": "ORDER_TYPE_LIMIT",
        "price": {"value": f"{limit:.2f}", "currency": "USD"},
        "quantity": int(pos["shares"]),
        "tif": LIVE_TIF,
    })
    if pos.get("order_id"):  # clean up any unfilled remainder of the entry order
        try:
            client.orders.cancel(pos["order_id"], {"marketSlug": pos["slug"]})
        except Exception:  # noqa: BLE001
            pass
    return {"order_id": order_id(resp), "limit": limit}


# ------------------------------------------------------------------ TRADING --
async def open_position(side: str, quote: Dict[str, Any], *, manual: bool, confidence: Optional[float],
                        move_pct: float, stake: Optional[float] = None, why: str = "") -> Dict[str, Any]:
    async with st["lock"]:
        if st["position"] is not None:
            raise RuntimeError("a position is already open")
        mode = "live" if cfg["live_trading"] else "paper"
        price = quote[side.lower()]
        stake = float(stake if stake is not None else cfg["stake_usd"])
        order: Dict[str, Any] = {}
        slug = f"btc-updown-5m-{st['window_start']}"
        if mode == "live":
            ok, reason = live_ready()
            if not ok:
                raise RuntimeError(f"live trading not ready: {reason}")
            if quote["source"] == "estimated":
                raise RuntimeError("no real market price available, so I won't place a live order")
            stake = min(stake, cfg["max_live_stake"])
            qty = int(stake // price)
            if qty < 1:
                raise RuntimeError("stake is too small to buy one contract at this price")
            slug = live_slug()
            order = await asyncio.to_thread(live_buy, slug, side, price, qty)
            shares: float = qty
        else:
            shares = stake / price
        now = time.time()
        pos = {
            "id": f"t{int(now * 1000)}", "mode": mode, "side": side, "slug": slug,
            "window_start": st["window_start"], "opened_at": now, "entry": round(price, 4),
            "shares": shares, "stake": round(shares * price, 2), "price_source": quote["source"],
            "btc_open": st["window_open"], "btc_at_entry": st["btc"], "move_pct_at_entry": round(move_pct, 4),
            "signal_source": "manual" if manual else cfg["signal_source"], "confidence": confidence,
            "manual": manual, "order_id": order.get("order_id"), "fee_pct": cfg["fee_pct"],
        }
        st["position"] = pos
        st["traded_window"] = st["window_start"]
        add_log(f"[{mode.upper()}] BUY {side} @ {price:.2f} x{shares:.2f} (${pos['stake']:.2f}) "
                f"{'MANUAL' if manual else cfg['signal_source']}")
        intro = why or "You asked for this one."
        think(f"entered{pos['id']}", f"🟢 {intro} Buying {side} with ${pos['stake']:.2f} "
                                     f"[{mode.upper()}].", "go")
        return pos


async def close_position(price: float, reason: str, settle: bool = False,
                         raise_on_fail: bool = False) -> Optional[Dict[str, Any]]:
    async with st["lock"]:
        pos = st["position"]
        if pos is None:
            return None
        info: Dict[str, Any] = {}
        if pos["mode"] == "live" and not settle:
            try:
                info = await asyncio.to_thread(live_sell, pos, price)
            except Exception as e:  # noqa: BLE001
                st["close_fail_ts"] = time.time()
                st["live_error"] = str(e)
                add_log(f"LIVE close failed: {e}")
                think("closefail", f"⚠️ I couldn't close the live position ({e}). I'll keep retrying.", "loss")
                if raise_on_fail:
                    raise HTTPException(status_code=502, detail=f"Live close failed: {e}")
                return None
        fee = pos["stake"] * pos["fee_pct"] / 100.0
        pnl = pos["shares"] * (price - pos["entry"]) - fee
        now = time.time()
        trade = {
            **pos, "exit": round(price, 4), "reason": reason, "pnl": round(pnl, 4), "fee": round(fee, 4),
            "closed_at": now, "held_sec": round(now - pos["opened_at"], 1), "btc_at_exit": st["btc"],
            "close_order_id": info.get("order_id"), "settled": settle,
        }
        st["trades"].append(trade)
        del st["trades"][:-2000]
        st["position"] = None
        save_state()
        add_log(f"[{pos['mode'].upper()}] SELL {pos['side']} @ {price:.2f} ({reason}) pnl ${pnl:+.2f}")
        think(f"exit{trade['id']}", f"{'✅' if pnl > 0 else '❌'} Sold {pos['side']} at {pc(price)} ({reason}). "
                                    f"{pnl:+.2f} USD after estimated fees.", "win" if pnl > 0 else "loss")
        if pos["mode"] == "live":
            check_breaker()
        return trade


async def on_new_window(ws: int, elapsed: float, btc: float) -> None:
    pos = st["position"]
    if pos is not None and st["window_start"] is not None:
        final = st["window_last_btc"] or btc
        opened = st["window_open"] or final
        up_won = final >= opened
        won = (pos["side"] == "UP") == up_won
        await close_position(1.0 if won else 0.0, f"window expired ({'won' if won else 'lost'})", settle=True)
    st["window_start"] = ws
    open_price, source, partial = btc, "live feed", False
    if elapsed > 3:
        t = terry_fresh()
        ptb = None
        if t and terry_matches(t, WINDOW - elapsed):
            try:
                ptb = float(t["item"].get("price_to_beat"))
            except (TypeError, ValueError):
                ptb = None
        if ptb and ptb > 0:
            open_price, source = ptb, "terrycalls price-to-beat"
        else:
            partial = True
    st["window_open"], st["open_source"], st["partial"] = open_price, source, partial
    add_log(f"New window {ws}: target {open_price:,.2f} ({source})" + (" - joined late, sitting out" if partial else ""))
    say("sys", f"w{ws}", f"New 5-minute window · target ${open_price:,.2f}", "sys")


async def manage_position(quote: Dict[str, Any], elapsed: float) -> None:
    pos = st["position"]
    price = quote[pos["side"].lower()]
    delta = price - pos["entry"]
    same = price_class(quote["source"]) == price_class(pos["price_source"])
    reason = None
    if elapsed >= cfg["force_exit_sec"]:
        reason = "time exit"
    elif same and delta >= cfg["take_profit"]:
        reason = "take profit"
    elif same and -delta >= cfg["stop_loss"]:
        reason = "stop loss"
    if reason:
        if time.time() - st["close_fail_ts"] < 3:
            return
        await close_position(price, reason)
    else:
        think(f"hold{pos['id']}", f"Holding {pos['side']} from {pc(pos['entry'])}. Now {pc(price)} "
                                  f"({delta * 100:+.0f} pts). I sell at +{cfg['take_profit'] * 100:.0f} pts or "
                                  f"-{cfg['stop_loss'] * 100:.0f} pts, or by {cfg['force_exit_sec']}s.", "info")


async def consider_entry(quote: Dict[str, Any], elapsed: float, move: float) -> None:
    ws = st["window_start"]
    sig = st["signal"]
    side, conf = sig["side"], sig["confidence"]
    terry = cfg["signal_source"] == "terrycalls"
    view = (f"{'Terry says' if terry else 'My read says'} {side} at {conf:.0f}%"
            if side and conf is not None else sig["detail"])
    if cfg["paused"]:
        think("paused", "I'm paused, so I'm only watching. Hit Resume when you want me trading.", "wait")
        return
    if st["partial"]:
        think("partial", "I joined this window late, so I don't trust the target price. "
                         "I'm sitting this one out and I'll be ready for the next.", "wait")
        return
    if st["traded_window"] == ws:
        think(f"done{ws}", "That's my trade for this window. I'll wait for the next one.", "wait")
        return
    if elapsed < cfg["entry_min_sec"]:
        think(f"early:{side}", f"{view}. It's only {int(elapsed)}s in and early moves are noisy, so I'll wait "
                               f"until {cfg['entry_min_sec']}s before I decide.", "wait")
        return
    if elapsed > cfg["entry_max_sec"]:
        think("late", "It's too late in this window to enter safely. On to the next one.", "wait")
        return
    if side is None:
        think(f"sig:{sig['status']}:{sig.get('lean')}", f"{sig['detail']} I'll keep watching.", "wait")
        return
    if conf is None or conf < cfg["min_confidence"]:
        think(f"lowconf:{side}", f"{view}, but I need at least {cfg['min_confidence']:.0f}%. Passing for now.", "wait")
        return
    if terry and cfg["winston_veto"]:
        against = (side == "UP" and move <= -cfg["min_btc_move"]) or (side == "DOWN" and move >= cfg["min_btc_move"])
        if against:
            think(f"veto:{side}", f"{view}, but BTC is {move:+.3f}% vs the target, which points the other way. "
                                  f"I don't take calls the price disagrees with, so I'm overruling this one.", "info")
            return
    price = quote[side.lower()]
    if price > cfg["max_entry_price"]:
        think(f"pricey:{side}", f"{view}. But {side} already costs {pc(price)}, over my {pc(cfg['max_entry_price'])} "
                                f"limit, so the move is priced in. Passing.", "wait")
        return
    if price < cfg["min_entry_price"]:
        think(f"cheap:{side}", f"{view}. But the crowd only gives {side} {pc(price)}, so they strongly disagree. "
                               f"Passing.", "wait")
        return
    if cfg["live_trading"]:
        if quote["source"] == "estimated":
            think("estlive", "I don't have a real market price right now, so I won't place a live order.", "wait")
            return
        if live_pnl_today() <= -cfg["max_daily_loss"]:
            check_breaker()
            return
    agrees = (side == "UP" and move >= 0) or (side == "DOWN" and move <= 0)
    why = (f"{view}, and BTC {'agrees' if agrees else 'is flat-ish'} ({move:+.3f}% vs the target). "
           f"{side} costs {pc(price)}, inside my limits.")
    try:
        await open_position(side, quote, manual=False, confidence=conf, move_pct=move, why=why)
    except Exception as e:  # noqa: BLE001
        st["traded_window"] = ws  # don't hammer the exchange every second
        add_log(f"Entry failed: {e}")
        think(f"entryfail{ws}", f"⚠️ I wanted to buy {side} but it failed: {e}", "loss")


async def tick() -> None:
    now = time.time()
    btc = st["btc"]
    if btc is None or now - st["btc_ts"] > 20:
        think("nofeed", "The BTC price feed is down or stale. I won't trade blind.", "wait")
        return
    ts = int(now)
    h = st["history"]
    new_second = not h or h[-1][0] != ts
    if new_second:
        h.append([ts, btc])
        del h[:-1800]
    ws = ts // WINDOW * WINDOW
    elapsed = now - ws
    left = WINDOW - elapsed
    if st["window_start"] != ws:
        await on_new_window(ws, elapsed, btc)
    st["window_last_btc"] = btc
    open_price = st["window_open"]
    move = (btc - open_price) / open_price * 100.0
    quote = get_quote(left, move)
    st["last_quote"], st["last_move"] = quote, move
    if new_second:
        total = quote["up"] + quote["down"]
        st["odds_hist"].append([ts, round(quote["up"] / total * 100.0, 1)])
        del st["odds_hist"][:-900]
    st["signal"] = signal_snapshot(move, left)
    if st["position"] is not None:
        await manage_position(quote, elapsed)
    else:
        await consider_entry(quote, elapsed, move)


# -------------------------------------------------------------- BACKGROUND --
def on_price(p: float, source: str) -> None:
    st["btc"], st["btc_ts"], st["feed_source"] = p, time.time(), source


async def feed_task() -> None:
    uri = "wss://ws-feed.exchange.coinbase.com"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20, max_size=2 ** 20) as ws:
                await ws.send(json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}))
                async for raw in ws:
                    m = json.loads(raw)
                    if m.get("type") == "ticker" and m.get("price"):
                        on_price(float(m["price"]), "coinbase-ws")
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            st["feed_source"] = f"reconnecting ({type(e).__name__})"
            await asyncio.sleep(2)


def fetch_spot() -> float:
    r = requests.get("https://api.coinbase.com/v2/prices/BTC-USD/spot", timeout=5)
    r.raise_for_status()
    return float(r.json()["data"]["amount"])


async def rest_task() -> None:
    while True:
        await asyncio.sleep(2)
        if time.time() - st["btc_ts"] > 4:
            try:
                on_price(await asyncio.to_thread(fetch_spot), "coinbase-rest")
            except Exception:  # noqa: BLE001
                pass


def fetch_terry() -> Dict[str, Any]:
    r = requests.get("https://terrycalls.com/api/v1/calls", params={"horizon": "5m"},
                     headers={"X-API-Key": os.environ["TERRY_API_KEY"].strip(), "Accept": "application/json"},
                     timeout=6)
    if r.status_code == 429:
        raise RuntimeError("rate limited")
    r.raise_for_status()
    return r.json()


async def terry_task() -> None:
    while True:
        delay = TERRY_POLL_SEC
        if not os.environ.get("TERRY_API_KEY", "").strip():
            st["terry_status"] = "no API key"
            await asyncio.sleep(10)
            continue
        try:
            data = await asyncio.to_thread(fetch_terry)
            item = None
            for m in data.get("markets", []):
                if str(m.get("horizon", "")).lower().startswith("5"):
                    item = m
                    break
            if item is None:
                st["terry_status"] = "no 5m market in response"
            else:
                st["terry"] = {"item": item, "fetched": time.time(), "stale": bool(data.get("stale"))}
                st["terry_status"] = "stale" if data.get("stale") else "ok"
                terry_announce()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            st["terry_status"] = f"error: {e}"
            if "rate limited" in str(e):
                delay = 30
        await asyncio.sleep(delay)


def fetch_gamma(slug: str) -> Dict[str, Any]:
    r = requests.get("https://gamma-api.polymarket.com/events", params={"slug": slug}, timeout=6)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise RuntimeError("market not listed yet")
    market = data[0]["markets"][0]
    outcomes = market["outcomes"]
    prices = market["outcomePrices"]
    outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
    prices = json.loads(prices) if isinstance(prices, str) else prices
    mapped = {str(o).lower(): norm_price(p) for o, p in zip(outcomes, prices)}
    tokens: Dict[str, str] = {}
    raw_ids = market.get("clobTokenIds")
    if raw_ids:
        try:
            ids = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
            tokens = {str(o).lower(): str(i) for o, i in zip(outcomes, ids)}
        except Exception:  # noqa: BLE001
            tokens = {}
    return {"slug": slug, "up": mapped.get("up"), "down": mapped.get("down"), "tokens": tokens,
            "fetched": time.time()}


async def gamma_task() -> None:
    """Keeps the current AND next window's market cached so odds are ready the moment a window rolls."""
    while True:
        base = int(time.time()) // WINDOW * WINDOW
        for i, w in enumerate((base, base + WINDOW)):
            slug = f"btc-updown-5m-{w}"
            try:
                st["gamma"][slug] = await asyncio.to_thread(fetch_gamma, slug)
                if i == 0:
                    st["gamma_status"] = "ok"
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                if i == 0:
                    st["gamma_status"] = f"error: {e}"
        for k in list(st["gamma"]):
            try:
                if int(k.rsplit("-", 1)[1]) < base - WINDOW:
                    del st["gamma"][k]
            except Exception:  # noqa: BLE001
                st["gamma"].pop(k, None)
        await asyncio.sleep(3)


def fetch_mid(token: str) -> Optional[float]:
    r = requests.get("https://clob.polymarket.com/midpoint", params={"token_id": token}, timeout=4)
    r.raise_for_status()
    return norm_price(r.json().get("mid"))


async def clob_task() -> None:
    """Fast (1.5s) live midpoint for the current window's Up and Down contracts."""
    while True:
        try:
            slug = f"btc-updown-5m-{int(time.time()) // WINDOW * WINDOW}"
            tok = (st["gamma"].get(slug) or {}).get("tokens") or {}
            if tok.get("up") and tok.get("down"):
                up, dn = await asyncio.gather(asyncio.to_thread(fetch_mid, tok["up"]),
                                              asyncio.to_thread(fetch_mid, tok["down"]))
                if up is not None and dn is not None:
                    st["clob"] = {"slug": slug, "up": up, "down": dn, "fetched": time.time()}
                    st["clob_status"] = "ok"
                else:
                    st["clob_status"] = "no midpoint yet"
            else:
                st["clob_status"] = "waiting for market ids"
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            st["clob_status"] = f"error: {e}"
        await asyncio.sleep(1.5)


async def engine() -> None:
    add_log(f"{BOT_NAME}'s 24/7 engine started (paper mode).")
    while True:
        t0 = time.time()
        try:
            await tick()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            add_log(f"Engine error: {type(e).__name__}: {e}")
        await asyncio.sleep(max(0.05, 1.0 - (time.time() - t0)))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    st["lock"] = asyncio.Lock()
    load_state()
    say("sys", "boot", f"{BOT_NAME} is online · paper mode", "sys")
    if not ADMIN_TOKEN_ENV:
        print(f"\n*** ADMIN_TOKEN not set. Temporary dashboard password for this run: {ADMIN_TOKEN}\n"
              f"*** Set ADMIN_TOKEN in your environment to keep it (required for live trading).\n", flush=True)
    tasks = [asyncio.create_task(f()) for f in (feed_task, rest_task, terry_task, gamma_task, clob_task, engine)]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()


app = FastAPI(title=f"{BOT_NAME} · 5-Minute BTC Up/Down Desk", version="4.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False,
                   allow_methods=["*"], allow_headers=["*"])


def require_auth(token: str) -> None:
    if not hmac.compare_digest((token or "").encode(), ADMIN_TOKEN.encode()):
        raise HTTPException(status_code=401, detail="Wrong or missing admin token.")


class ConfigUpdate(BaseModel):
    stake_usd: Optional[float] = None
    max_entry_price: Optional[float] = None
    min_entry_price: Optional[float] = None
    min_confidence: Optional[float] = None
    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None
    min_btc_move: Optional[float] = None
    entry_min_sec: Optional[float] = None
    entry_max_sec: Optional[float] = None
    force_exit_sec: Optional[float] = None
    fee_pct: Optional[float] = None
    max_daily_loss: Optional[float] = None
    max_live_stake: Optional[float] = None
    signal_source: Optional[str] = None
    winston_veto: Optional[bool] = None
    live_trading: Optional[bool] = None
    paused: Optional[bool] = None
    confirm_live: Optional[str] = None


class ManualTrade(BaseModel):
    side: str
    stake_usd: Optional[float] = None


def dump(model: BaseModel) -> Dict[str, Any]:
    fn = getattr(model, "model_dump", None) or model.dict
    return {k: v for k, v in fn().items() if v is not None}


def position_view() -> Optional[Dict[str, Any]]:
    pos = st["position"]
    if not pos:
        return None
    q = st["last_quote"] or {}
    cur = q.get(pos["side"].lower())
    view = dict(pos)
    view["current"] = cur
    view["held_sec"] = round(time.time() - pos["opened_at"], 1)
    view["tp_price"] = round(min(0.99, pos["entry"] + cfg["take_profit"]), 2)
    view["sl_price"] = round(max(0.01, pos["entry"] - cfg["stop_loss"]), 2)
    if cur is not None:
        fee = pos["stake"] * pos["fee_pct"] / 100.0
        view["value"] = round(pos["shares"] * cur, 2)
        view["unrealized_pnl"] = round(pos["shares"] * (cur - pos["entry"]) - fee, 2)
        view["unrealized_pct"] = round(view["unrealized_pnl"] / pos["stake"] * 100, 1) if pos["stake"] else 0.0
    return view


def markers_view(since: float) -> list:
    out = []
    for t in st["trades"][-60:]:
        if t["opened_at"] >= since:
            out.append({"time": int(t["opened_at"]), "kind": "enter", "side": t["side"], "price": t["entry"]})
        if t["closed_at"] >= since:
            out.append({"time": int(t["closed_at"]), "kind": "exit", "side": t["side"],
                        "price": t["exit"], "pnl": t["pnl"]})
    p = st["position"]
    if p and p["opened_at"] >= since:
        out.append({"time": int(p["opened_at"]), "kind": "enter", "side": p["side"], "price": p["entry"]})
    return out


def build_state(since: Optional[int] = None) -> Dict[str, Any]:
    """`since` = newest chart timestamp the browser already has; only newer points are sent."""
    now = time.time()
    ws = st["window_start"] or int(now) // WINDOW * WINDOW
    left = max(0.0, WINDOW - (now - ws))
    btc, open_price = st["btc"], st["window_open"]
    q = st["last_quote"] or {}
    ok, why = live_ready()
    mode = "live" if cfg["live_trading"] else "paper"
    floor = ws - WINDOW
    full = since is None or since < floor
    lo = floor if full else since + 1
    up, dn = q.get("up"), q.get("down")
    tot = (up + dn) if up is not None and dn is not None else None
    return {
        "name": BOT_NAME, "server_time": now, "mode": mode,
        "btc_price": btc, "window_start": ws, "window_end": ws + WINDOW, "seconds_left": round(left, 1),
        "window_open": open_price, "open_source": st["open_source"], "partial": st["partial"],
        "delta_pct": round(st["last_move"], 4),
        "delta_usd": round(btc - open_price, 2) if btc and open_price else 0.0,
        "feed": {"source": st["feed_source"], "age": round(now - st["btc_ts"], 1) if st["btc_ts"] else None},
        "prices": {"up": up, "down": dn, "source": q.get("source", "none"), "age": q.get("age"),
                   "norm_up": round(up / tot * 100, 1) if tot else None,
                   "norm_down": round(dn / tot * 100, 1) if tot else None},
        "signal": st["signal"],
        "terry": {"enabled": bool(os.environ.get("TERRY_API_KEY", "").strip()), "status": st["terry_status"]},
        "polymarket": {"status": st["gamma_status"], "live": st["clob_status"]},
        "position": position_view(),
        "scoreboard": scoreboard(mode),
        "thinking": st["now_thinking"],
        "chat": st["chat"][-60:],
        "recent_trades": list(reversed(st["trades"][-50:])),
        "markers": markers_view(floor),
        "history_full": full,
        "history": [p for p in st["history"] if p[0] >= lo],
        "odds_history": [p for p in st["odds_hist"] if p[0] >= lo],
        "config": cfg,
        "live": {"enabled": cfg["live_trading"], "ready": ok, "reason": why, "error": st["live_error"],
                 "pnl_today": round(live_pnl_today(), 2), "slug": live_slug()},
        "logs": st["logs"][:30],
    }


# ---------------------------------------------------------------- ENDPOINTS --
@app.get("/")
async def index():
    return FileResponse(BASE_DIR / "index.html")


@app.get("/health")
async def health():
    return {"ok": True, "name": BOT_NAME, "feed": st["feed_source"], "mode": "live" if cfg["live_trading"] else "paper"}


@app.get("/state")
async def get_state(since: Optional[int] = None, x_admin_token: str = Header(default="")):
    require_auth(x_admin_token)
    return build_state(since)


@app.post("/config")
async def update_config(update: ConfigUpdate, x_admin_token: str = Header(default="")):
    require_auth(x_admin_token)
    data = dump(update)
    confirm = data.pop("confirm_live", None)
    try:
        for key, value in data.items():
            if key == "live_trading":
                if value:
                    if confirm != "GO LIVE":
                        raise ValueError("Live mode needs the confirmation. Use the GO LIVE button.")
                    ok, why = live_ready()
                    if not ok:
                        raise ValueError(f"Live mode isn't ready: {why}.")
                    if live_pnl_today() <= -cfg["max_daily_loss"]:
                        raise ValueError("Today's live loss limit is already hit. Try again tomorrow or raise it.")
                    cfg["live_trading"] = True
                    st["live_error"] = ""
                    add_log("*** LIVE TRADING ENABLED: REAL MONEY ***")
                    think("golive", "🔴 Live mode is ON. From here I'm placing real orders.", "go")
                else:
                    cfg["live_trading"] = False
                    add_log("Live trading switched OFF (paper mode).")
                    think("gopaper", "Back to paper mode. No real orders from me.", "info")
            else:
                cfg[key] = clean_value(key, value)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    save_state()
    return {"status": "success", "config": cfg, "live_trading": cfg["live_trading"]}


@app.post("/cashout")
async def cashout(x_admin_token: str = Header(default="")):
    require_auth(x_admin_token)
    pos = st["position"]
    if not pos:
        raise HTTPException(status_code=400, detail="No active position to cash out.")
    q = st["last_quote"] or {}
    price = q.get(pos["side"].lower())
    if price is None:
        raise HTTPException(status_code=400, detail="No live price available right now.")
    trade = await close_position(price, "cash out (manual)", raise_on_fail=True)
    if trade is None:
        raise HTTPException(status_code=400, detail="Position was already closed.")
    return {"status": "success", "pnl": round(trade["pnl"], 2), "exit": trade["exit"]}


@app.post("/manual-trade")
async def manual_trade(req: ManualTrade, x_admin_token: str = Header(default="")):
    require_auth(x_admin_token)
    side = norm_side(req.side)
    if side is None:
        raise HTTPException(status_code=400, detail="side must be UP or DOWN.")
    if st["position"] is not None:
        raise HTTPException(status_code=400, detail="You already have an open position. Cash out first.")
    q = st["last_quote"]
    if not q or st["window_open"] is None:
        raise HTTPException(status_code=400, detail="No live price yet. Try again in a moment.")
    if cfg["live_trading"] and q["source"] == "estimated":
        raise HTTPException(status_code=400, detail="No real market price available, so live orders are blocked.")
    try:
        pos = await open_position(side, q, manual=True, confidence=None, move_pct=st["last_move"],
                                  stake=req.stake_usd, why="You told me to buy this one.")
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "success", "position": pos}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
