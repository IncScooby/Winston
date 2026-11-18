"""
Winston v7 -- 5-Minute BTC Up/Down desk. 24/7 cloud engine (FastAPI + asyncio).

v7 = v5.2 strict gates + v6 losing-streak breaker, with the decision logic moved into strategy.py
  * strategy.py holds evaluate_entry() / exit_reason() as PURE functions. The live bot and backtest.py call the
    exact same code, so a backtest tests what actually trades.
  * New gates: `chop` (price keeps stepping over the target = noise, skip) and `min_win_prob` (absolute floor on
    the win probability, on top of the edge-vs-cost check).
  * Quote pairs are validated structurally (valid_book): bid<=ask, Up+Down mids ~1, and ask_up ~ 1 - bid_down.
  * Every second is recorded to ticks.jsonl (target, BTC, both order books) so backtest.py can replay REAL data.
  * Exit defaults tightened (take profit 8 pts / stop 10 pts) so one loss can't erase two wins.
  * Live trading still ALWAYS boots OFF. Nothing here is a profit guarantee.

Environment variables
  ADMIN_TOKEN (required for live) | TERRY_API_KEY | POLYMARKET_KEY_ID | POLYMARKET_SECRET_KEY
  BOT_NAME | DATA_DIR | TZ_NAME | EST_VOL_PCT | TERRY_POLL_SEC | ACCOUNT_POLL_SEC | RECORD_TICKS (default 1)
  LIVE_SLIPPAGE | LIVE_TIF | LIVE_SLUG_TEMPLATE | LIVE_FILL_WAIT_SEC | PM_WS_URL
  LIVE_UP_OPEN_INTENT / LIVE_UP_CLOSE_INTENT / LIVE_DOWN_OPEN_INTENT / LIVE_DOWN_CLOSE_INTENT
"""

import asyncio
import hmac
import json
import math
import os
import re
import secrets
import time
from collections import deque
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

import strategy as strat

try:  # live SDK is optional until you flip the switch
    from polymarket_us import PolymarketUS
    POLY_SDK = True
except Exception:  # noqa: BLE001
    PolymarketUS = None
    POLY_SDK = False

# ================================================================= SETTINGS ==
WINDOW = 300
BOT_NAME = os.environ.get("BOT_NAME", "Winston").strip() or "Winston"
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", str(BASE_DIR)))
STATE_FILE = DATA_DIR / "desk_state.json"
EVENT_FILE = DATA_DIR / "events.jsonl"
TICK_FILE = DATA_DIR / "ticks.jsonl"
RECORD_TICKS = os.environ.get("RECORD_TICKS", "1").strip() not in ("0", "false", "no")
TZ = ZoneInfo(os.environ.get("TZ_NAME", "America/Los_Angeles"))

EST_VOL_PCT = float(os.environ.get("EST_VOL_PCT", "0.10"))
TERRY_POLL_SEC = max(3.0, float(os.environ.get("TERRY_POLL_SEC", "5")))
ACCOUNT_POLL_SEC = max(5.0, float(os.environ.get("ACCOUNT_POLL_SEC", "15")))
LIVE_SLIPPAGE = float(os.environ.get("LIVE_SLIPPAGE", "0.02"))
LIVE_TIF = os.environ.get("LIVE_TIF", "TIME_IN_FORCE_GOOD_TILL_CANCEL")
LIVE_SLUG_TEMPLATE = os.environ.get("LIVE_SLUG_TEMPLATE", "btc-updown-5m-{ts}")
LIVE_FILL_WAIT = float(os.environ.get("LIVE_FILL_WAIT_SEC", "2.5"))
PM_WS_URL = os.environ.get("PM_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
LIVE_INTENTS = {
    ("UP", "open"): os.environ.get("LIVE_UP_OPEN_INTENT", "ORDER_INTENT_BUY_LONG"),
    ("UP", "close"): os.environ.get("LIVE_UP_CLOSE_INTENT", "ORDER_INTENT_SELL_LONG"),
    ("DOWN", "open"): os.environ.get("LIVE_DOWN_OPEN_INTENT", "ORDER_INTENT_BUY_SHORT"),
    ("DOWN", "close"): os.environ.get("LIVE_DOWN_CLOSE_INTENT", "ORDER_INTENT_SELL_SHORT"),
}
# Max age (s) before the next price source down the list is used.
FRESH_PM_LINK, FRESH_CLOB, FRESH_TERRY, FRESH_GAMMA = 15.0, 6.0, 8.0, 15.0
PAIR_TOL = 0.08  # Up+Down mids must sum to ~1, otherwise the pair is stale or from a mismatched market

ADMIN_TOKEN_ENV = os.environ.get("ADMIN_TOKEN", "").strip()
ADMIN_TOKEN = ADMIN_TOKEN_ENV or secrets.token_urlsafe(18)

DEFAULT_CONFIG: Dict[str, Any] = {
    # entry
    "min_confidence": 60.0, "min_btc_move": 0.02, "entry_min_sec": 20, "entry_max_sec": 60,
    "max_entry_price": 0.74, "min_entry_price": 0.40,
    # structural confirmation + math gates
    "confirm_sec": 4.0, "min_edge": 0.04, "align_min_odds": 0.55, "max_spread": 0.05,
    "terry_weight": 0.5, "vol_lookback_sec": 90, "min_win_prob": 0.62,
    "chop_lookback_sec": 30, "max_chop_crosses": 1,
    # multi-trade
    "max_trades_per_window": 1, "reentry_cooldown_sec": 6, "after_loss_cooldown_sec": 30, "max_window_losses": 2,
    "knife_pts": 0.05, "knife_window_sec": 6,
    # exits
    "take_profit": 0.08, "stop_loss": 0.10, "force_exit_sec": 150, "scalp_mode": True,
    "inval_buffer_pct": 0.015, "inval_hold_sec": 4.0, "min_hold_sec": 3.0,
    "trail_pct": 0.07, "hold_to_settle_price": 0.95,
    # losing-streak breaker: N losses in a row -> sit out M full rounds
    "loss_streak_pause": 3, "streak_pause_windows": 2,
    # sizing / live safety
    "stake_usd": 10.0, "fee_pct": 2.0, "max_daily_loss": 30.0, "max_live_stake": 25.0,
    "max_stake_pct_balance": 25.0,
    # modes
    "signal_source": "terrycalls" if os.environ.get("TERRY_API_KEY", "").strip() else "momentum",
    "winston_veto": True, "live_trading": False, "paused": False,
}
RANGES = {  # kept wide so the dashboard sliders are never silently clamped
    "stake_usd": (1.0, 5000.0), "max_entry_price": (0.01, 1.0), "min_entry_price": (0.0, 0.99),
    "min_confidence": (0.0, 100.0), "take_profit": (0.005, 2.0), "stop_loss": (0.005, 1.0),
    "min_btc_move": (0.0, 2.0), "entry_min_sec": (0, 299), "entry_max_sec": (0, 299),
    "force_exit_sec": (1, 299), "fee_pct": (0.0, 10.0), "max_daily_loss": (1.0, 100000.0),
    "max_live_stake": (1.0, 10000.0), "max_trades_per_window": (1, 20),
    "reentry_cooldown_sec": (0.0, 120.0), "inval_buffer_pct": (0.0, 1.0), "inval_hold_sec": (0.0, 60.0),
    "min_hold_sec": (0.0, 60.0), "trail_pct": (0.005, 1.0), "hold_to_settle_price": (0.5, 1.0),
    "max_stake_pct_balance": (0.0, 100.0),
    "after_loss_cooldown_sec": (0.0, 600.0), "max_window_losses": (1, 20), "knife_pts": (0.0, 0.5),
    "knife_window_sec": (1.0, 60.0), "confirm_sec": (0.0, 30.0), "min_edge": (0.0, 0.5), "align_min_odds": (0.3, 0.95),
    "max_spread": (0.005, 0.5), "terry_weight": (0.0, 1.0), "vol_lookback_sec": (15, 300),
    "min_win_prob": (0.0, 0.99), "chop_lookback_sec": (5, 120), "max_chop_crosses": (0, 20),
    "loss_streak_pause": (1, 10), "streak_pause_windows": (0, 12),
}
INT_KEYS = {"entry_min_sec", "entry_max_sec", "force_exit_sec", "max_trades_per_window", "max_window_losses",
            "max_chop_crosses", "loss_streak_pause", "streak_pause_windows"}
BOOL_KEYS = ("live_trading", "paused", "winston_veto", "scalp_mode")

cfg: Dict[str, Any] = dict(DEFAULT_CONFIG)
cfg_meta = {"version": 0, "updated": time.time()}
SCHEMA = 4  # bump when defaults change meaning; older saved exit/risk settings are then reset
RESET_KEYS = {"stop_loss", "take_profit", "min_hold_sec", "trail_pct", "max_trades_per_window", "reentry_cooldown_sec",
              "min_confidence", "min_entry_price", "max_entry_price", "inval_buffer_pct", "inval_hold_sec",
              "entry_min_sec", "entry_max_sec", "force_exit_sec", "min_btc_move", "min_win_prob"}

st: Dict[str, Any] = {
    "btc": None, "btc_ts": 0.0, "feed_source": "starting", "latency_ms": None,
    "ticks": deque(maxlen=600), "history": [], "odds_hist": [],
    "window_start": None, "window_open": None, "open_source": "", "partial": False,
    "win_trades": {}, "win_losses": {}, "side_block": {}, "last_exit_loss": False, "basis": 0.0,
    "loss_streak": 0, "pause_until": 0.0, "tick_buf": [],
    "mid_hist": deque(maxlen=400), "gates": [], "eval": {}, "side_since": {"side": None, "since": 0.0}, "last_exit_ts": 0.0, "entry_block_until": 0.0,
    "terry": None, "terry_status": "starting", "terry_key": None,
    "gamma": {}, "gamma_status": "starting", "clob": None, "clob_status": "starting",
    "books": {}, "pm_last_msg": 0.0, "pm_sub": set(), "pm_status": "starting",
    "last_quote": None, "last_move": 0.0, "signal": None,
    "position": None, "trades": [], "chat": [], "logs": [], "now_thinking": "Starting up…",
    "client": None, "creds": None, "live_error": "", "close_fail_ts": 0.0, "last_kind": {},
    "slug_ok": set(), "account": {"status": "not linked"},
    "lock": None, "evt": None, "acct_evt": None,
}


# ================================================================= HELPERS ==
def add_log(msg: str) -> None:
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    st["logs"].insert(0, line)
    del st["logs"][80:]
    print(line, flush=True)


def log_event(kind: str, **data: Any) -> None:
    """Structured JSONL audit trail (entries, exits, breaker, config, errors)."""
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        if EVENT_FILE.exists() and EVENT_FILE.stat().st_size > 5_000_000:
            EVENT_FILE.replace(EVENT_FILE.with_suffix(".jsonl.1"))
        with EVENT_FILE.open("a") as f:
            f.write(json.dumps({"ts": round(time.time(), 3), "kind": kind, **data}, default=str) + "\n")
    except Exception:  # noqa: BLE001
        pass


def say(who: str, kind: str, text: str, tone: str = "wait") -> None:
    if who == "winston":
        st["now_thinking"] = text
    if st["last_kind"].get(who) == kind:
        return
    st["last_kind"][who] = kind
    st["chat"].append({"ts": time.time(), "who": who, "text": text, "tone": tone})
    del st["chat"][:-80]


def think(kind: str, text: str, tone: str = "wait") -> None:
    say("winston", kind, text, tone)


def pc(p: float) -> str:
    return f"{p * 100:.0f}%"


def window_of(t: float) -> int:
    return int(t // WINDOW) * WINDOW


norm_cdf = strat.norm_cdf


def est_probs(move_pct: float, left: float):
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


def hist_price_at(ts: int) -> Optional[float]:
    """Raw Coinbase price at a window boundary, from the per-second history."""
    for t, p in st["history"]:
        if ts - 2 <= t <= ts + 1:
            return p
    return None


def btc_adj() -> Optional[float]:
    """Coinbase price shifted by this window's basis so it lines up with the market's target source."""
    return None if st["btc"] is None else st["btc"] + st["basis"]


def odds_drop(side: str, secs: float) -> float:
    """How many points this side's price fell over the last `secs` seconds (falling-knife detector)."""
    now, k, cur, past = time.time(), (1 if side == "UP" else 2), None, None
    for rec in reversed(st["mid_hist"]):
        if cur is None:
            cur = rec[k]
        if now - rec[0] >= secs:
            past = rec[k]
            break
    return (past - cur) if past is not None and cur is not None else 0.0


fill_px = strat.fill_px


def to_plain(o: Any) -> Any:
    for n in ("model_dump", "dict"):
        f = getattr(o, n, None)
        if callable(f):
            try:
                return f()
            except Exception:  # noqa: BLE001
                pass
    return o if isinstance(o, (dict, list, str, int, float)) else getattr(o, "__dict__", {})


def _num(v: Any) -> Optional[float]:
    if isinstance(v, dict):
        return _num(v.get("value", v.get("amount")))
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def find_num(obj: Any, names: set, depth: int = 0) -> Optional[float]:
    """Search a nested SDK response for the first numeric field whose name is in `names`."""
    if depth > 4:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower().replace("_", "") in names:
                n = _num(v)
                if n is not None:
                    return n
        for v in obj.values():
            r = find_num(v, names, depth + 1)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj[:20]:
            r = find_num(v, names, depth + 1)
            if r is not None:
                return r
    return None


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


def set_cfg(key: str, value: Any) -> None:
    cfg[key] = clean_value(key, value)
    cfg_meta["version"] += 1
    cfg_meta["updated"] = time.time()


def save_state() -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        payload = {"schema": SCHEMA, "trades": st["trades"][-2000:], "config": {**cfg, "live_trading": False}}
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
            stale = data.get("schema") != SCHEMA
            if stale:
                add_log("Old saved settings found: exit/entry limits reset to the new defaults.")
            for k, v in (data.get("config") or {}).items():
                if k in cfg and k != "live_trading" and not (stale and k in RESET_KEYS):
                    try:
                        cfg[k] = clean_value(k, v)
                    except Exception:  # noqa: BLE001
                        pass
            add_log(f"Loaded {len(st['trades'])} past trades from disk.")
    except Exception as e:  # noqa: BLE001
        add_log(f"Could not load saved state: {e}")
    cfg["live_trading"] = False  # always boot into paper mode


# --------------------------------------------------------------- live status --
def creds():
    if st["creds"]:
        return st["creds"]
    k, s = os.environ.get("POLYMARKET_KEY_ID", ""), os.environ.get("POLYMARKET_SECRET_KEY", "")
    return (k, s) if k and s else None


def linked() -> bool:
    return POLY_SDK and creds() is not None


def live_ready():
    if not POLY_SDK:
        return False, "polymarket-us SDK is not installed"
    if creds() is None:
        return False, "Polymarket keys are not set (env vars or /account/link)"
    if not ADMIN_TOKEN_ENV:
        return False, "set an ADMIN_TOKEN environment variable first"
    return True, "ready"


def live_pnl_today() -> float:
    today = datetime.now(TZ).date()
    return sum(t["pnl"] for t in st["trades"] if t.get("mode") == "live" and trade_date(t) == today)


def loss_budget_left() -> float:
    return cfg["max_daily_loss"] + live_pnl_today()


def check_breaker() -> None:
    if cfg["live_trading"] and live_pnl_today() <= -cfg["max_daily_loss"]:
        cfg["live_trading"] = False
        cfg_meta["version"] += 1
        save_state()
        add_log("CIRCUIT BREAKER: daily live loss limit hit. Switched back to PAPER mode.")
        log_event("breaker", pnl_today=live_pnl_today(), limit=cfg["max_daily_loss"])
        think("breaker", "🛑 We hit today's loss limit, so live trading is OFF. "
                         "I'm in paper mode and still watching.", "loss")


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


# ================================================================== QUOTES ==
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


def ws_quote(slug: str, now: float) -> Optional[Dict[str, Any]]:
    """Primary source: live Polymarket order-book stream (best bid/ask for both contracts)."""
    if now - st["pm_last_msg"] > FRESH_PM_LINK:
        return None
    tok = (st["gamma"].get(slug) or {}).get("tokens") or {}
    u, d = st["books"].get(tok.get("up")), st["books"].get(tok.get("down"))
    if not u or not d:
        return None
    ub, ua, db, da = u.get("bid"), u.get("ask"), d.get("bid"), d.get("ask")
    if not strat.valid_book(ub, ua, db, da, PAIR_TOL):
        return None  # one-sided, crossed, or Up/Down disagree with each other: treat as bad data, use REST
    return {"up": round((ub + ua) / 2, 4), "down": round((db + da) / 2, 4),
            "up_bid": ub, "up_ask": ua, "down_bid": db, "down_ask": da,
            "source": "polymarket-ws", "age": round(now - min(u["ts"], d["ts"]), 1)}


def get_quote(left: float, move_pct: float) -> Dict[str, Any]:
    now = time.time()
    slug = f"btc-updown-5m-{st['window_start']}"
    q = ws_quote(slug, now)
    if q:
        return q
    c = st["clob"]
    if c and c["slug"] == slug and now - c["fetched"] <= FRESH_CLOB and abs(c["up"] + c["down"] - 1) <= PAIR_TOL:
        return {"up": c["up"], "down": c["down"], "source": "polymarket-live", "age": round(now - c["fetched"], 1)}
    t = terry_fresh()
    if t and now - t["fetched"] <= FRESH_TERRY and terry_matches(t, left):
        pr = t["item"].get("prices") or {}
        up, dn = norm_price(pr.get("up")), norm_price(pr.get("down"))
        if up is not None and dn is not None and abs(up + dn - 1) <= PAIR_TOL:
            return {"up": up, "down": dn, "source": "terrycalls", "age": round(now - t["fetched"], 1)}
    g = st["gamma"].get(slug)
    if (g and now - g["fetched"] <= FRESH_GAMMA and g["up"] is not None and g["down"] is not None
            and abs(g["up"] + g["down"] - 1) <= PAIR_TOL):
        return {"up": g["up"], "down": g["down"], "source": "polymarket", "age": round(now - g["fetched"], 1)}
    up, dn = est_probs(move_pct, left)
    return {"up": round(up, 3), "down": round(dn, 3), "source": "estimated", "age": 0.0}


def signal_snapshot(move: float, left: float) -> Dict[str, Any]:
    src = cfg["signal_source"]
    if src == "momentum":
        return strat.momentum_signal(cfg, move, left, EST_VOL_PCT)

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
    t, ws = st["terry"], st["window_start"]
    if not t or ws is None:
        return
    if not terry_matches(t, WINDOW - (time.time() - ws)):
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


def adopt_price_to_beat() -> None:
    """Terry's price-to-beat is the market's real target. Coinbase and the market's price source differ by
    a few dollars (the "basis"), so we measure that offset at the window boundary and shift our price by it.
    That keeps the chart starting ON the target and the gap maths honest."""
    t, ws = st["terry"], st["window_start"]
    if not t or ws is None or st["open_source"].startswith("terrycalls"):
        return
    if not terry_matches(t, WINDOW - (time.time() - ws)):
        return
    try:
        ptb = float(t["item"].get("price_to_beat"))
    except (TypeError, ValueError):
        return
    cb0 = hist_price_at(ws)
    if ptb <= 0 or cb0 is None or abs(ptb - cb0) / cb0 * 100 > 0.15:
        return
    st["window_open"], st["basis"] = ptb, ptb - cb0
    st["open_source"], st["partial"] = "terrycalls price-to-beat", False
    add_log(f"Target {ptb:,.2f} from Terry; basis vs Coinbase {ptb - cb0:+.2f}")


# ============================================================ LIVE (SDK) ==
def get_client():
    if st["client"] is None:
        k, s = creds()
        st["client"] = PolymarketUS(key_id=k, secret_key=s)
    return st["client"]


def live_slug() -> str:
    ws = st["window_start"] or window_of(time.time())
    t = terry_fresh()
    if t:
        m = t["item"].get("market")
        if isinstance(m, str) and re.fullmatch(r"[a-z0-9][a-z0-9\-_]{5,100}", m) and m.endswith(str(ws)):  # must be THIS window
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
    if slug not in st["slug_ok"]:  # validated ahead of time by the pre-warm loop when possible
        client.markets.retrieve_by_slug(slug)
        st["slug_ok"].add(slug)
    limit = min(0.99, round(price + LIVE_SLIPPAGE, 2))
    resp = client.orders.create({
        "marketSlug": slug, "intent": LIVE_INTENTS[(side, "open")], "type": "ORDER_TYPE_LIMIT",
        "price": {"value": f"{limit:.2f}", "currency": "USD"}, "quantity": int(qty), "tif": LIVE_TIF,
    })
    return {"order_id": order_id(resp), "slug": slug, "limit": limit}


def live_cancel(oid: Optional[str], slug: str) -> None:
    if oid:
        try:
            get_client().orders.cancel(oid, {"marketSlug": slug})
        except Exception:  # noqa: BLE001
            pass


def live_fill_qty(oid: Optional[str], want: int) -> Optional[float]:
    """Best-effort fill check. Returns filled quantity, or None if the SDK can't tell us."""
    if not oid:
        return None
    client, got, end = get_client(), None, time.time() + LIVE_FILL_WAIT
    while time.time() < end:
        try:
            fn = getattr(client.orders, "retrieve", None) or getattr(client.orders, "get", None)
            if fn is None:
                return None
            q = find_num(to_plain(fn(oid)), {"cumquantity", "filledquantity", "filled", "executedquantity", "cumqty"})
            if q is not None:
                got = q
                if q >= want:
                    return q
        except Exception:  # noqa: BLE001
            return got
        time.sleep(0.3)
    return got


def live_sell(pos: Dict[str, Any], price: float) -> Dict[str, Any]:
    client = get_client()
    limit = max(0.01, round(price - LIVE_SLIPPAGE, 2))
    resp = client.orders.create({
        "marketSlug": pos["slug"], "intent": LIVE_INTENTS[(pos["side"], "close")], "type": "ORDER_TYPE_LIMIT",
        "price": {"value": f"{limit:.2f}", "currency": "USD"}, "quantity": int(pos["shares"]), "tif": LIVE_TIF,
    })
    live_cancel(pos.get("order_id"), pos["slug"])  # clean up any unfilled remainder of the entry
    return {"order_id": order_id(resp), "limit": limit}


# ============================================================== ACCOUNT ==
def fetch_account() -> Dict[str, Any]:
    """Pull cash / buying power / portfolio value. The SDK's method and field names aren't documented
    in this repo, so several likely call paths and field names are tried. state['balance']['fields']
    lists the top-level keys returned, so a wrong mapping is easy to spot on the first run."""
    client, raw, last = get_client(), None, None
    for path in ("account.balances", "account.balance", "accounts.balances", "portfolio.balances"):
        try:
            obj = client
            for part in path.split("."):
                obj = getattr(obj, part)
            raw = to_plain(obj() if callable(obj) else obj)
            break
        except Exception as e:  # noqa: BLE001
            last = e
    if raw is None:
        raise RuntimeError(f"no balance method found on the SDK ({last})")
    return {
        "cash": find_num(raw, {"cash", "cashbalance", "availablebalance", "available", "balance", "currentbalance"}),
        "buying_power": find_num(raw, {"buyingpower", "availabletotrade", "availablebuyingpower"}),
        "portfolio_value": find_num(raw, {"portfoliovalue", "totalvalue", "equity", "netliquidation", "totalbalance"}),
        "fields": sorted(raw.keys())[:12] if isinstance(raw, dict) else [],
    }


def spendable() -> Optional[float]:
    a = st["account"]
    if a.get("status") != "ok" or time.time() - a.get("ts", 0) > 120:
        return None
    return a.get("buying_power") if a.get("buying_power") is not None else a.get("cash")


async def account_task() -> None:
    """Keeps the balance fresh and pre-warms the live client + the next markets (so entries are instant)."""
    while True:
        if linked():
            try:
                data = await asyncio.to_thread(fetch_account)
                st["account"] = {**data, "ts": time.time(), "status": "ok", "error": ""}
                ws = window_of(time.time())
                for w in (ws, ws + WINDOW):
                    slug = LIVE_SLUG_TEMPLATE.format(ts=w)
                    if slug not in st["slug_ok"]:
                        try:
                            await asyncio.to_thread(lambda s=slug: get_client().markets.retrieve_by_slug(s))
                            st["slug_ok"].add(slug)
                        except Exception:  # noqa: BLE001
                            pass
                if len(st["slug_ok"]) > 40:
                    st["slug_ok"].clear()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                st["account"] = {**st["account"], "status": "error", "error": str(e)[:200]}
        else:
            st["account"] = {"status": "not linked"}
        try:
            await asyncio.wait_for(st["acct_evt"].wait(), ACCOUNT_POLL_SEC)
        except asyncio.TimeoutError:
            pass
        st["acct_evt"].clear()


# ============================================================== TRADING ==
def live_stake(price: float) -> float:
    """Hard-capped live stake: setting, hard cap, % of balance, and what's left of today's loss budget."""
    stake = min(cfg["stake_usd"], cfg["max_live_stake"], loss_budget_left())
    bal = spendable()
    if bal is not None:
        stake = min(stake, bal * 0.98)
        if cfg["max_stake_pct_balance"] > 0:
            stake = min(stake, bal * cfg["max_stake_pct_balance"] / 100.0)
    return stake


async def open_position(side: str, quote: Dict[str, Any], *, manual: bool, confidence: Optional[float],
                        move_pct: float, stake: Optional[float] = None, why: str = "") -> Dict[str, Any]:
    async with st["lock"]:
        if st["position"] is not None:
            raise RuntimeError("a position is already open")
        mode = "live" if cfg["live_trading"] else "paper"
        price = fill_px(quote, side, "buy")
        stake = float(stake if stake is not None else cfg["stake_usd"])
        order: Dict[str, Any] = {}
        slug = f"btc-updown-5m-{st['window_start']}"
        if mode == "live":
            ok, reason = live_ready()
            if not ok:
                raise RuntimeError(f"live trading not ready: {reason}")
            if quote["source"] == "estimated":
                raise RuntimeError("no real market price available, so I won't place a live order")
            check_breaker()
            if not cfg["live_trading"]:
                raise RuntimeError("daily loss limit reached")
            stake = min(stake, live_stake(price))
            qty = int(stake // price)
            if qty < 1:
                raise RuntimeError("stake is too small to buy one contract (check balance and loss budget)")
            slug = live_slug()
            order = await asyncio.to_thread(live_buy, slug, side, price, qty)
            shares: float = qty
            filled = await asyncio.to_thread(live_fill_qty, order.get("order_id"), qty)
            if filled is None:
                add_log("WARNING: could not verify the fill with the SDK, assuming a full fill.")
            elif filled <= 0:
                await asyncio.to_thread(live_cancel, order.get("order_id"), slug)
                raise RuntimeError("entry order did not fill in time, cancelled")
            elif filled < qty:
                await asyncio.to_thread(live_cancel, order.get("order_id"), slug)
                shares = filled
        else:
            shares = stake / price
        now = time.time()
        ws = st["window_start"]
        st["win_trades"][ws] = st["win_trades"].get(ws, 0) + 1
        pos = {
            "id": f"t{int(now * 1000)}", "mode": mode, "side": side, "slug": slug, "window_start": ws,
            "opened_at": now, "entry": round(price, 4), "shares": shares, "stake": round(shares * price, 2),
            "price_source": quote["source"], "btc_open": st["window_open"], "btc_at_entry": btc_adj(),
            "move_pct_at_entry": round(move_pct, 4), "signal_source": "manual" if manual else cfg["signal_source"],
            "confidence": confidence, "manual": manual, "order_id": order.get("order_id"),
            "fee_pct": cfg["fee_pct"], "peak": round(price, 4), "inval_since": None,
            "n_in_window": st["win_trades"][ws],
        }
        st["position"] = pos
        add_log(f"[{mode.upper()}] BUY {side} @ {price:.2f} x{shares:.2f} (${pos['stake']:.2f}) "
                f"#{pos['n_in_window']} {'MANUAL' if manual else cfg['signal_source']}")
        log_event("entry", **{k: pos[k] for k in ("id", "mode", "side", "entry", "shares", "stake", "price_source",
                                                   "move_pct_at_entry", "confidence", "signal_source",
                                                   "n_in_window")}, why=why)
        think(f"entered{pos['id']}", f"🟢 {why or 'You asked for this one.'} Buying {side} with "
                                     f"${pos['stake']:.2f} [{mode.upper()}].", "go")
        return pos


def note_streak(pos: Dict[str, Any], pnl: float) -> None:
    """Losing-streak breaker: N losing trades in a row -> sit out the next M full rounds."""
    if pos.get("manual"):
        return
    if pnl > 0:
        st["loss_streak"] = 0
        return
    st["loss_streak"] += 1
    n, w = int(cfg["loss_streak_pause"]), int(cfg["streak_pause_windows"])
    if w > 0 and st["loss_streak"] >= n:
        st["pause_until"] = pos["window_start"] + (1 + w) * WINDOW
        add_log(f"{st['loss_streak']} losses in a row: standing down for {w} round(s).")
        log_event("streak_pause", losses=st["loss_streak"], rounds=w)


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
                log_event("error", where="live_sell", error=str(e))
                think("closefail", f"⚠️ I couldn't close the live position ({e}). I'll keep retrying.", "loss")
                if raise_on_fail:
                    raise HTTPException(status_code=502, detail=f"Live close failed: {e}")
                return None
        fee = pos["stake"] * pos["fee_pct"] / 100.0
        pnl = pos["shares"] * (price - pos["entry"]) - fee
        now = time.time()
        trade = {**pos, "exit": round(price, 4), "reason": reason, "pnl": round(pnl, 4), "fee": round(fee, 4),
                 "closed_at": now, "held_sec": round(now - pos["opened_at"], 1), "btc_at_exit": btc_adj(),
                 "close_order_id": info.get("order_id"), "settled": settle}
        st["trades"].append(trade)
        del st["trades"][:-2000]
        st["position"] = None
        st["last_exit_ts"] = now
        st["last_exit_loss"] = pnl < 0 and not settle
        note_streak(pos, pnl)
        if pnl < 0 and not settle:  # a loss: that side is off-limits for the rest of this window
            w = pos["window_start"]
            st["win_losses"][w] = st["win_losses"].get(w, 0) + 1
            st["side_block"].setdefault(w, set()).add(pos["side"])
        save_state()
        add_log(f"[{pos['mode'].upper()}] SELL {pos['side']} @ {price:.2f} ({reason}) pnl ${pnl:+.2f}")
        log_event("exit", id=pos["id"], mode=pos["mode"], side=pos["side"], exit=trade["exit"], reason=reason,
                  pnl=trade["pnl"], held_sec=trade["held_sec"], settled=settle)
        think(f"exit{trade['id']}", f"{'✅' if pnl > 0 else '❌'} Sold {pos['side']} at {pc(price)} ({reason}). "
                                    f"{pnl:+.2f} USD after estimated fees.", "win" if pnl > 0 else "loss")
        if pos["mode"] == "live":
            st["acct_evt"].set()
            check_breaker()
        return trade


def price_at(ts: float) -> Optional[float]:
    """Last tick at or before `ts` (so window boundaries use the true boundary price, not a late tick)."""
    best = None
    for t, p in st["ticks"]:
        if t <= ts:
            best = (t, p)
        else:
            break
    return best[1] if best and ts - best[0] <= 5 else None


async def on_new_window(ws: int, elapsed: float, btc: float) -> None:
    pos = st["position"]
    if pos is not None and st["window_start"] is not None:
        q = st["last_quote"] or {}
        mid = q.get(pos["side"].lower())
        if q.get("source") not in (None, "estimated") and mid is not None and (mid >= 0.9 or mid <= 0.1):
            won = mid >= 0.9  # the market's own final price is the best read on the real settlement
        else:
            final = (price_at(ws) or btc) + st["basis"]
            won = (pos["side"] == "UP") == (final >= (st["window_open"] or final))
        await close_position(1.0 if won else 0.0, f"window expired ({'won' if won else 'lost'})", settle=True)
    st["window_start"] = ws
    for d in (st["win_trades"], st["win_losses"], st["side_block"]):
        for k in [k for k in d if k < ws - 3 * WINDOW]:
            del d[k]
    st["basis"] = 0.0
    st["side_since"] = {"side": None, "since": time.time()}
    cb0 = price_at(ws) if elapsed <= 3 else hist_price_at(ws)
    open_price, source, partial = (cb0 if cb0 is not None else btc), "live feed", (cb0 is None and elapsed > 3)
    st["window_open"], st["open_source"], st["partial"] = open_price, source, partial
    add_log(f"New window {ws}: target {open_price:,.2f} ({source})" + (" - joined late, sitting out" if partial else ""))
    say("sys", f"w{ws}", f"New 5-minute window · target ${open_price:,.2f}", "sys")
    adopt_price_to_beat()


async def manage_position(q: Dict[str, Any], elapsed: float, move: float) -> None:
    """Exits are driven by the target-to-BTC gap first, contract price second (rules live in strategy.py)."""
    pos, now = st["position"], time.time()
    side = pos["side"]
    mid, bid = q[side.lower()], fill_px(q, side, "sell")
    same = price_class(q["source"]) == price_class(pos["price_source"])
    reason, state = strat.exit_reason(cfg, pos, q, elapsed, move, now, same)
    if reason:
        if now - st["close_fail_ts"] >= 3:
            await close_position(bid, reason)
        return
    edge = move if side == "UP" else -move
    think(f"hold{pos['id']}:{state}", f"{state.capitalize()} {side} from {pc(pos['entry'])}, now {pc(mid)}. "
                                     f"BTC is {edge:+.3f}% on my side of the target. I exit if the gap flips "
                                     f"for {cfg['inval_hold_sec']:.0f}s, not on price noise.", "info")


def evaluate_entry(q: Dict[str, Any], elapsed: float, move: float):
    """Builds the context snapshot and runs strategy.evaluate_entry (the same function the backtester uses).
    Returns (side, p_win, edge, gates)."""
    ws, now = st["window_start"], time.time()
    if st["pause_until"] and now >= st["pause_until"]:
        st["pause_until"], st["loss_streak"] = 0.0, 0
        add_log("Losing-streak pause is over. Trading again.")
    sig = st["signal"]
    ss = st["side_since"]
    basis, target = st["basis"], st["window_open"]
    prices = [p[1] + basis for p in st["history"][-150:] if p[0] >= ws]
    ctx = {
        "elapsed": elapsed, "move": move, "signal": sig, "quote": q, "prices": prices, "target": target,
        "side_held": (now - ss["since"]) if ss["side"] == sig.get("side") and sig.get("side") else 0.0,
        "odds_drop": {s: odds_drop(s, cfg["knife_window_sec"]) for s in ("UP", "DOWN")},
        "n_trades": st["win_trades"].get(ws, 0), "n_losses": st["win_losses"].get(ws, 0),
        "last_exit_loss": st["last_exit_loss"], "since_exit": now - st["last_exit_ts"],
        "side_blocked": st["side_block"].get(ws, ()), "position": st["position"] is not None,
        "paused": cfg["paused"], "partial": st["partial"], "entry_blocked": now < st["entry_block_until"],
        "pause_left": max(0.0, st["pause_until"] - now), "live": cfg["live_trading"], "est_vol_pct": EST_VOL_PCT,
    }
    side, p, edge, G, info = strat.evaluate_entry(cfg, ctx)
    st["eval"] = info
    return side, p, edge, G


async def consider_entry(q: Dict[str, Any], elapsed: float, move: float, res) -> None:
    side, p, edge, G = res
    ws, now = st["window_start"], time.time()
    bad = next((g for g in G if not g["ok"]), None)
    if bad:
        return think(f"gate:{bad['key']}:{side}", f"Waiting. {bad['label']}." + (f" {bad['note']}" if bad["note"] else ""))
    price = fill_px(q, side, "buy")
    why = (f"Every gate passed: {side} held {cfg['confirm_sec']:.0f}s+, market {pc(q[side.lower()])}, win chance "
           f"{p * 100:.0f}% vs cost {price * (1 + cfg['fee_pct'] / 100) * 100:.0f}% (+{edge * 100:.1f} pts).")
    try:
        await open_position(side, q, manual=False, confidence=st["signal"]["confidence"], move_pct=move, why=why)
    except Exception as e:  # noqa: BLE001
        st["entry_block_until"] = time.time() + 10
        add_log(f"Entry failed: {e}")
        log_event("error", where="entry", error=str(e))
        think(f"entryfail{ws}:{int(now)}", f"⚠️ I wanted to buy {side} but it failed: {e}", "loss")


def record_tick(ts: int, btc: float, q: Dict[str, Any]) -> None:
    """One JSON line per second with everything the strategy saw, so backtest.py can replay REAL market data.
    Buffered and flushed every 20s; file rotates at 40 MB."""
    if not RECORD_TICKS:
        return
    st["tick_buf"].append(json.dumps({
        "ts": ts, "ws": st["window_start"], "btc": round(btc, 2), "basis": round(st["basis"], 2),
        "target": st["window_open"], "up": q.get("up"), "down": q.get("down"), "ub": q.get("up_bid"),
        "ua": q.get("up_ask"), "db": q.get("down_bid"), "da": q.get("down_ask"), "src": q.get("source"),
        "partial": st["partial"]}, separators=(",", ":")))
    if len(st["tick_buf"]) >= 20:
        buf, st["tick_buf"] = st["tick_buf"], []
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            if TICK_FILE.exists() and TICK_FILE.stat().st_size > 40_000_000:
                TICK_FILE.replace(TICK_FILE.with_suffix(".jsonl.1"))
            with TICK_FILE.open("a") as f:
                f.write("\n".join(buf) + "\n")
        except Exception:  # noqa: BLE001
            pass


async def tick() -> None:
    now, btc = time.time(), st["btc"]
    if btc is None or now - st["btc_ts"] > 20:
        think("nofeed", "The BTC price feed is down or stale. I won't trade blind.")
        return
    ts, h = int(now), st["history"]
    new_second = not h or h[-1][0] != ts
    if new_second:
        h.append([ts, btc])
        del h[:-1800]
    else:
        h[-1][1] = btc
    ws = window_of(now)
    elapsed = now - ws
    if st["window_start"] != ws:
        await on_new_window(ws, elapsed, btc)
    open_price = st["window_open"]
    move = (btc + st["basis"] - open_price) / open_price * 100.0
    q = get_quote(WINDOW - elapsed, move)
    st["last_quote"], st["last_move"] = q, move
    if new_second:
        record_tick(ts, btc, q)
    if q["source"] != "estimated":
        st["mid_hist"].append((now, q["up"], q["down"]))
    if new_second:
        total = q["up"] + q["down"]
        st["odds_hist"].append([ts, round(q["up"] / total * 100.0, 1)])
        del st["odds_hist"][:-900]
    st["signal"] = signal_snapshot(move, WINDOW - elapsed)
    lean = "UP" if move >= cfg["min_btc_move"] else "DOWN" if move <= -cfg["min_btc_move"] else None
    if st["side_since"]["side"] != lean:
        st["side_since"] = {"side": lean, "since": now}
    res = evaluate_entry(q, elapsed, move)
    st["gates"] = res[3]
    if st["position"] is not None:
        await manage_position(q, elapsed, move)
    else:
        await consider_entry(q, elapsed, move, res)


# =========================================================== BACKGROUND ==
def on_price(p: float, source: str, ex_ts: Optional[float] = None) -> None:
    t = time.time()
    st["btc"], st["btc_ts"], st["feed_source"] = p, t, source
    st["ticks"].append((t, p))
    if ex_ts:
        lat = (t - ex_ts) * 1000.0
        st["latency_ms"] = lat if st["latency_ms"] is None else st["latency_ms"] * 0.9 + lat * 0.1
    st["evt"].set()  # wake the engine NOW: this is what makes entries instant


async def feed_task() -> None:
    delay = 1.0
    while True:
        try:
            async with websockets.connect("wss://ws-feed.exchange.coinbase.com", ping_interval=20,
                                          ping_timeout=20, max_size=2 ** 20) as ws:
                await ws.send(json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}))
                async for raw in ws:
                    m = json.loads(raw)
                    if m.get("type") == "ticker" and m.get("price"):
                        delay = 1.0
                        ex = None
                        try:
                            ex = datetime.fromisoformat(m["time"].replace("Z", "+00:00")).timestamp()
                        except Exception:  # noqa: BLE001
                            pass
                        on_price(float(m["price"]), "coinbase-ws", ex)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            st["feed_source"] = f"reconnecting ({type(e).__name__})"
        await asyncio.sleep(delay)
        delay = min(15.0, delay * 2)  # exponential backoff


def fetch_spot() -> float:
    r = requests.get("https://api.coinbase.com/v2/prices/BTC-USD/spot", timeout=5)
    r.raise_for_status()
    return float(r.json()["data"]["amount"])


async def rest_task() -> None:
    """Fallback only: used while the WebSocket is silent for >4s."""
    while True:
        await asyncio.sleep(2)
        if time.time() - st["btc_ts"] > 4:
            try:
                on_price(await asyncio.to_thread(fetch_spot), "coinbase-rest")
            except Exception:  # noqa: BLE001
                pass


def pm_update(asset: str, bid: Optional[float] = None, ask: Optional[float] = None) -> None:
    b = st["books"].setdefault(asset, {})
    if bid is not None:
        b["bid"] = bid
    if ask is not None:
        b["ask"] = ask
    b["ts"] = time.time()


def _f(v: Any) -> Optional[float]:
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def pm_handle(m: Dict[str, Any]) -> None:
    et = m.get("event_type")
    if et == "book" and m.get("asset_id"):
        bids = [float(x["price"]) for x in m.get("bids", []) if float(x.get("size", 0)) > 0]
        asks = [float(x["price"]) for x in m.get("asks", []) if float(x.get("size", 0)) > 0]
        pm_update(m["asset_id"], max(bids) if bids else 0.0, min(asks) if asks else 1.0)
    elif et == "price_change":
        for c in m.get("price_changes", []):
            if c.get("asset_id"):
                pm_update(c["asset_id"], _f(c.get("best_bid")), _f(c.get("best_ask")))
    elif et == "best_bid_ask" and m.get("asset_id"):
        pm_update(m["asset_id"], _f(m.get("best_bid")), _f(m.get("best_ask")))
    else:
        return
    st["evt"].set()


def pm_tokens() -> set:
    out = set()
    for g in st["gamma"].values():
        out.update((g.get("tokens") or {}).values())
    return out


async def pm_ws_task() -> None:
    """Primary odds source: Polymarket order-book stream. Re-subscribes as new windows' markets appear."""
    delay = 1.0
    while True:
        helpers = []
        try:
            async with websockets.connect(PM_WS_URL, ping_interval=None, max_size=2 ** 22) as ws:
                st["pm_sub"] = set()
                st["pm_status"] = "connected"

                async def pinger():
                    while True:
                        await asyncio.sleep(10)
                        await ws.send("PING")

                async def subscriber():
                    first = True
                    while True:
                        new = pm_tokens() - st["pm_sub"]
                        if new:
                            msg = ({"assets_ids": sorted(new), "type": "market"} if first
                                   else {"assets_ids": sorted(new), "operation": "subscribe"})
                            await ws.send(json.dumps(msg))
                            st["pm_sub"] |= new
                            first = False
                        await asyncio.sleep(0.5)

                helpers = [asyncio.create_task(pinger()), asyncio.create_task(subscriber())]
                async for raw in ws:
                    st["pm_last_msg"] = time.time()
                    delay = 1.0
                    if raw in ("PONG", b"PONG"):
                        continue
                    try:
                        data = json.loads(raw)
                    except Exception:  # noqa: BLE001
                        continue
                    for m in (data if isinstance(data, list) else [data]):
                        if isinstance(m, dict):
                            pm_handle(m)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            st["pm_status"] = f"reconnecting ({type(e).__name__})"
        finally:
            for t in helpers:
                t.cancel()
        await asyncio.sleep(delay)
        delay = min(15.0, delay * 2)


def fetch_terry() -> Dict[str, Any]:
    r = requests.get("https://terrycalls.com/api/v1/calls", params={"horizon": "5m"},
                     headers={"X-API-Key": os.environ["TERRY_API_KEY"].strip(), "Accept": "application/json"}, timeout=6)
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
            item = next((m for m in data.get("markets", []) if str(m.get("horizon", "")).lower().startswith("5")), None)
            if item is None:
                st["terry_status"] = "no 5m market in response"
            else:
                st["terry"] = {"item": item, "fetched": time.time(), "stale": bool(data.get("stale"))}
                st["terry_status"] = "stale" if data.get("stale") else "ok"
                terry_announce()
                adopt_price_to_beat()
                st["evt"].set()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            st["terry_status"] = f"error: {e}"
            if "rate limited" in str(e):
                delay = 30
        await asyncio.sleep(delay)


SIDE_WORDS = {"up": "up", "yes": "up", "higher": "up", "down": "down", "no": "down", "lower": "down"}


def _jl(v: Any) -> list:
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except Exception:  # noqa: BLE001
            return []
    return list(v or [])


def fetch_gamma(slug: str) -> Dict[str, Any]:
    """Parse Polymarket's event JSON into {up, down, tokens}. Outcome names are mapped by label (never by
    position), closed markets are skipped, and an event whose slug doesn't match is rejected."""
    r = requests.get("https://gamma-api.polymarket.com/events", params={"slug": slug}, timeout=6)
    r.raise_for_status()
    data = r.json()
    ev = next((e for e in data if e.get("slug") == slug), None)
    if ev is None:
        raise RuntimeError("market not listed yet")
    for market in ev.get("markets", []):
        if market.get("closed"):
            continue
        outcomes, prices, ids = _jl(market.get("outcomes")), _jl(market.get("outcomePrices")), _jl(market.get("clobTokenIds"))
        sides = [SIDE_WORDS.get(str(o).strip().lower()) for o in outcomes]
        if set(sides) != {"up", "down"} or len(prices) != len(sides):
            continue
        px = {sd: norm_price(p) for sd, p in zip(sides, prices)}
        tokens = {sd: str(i) for sd, i in zip(sides, ids)} if len(ids) == len(sides) else {}
        return {"slug": slug, "up": px.get("up"), "down": px.get("down"), "tokens": tokens, "fetched": time.time()}
    raise RuntimeError("no open Up/Down market in event")


async def gamma_task() -> None:
    """Caches current AND next window's market so token ids are known (and subscribed) before the roll."""
    while True:
        base = window_of(time.time())
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
                    for tok in (st["gamma"].pop(k).get("tokens") or {}).values():
                        st["books"].pop(tok, None)
            except Exception:  # noqa: BLE001
                st["gamma"].pop(k, None)
        await asyncio.sleep(3)


def fetch_mid(token: str) -> Optional[float]:
    r = requests.get("https://clob.polymarket.com/midpoint", params={"token_id": token}, timeout=4)
    r.raise_for_status()
    return norm_price(r.json().get("mid"))


async def clob_task() -> None:
    """REST midpoint fallback. Skips itself while the order-book stream is healthy."""
    while True:
        try:
            slug = f"btc-updown-5m-{window_of(time.time())}"
            if ws_quote(slug, time.time()):
                st["clob_status"] = "idle (ws ok)"
            else:
                tok = (st["gamma"].get(slug) or {}).get("tokens") or {}
                if tok.get("up") and tok.get("down"):
                    up, dn = await asyncio.gather(asyncio.to_thread(fetch_mid, tok["up"]),
                                                  asyncio.to_thread(fetch_mid, tok["down"]))
                    if up is not None and dn is not None:
                        st["clob"] = {"slug": slug, "up": up, "down": dn, "fetched": time.time()}
                        st["clob_status"] = "ok"
                        st["evt"].set()
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
    """Event-driven: wakes on every price / order-book update, with a 250ms heartbeat for timers."""
    add_log(f"{BOT_NAME}'s 24/7 engine started (paper mode).")
    while True:
        try:
            await asyncio.wait_for(st["evt"].wait(), 0.25)
        except asyncio.TimeoutError:
            pass
        st["evt"].clear()
        try:
            await tick()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            add_log(f"Engine error: {type(e).__name__}: {e}")
            log_event("error", where="engine", error=f"{type(e).__name__}: {e}")
        await asyncio.sleep(0.02)  # coalesce bursts of ticks


@asynccontextmanager
async def lifespan(_app: FastAPI):
    st["lock"], st["evt"], st["acct_evt"] = asyncio.Lock(), asyncio.Event(), asyncio.Event()
    load_state()
    say("sys", "boot", f"{BOT_NAME} is online · paper mode", "sys")
    if not ADMIN_TOKEN_ENV:
        print(f"\n*** ADMIN_TOKEN not set. Temporary dashboard password for this run: {ADMIN_TOKEN}\n"
              f"*** Set ADMIN_TOKEN in your environment to keep it (required for live trading).\n", flush=True)
    tasks = [asyncio.create_task(f()) for f in
             (feed_task, rest_task, pm_ws_task, terry_task, gamma_task, clob_task, account_task, engine)]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()


# ================================================================== API ==
app = FastAPI(title=f"{BOT_NAME} · 5-Minute BTC Up/Down Desk", version="7.0", lifespan=lifespan)
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
    max_stake_pct_balance: Optional[float] = None
    max_trades_per_window: Optional[float] = None
    reentry_cooldown_sec: Optional[float] = None
    inval_buffer_pct: Optional[float] = None
    inval_hold_sec: Optional[float] = None
    min_hold_sec: Optional[float] = None
    trail_pct: Optional[float] = None
    hold_to_settle_price: Optional[float] = None
    after_loss_cooldown_sec: Optional[float] = None
    max_window_losses: Optional[float] = None
    knife_pts: Optional[float] = None
    knife_window_sec: Optional[float] = None
    confirm_sec: Optional[float] = None
    min_edge: Optional[float] = None
    align_min_odds: Optional[float] = None
    max_spread: Optional[float] = None
    terry_weight: Optional[float] = None
    vol_lookback_sec: Optional[float] = None
    scalp_mode: Optional[bool] = None
    min_win_prob: Optional[float] = None
    chop_lookback_sec: Optional[float] = None
    max_chop_crosses: Optional[float] = None
    loss_streak_pause: Optional[float] = None
    streak_pause_windows: Optional[float] = None
    signal_source: Optional[str] = None
    winston_veto: Optional[bool] = None
    live_trading: Optional[bool] = None
    paused: Optional[bool] = None
    confirm_live: Optional[str] = None


class ManualTrade(BaseModel):
    side: str
    stake_usd: Optional[float] = None


class LinkAccount(BaseModel):
    key_id: str
    secret_key: str


def dump(model: BaseModel) -> Dict[str, Any]:
    fn = getattr(model, "model_dump", None) or model.dict
    return {k: v for k, v in fn().items() if v is not None}


def position_view() -> Optional[Dict[str, Any]]:
    pos = st["position"]
    if not pos:
        return None
    q = st["last_quote"] or {}
    k = pos["side"].lower()
    view = dict(pos)
    view["current"] = q.get(k)
    view["held_sec"] = round(time.time() - pos["opened_at"], 1)
    view["tp_price"] = round(min(0.99, pos["entry"] + cfg["take_profit"]), 2)
    view["sl_price"] = round(max(0.01, pos["entry"] - cfg["stop_loss"]), 2)
    if q.get(k) is not None:
        sell = fill_px(q, pos["side"], "sell")
        fee = pos["stake"] * pos["fee_pct"] / 100.0
        view["value"] = round(pos["shares"] * sell, 2)
        view["unrealized_pnl"] = round(pos["shares"] * (sell - pos["entry"]) - fee, 2)
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


def balance_view() -> Dict[str, Any]:
    a = st["account"]
    return {"linked": linked(), "status": a.get("status", "not linked"), "cash": a.get("cash"),
            "buying_power": a.get("buying_power"), "portfolio_value": a.get("portfolio_value"),
            "age": round(time.time() - a["ts"], 1) if a.get("ts") else None,
            "error": a.get("error", ""), "fields": a.get("fields", [])}


def build_state(since: Optional[int] = None) -> Dict[str, Any]:
    now = time.time()
    ws = st["window_start"] or window_of(now)
    left = max(0.0, WINDOW - (now - ws))
    open_price, btc = st["window_open"], btc_adj()
    q = st["last_quote"] or {}
    ok, why = live_ready()
    mode = "live" if cfg["live_trading"] else "paper"
    floor = ws - WINDOW
    full = since is None or since < floor
    lo = floor if full else since + 1
    up, dn = q.get("up"), q.get("down")
    tot = (up + dn) if up is not None and dn is not None else None
    n_win = st["win_trades"].get(ws, 0)
    return {
        "name": BOT_NAME, "server_time": now, "server_time_ms": int(now * 1000), "mode": mode,
        "btc_price": btc, "btc_raw": st["btc"], "basis": round(st["basis"], 2), "window_start": ws, "window_end": ws + WINDOW,
        "window_start_ms": ws * 1000, "window_end_ms": (ws + WINDOW) * 1000, "seconds_left": round(left, 3),
        "window_open": open_price, "open_source": st["open_source"], "partial": st["partial"],
        "delta_pct": round(st["last_move"], 4),
        "delta_usd": round(btc - open_price, 2) if btc and open_price else 0.0,
        "feed": {"source": st["feed_source"], "age": round(now - st["btc_ts"], 1) if st["btc_ts"] else None,
                 "latency_ms": round(st["latency_ms"]) if st["latency_ms"] is not None else None,
                 "polymarket_ws": st["pm_status"]},
        "prices": {"up": up, "down": dn, "source": q.get("source", "none"), "age": q.get("age"),
                   "up_bid": q.get("up_bid"), "up_ask": q.get("up_ask"),
                   "down_bid": q.get("down_bid"), "down_ask": q.get("down_ask"),
                   "norm_up": round(up / tot * 100, 1) if tot else None,
                   "norm_down": round(dn / tot * 100, 1) if tot else None},
        "signal": st["signal"], "gates": st["gates"], "eval": st["eval"],
        "terry": {"enabled": bool(os.environ.get("TERRY_API_KEY", "").strip()), "status": st["terry_status"]},
        "polymarket": {"status": st["gamma_status"], "live": st["clob_status"], "ws": st["pm_status"]},
        "position": position_view(),
        "scoreboard": scoreboard(mode),
        "thinking": st["now_thinking"],
        "chat": st["chat"][-60:],
        "recent_trades": list(reversed(st["trades"][-50:])),
        "markers": markers_view(floor),
        "history_full": full,
        "history": [[p[0], p[1] + st["basis"]] for p in st["history"] if p[0] >= lo],
        "odds_history": [p for p in st["odds_hist"] if p[0] >= lo],
        "config": cfg, "config_version": cfg_meta["version"],
        "live": {"enabled": cfg["live_trading"], "ready": ok, "reason": why, "error": st["live_error"],
                 "pnl_today": round(live_pnl_today(), 2), "slug": live_slug(),
                 "loss_budget_left": round(loss_budget_left(), 2)},
        "balance": balance_view(),
        "risk": {"trades_this_window": n_win, "max_trades_per_window": cfg["max_trades_per_window"],
                 "cooldown_left": round(max(0.0, cfg["reentry_cooldown_sec"] - (now - st["last_exit_ts"])), 1),
                 "loss_streak": st["loss_streak"], "pause_left": round(max(0.0, st["pause_until"] - now), 1)},
        "logs": st["logs"][:30],
    }


@app.get("/")
async def index():
    return FileResponse(BASE_DIR / "index.html")


@app.get("/health")
async def health():
    return {"ok": True, "name": BOT_NAME, "feed": st["feed_source"], "pm_ws": st["pm_status"],
            "mode": "live" if cfg["live_trading"] else "paper"}


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
                    set_cfg(key, True)
                    st["live_error"] = ""
                    st["acct_evt"].set()
                    add_log("*** LIVE TRADING ENABLED: REAL MONEY ***")
                    log_event("live_on", stake=cfg["stake_usd"], cap=cfg["max_live_stake"], limit=cfg["max_daily_loss"])
                    think("golive", "🔴 Live mode is ON. From here I'm placing real orders.", "go")
                else:
                    set_cfg(key, False)
                    add_log("Live trading switched OFF (paper mode).")
                    log_event("live_off")
                    think("gopaper", "Back to paper mode. No real orders from me.", "info")
            else:
                set_cfg(key, value)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    save_state()
    log_event("config", changed=data)
    return {"status": "success", "config": cfg, "config_version": cfg_meta["version"],
            "live_trading": cfg["live_trading"]}


@app.post("/cashout")
async def cashout(x_admin_token: str = Header(default="")):
    require_auth(x_admin_token)
    pos = st["position"]
    if not pos:
        raise HTTPException(status_code=400, detail="No active position to cash out.")
    q = st["last_quote"] or {}
    if q.get(pos["side"].lower()) is None:
        raise HTTPException(status_code=400, detail="No live price available right now.")
    trade = await close_position(fill_px(q, pos["side"], "sell"), "cash out (manual)", raise_on_fail=True)
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


@app.post("/kill")
async def kill(x_admin_token: str = Header(default="")):
    """Emergency stop: pause entries, live OFF, and try to flatten any open position."""
    require_auth(x_admin_token)
    set_cfg("paused", True)
    set_cfg("live_trading", False)
    log_event("kill")
    add_log("KILL SWITCH: paused, live OFF.")
    pos, q = st["position"], st["last_quote"] or {}
    if pos and q.get(pos["side"].lower()) is not None:
        await close_position(fill_px(q, pos["side"], "sell"), "kill switch")
    save_state()
    return {"status": "success", "position_open": st["position"] is not None}


@app.get("/account")
async def account(x_admin_token: str = Header(default="")):
    require_auth(x_admin_token)
    st["acct_evt"].set()
    return balance_view()


@app.post("/account/link")
async def account_link(req: LinkAccount, x_admin_token: str = Header(default="")):
    """Link keys at runtime. They are kept in memory only (lost on restart). For a permanent link,
    set POLYMARKET_KEY_ID / POLYMARKET_SECRET_KEY as Railway variables."""
    require_auth(x_admin_token)
    if not POLY_SDK:
        raise HTTPException(status_code=400, detail="polymarket-us SDK is not installed on this server.")
    old_creds, old_client = st["creds"], st["client"]
    st["creds"], st["client"] = (req.key_id.strip(), req.secret_key.strip()), None
    try:
        data = await asyncio.to_thread(fetch_account)
    except Exception as e:  # noqa: BLE001
        st["creds"], st["client"] = old_creds, old_client
        raise HTTPException(status_code=400, detail=f"Could not verify those keys: {str(e)[:160]}")
    st["account"] = {**data, "ts": time.time(), "status": "ok", "error": ""}
    log_event("account_linked")
    return balance_view()


@app.post("/account/unlink")
async def account_unlink(x_admin_token: str = Header(default="")):
    require_auth(x_admin_token)
    st["creds"], st["client"], st["account"] = None, None, {"status": "not linked"}
    if not creds():
        set_cfg("live_trading", False)
    return balance_view()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
