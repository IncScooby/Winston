"""
Winston v7 -- 5-Minute BTC Up/Down desk. 24/7 cloud engine (FastAPI + asyncio).

WHAT CHANGED vs v6
  Data alignment
  * Polymarket's 5-minute crypto markets settle on a 60-SECOND CHAINLINK TWAP (price-to-beat AND final value,
    since Aug 14 2026). v6 compared Coinbase spot to a Coinbase boundary tick. v7 models the real statistic:
    P(UP) = P( TWAP60(end) >= price_to_beat ), with the exact closed-form mean/variance of a 60s average of a
    Brownian path, a live volatility estimate (blended with EST_VOL_PCT), and the part of the closing minute that
    has already happened.
  * Target price: Terry's price_to_beat first, then Polymarket's RTDS Chainlink TWAP feed (optional, auto-degrades),
    else a clearly-flagged UNVERIFIED Coinbase TWAP. Unverified targets don't trade unless you allow it.
  * Order book: real bid/ask for BOTH contracts. Order of preference: Polymarket US BBO (the venue the SDK trades
    on), then the international CLOB stream/REST book. Non-book prices (Terry, Gamma, model) are display-only and
    can never trigger an entry. If both venues are visible and disagree by more than max_venue_gap, no entry.
  * "Odds" shown/charted are Polymarket's headline probability (book midpoint, last trade if spread > 10c); buy
    prices are the ASK, exits are judged on the BID.

  Entry gates (all must hold continuously for confirm_sec; /state.gates reports every one live)
  * z-score of the projected final TWAP vs the target, price-gap floor, direction persistence, slope and pullback
    from peak (structural confirmation), odds alignment, spread, falling-knife, price band,
    EDGE = P(win) - ask - fee, and a PAYOFF check (win-prob proxy must beat the TP/SL break-even).
  * One-and-done by default: 1 trade per round, no flipping sides in a round, loss locks the side, stricter z for
    any re-entry, cooldowns and the losing-streak breaker. Chat messages are rate-limited so they don't spam.
  * Exits: clean take-profit on the BID (trailing is opt-in via trail_pct > 0), gap-invalidation, stop, time exit.

  Everything else (feeds, live safety, /kill, account, admin token) is unchanged. Live trading still ALWAYS boots
  OFF. Nothing here is a profit guarantee.

Environment variables
  ADMIN_TOKEN (required for live) | TERRY_API_KEY | POLYMARKET_KEY_ID | POLYMARKET_SECRET_KEY
  BOT_NAME | DATA_DIR | TZ_NAME | EST_VOL_PCT | TERRY_POLL_SEC | ACCOUNT_POLL_SEC
  LIVE_SLIPPAGE | LIVE_TIF | LIVE_SLUG_TEMPLATE | LIVE_FILL_WAIT_SEC | PM_WS_URL | RTDS_WS_URL
  LIVE_REQUIRE_US_BOOK (default 1: live orders need the Polymarket US book)
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
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests
import websockets
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

try:  # live SDK is optional until you flip the switch
    from polymarket_us import PolymarketUS
    POLY_SDK = True
except Exception:  # noqa: BLE001
    PolymarketUS = None
    POLY_SDK = False

# ================================================================= SETTINGS ==
WINDOW = 300
TWAP_WIN = 60  # Polymarket 5-minute crypto markets settle on a 60s Chainlink TWAP (since Aug 14 2026)
BOT_NAME = os.environ.get("BOT_NAME", "Winston").strip() or "Winston"
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", str(BASE_DIR)))
STATE_FILE = DATA_DIR / "desk_state.json"
EVENT_FILE = DATA_DIR / "events.jsonl"
TZ = ZoneInfo(os.environ.get("TZ_NAME", "America/Los_Angeles"))

EST_VOL_PCT = float(os.environ.get("EST_VOL_PCT", "0.12"))  # prior for the 5-minute std of BTC, in percent
TERRY_POLL_SEC = max(3.0, float(os.environ.get("TERRY_POLL_SEC", "5")))
ACCOUNT_POLL_SEC = max(5.0, float(os.environ.get("ACCOUNT_POLL_SEC", "15")))
LIVE_SLIPPAGE = float(os.environ.get("LIVE_SLIPPAGE", "0.02"))
LIVE_TIF = os.environ.get("LIVE_TIF", "TIME_IN_FORCE_GOOD_TILL_CANCEL")
LIVE_SLUG_TEMPLATE = os.environ.get("LIVE_SLUG_TEMPLATE", "btc-updown-5m-{ts}")
LIVE_FILL_WAIT = float(os.environ.get("LIVE_FILL_WAIT_SEC", "2.5"))
LIVE_REQUIRE_US_BOOK = os.environ.get("LIVE_REQUIRE_US_BOOK", "1").strip().lower() not in ("0", "false", "no")
PM_WS_URL = os.environ.get("PM_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
RTDS_WS_URL = os.environ.get("RTDS_WS_URL", "wss://ws-live-data.polymarket.com")
LIVE_INTENTS = {
    ("UP", "open"): os.environ.get("LIVE_UP_OPEN_INTENT", "ORDER_INTENT_BUY_LONG"),
    ("UP", "close"): os.environ.get("LIVE_UP_CLOSE_INTENT", "ORDER_INTENT_SELL_LONG"),
    ("DOWN", "open"): os.environ.get("LIVE_DOWN_OPEN_INTENT", "ORDER_INTENT_BUY_SHORT"),
    ("DOWN", "close"): os.environ.get("LIVE_DOWN_CLOSE_INTENT", "ORDER_INTENT_SELL_SHORT"),
}
# Max age (s) before the next price source down the list is used.
FRESH_PM_LINK, FRESH_CLOB, FRESH_TERRY, FRESH_GAMMA, FRESH_US = 15.0, 6.0, 8.0, 15.0, 4.0
NOBOOK_HAIRCUT = 0.01  # when only a midpoint is known, assume we pay/receive 1c worse
REENTRY_Z_EXTRA = 0.3  # a second trade in the same round needs a stronger z than the first

ADMIN_TOKEN_ENV = os.environ.get("ADMIN_TOKEN", "").strip()
ADMIN_TOKEN = ADMIN_TOKEN_ENV or secrets.token_urlsafe(18)

DEFAULT_CONFIG: Dict[str, Any] = {
    # entry timing / price band
    "min_confidence": 62.0, "min_btc_move": 0.02, "entry_min_sec": 10, "entry_max_sec": 60,
    "max_entry_price": 0.72, "min_entry_price": 0.45,
    # confirmation + market checks
    "confirm_sec": 2.0, "min_edge": 0.03, "align_min_odds": 0.52, "max_spread": 0.03,
    # structural / mathematical gates
    "min_z": 0.6, "struct_sec": 8, "persist_min": 0.8, "min_slope": 0.0, "pullback_max": 0.35,
    "max_quote_age": 5.0, "payoff_margin": 0.02, "max_venue_gap": 0.06, "allow_unverified_target": False,
    # anti-thrash
    "max_trades_per_window": 1, "reentry_cooldown_sec": 10, "after_loss_cooldown_sec": 30, "max_window_losses": 1,
    "knife_pts": 0.04, "knife_window_sec": 6,
    "loss_streak_pause": 3, "streak_pause_windows": 2,
    # exits
    "take_profit": 0.06, "stop_loss": 0.08, "force_exit_sec": 120,
    "inval_buffer_pct": 0.015, "inval_hold_sec": 4.0, "min_hold_sec": 3.0,
    "trail_pct": 0.0, "hold_to_settle_price": 0.95,
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
    "min_hold_sec": (0.0, 60.0), "trail_pct": (0.0, 1.0), "hold_to_settle_price": (0.5, 1.0),
    "max_stake_pct_balance": (0.0, 100.0),
    "after_loss_cooldown_sec": (0.0, 600.0), "max_window_losses": (1, 20), "knife_pts": (0.0, 0.5),
    "knife_window_sec": (1.0, 60.0),
    "confirm_sec": (0.0, 10.0), "min_edge": (-0.5, 0.5), "align_min_odds": (0.0, 0.95), "max_spread": (0.0, 0.5),
    "loss_streak_pause": (1, 10), "streak_pause_windows": (0, 12),
    "min_z": (0.0, 4.0), "struct_sec": (3, 60), "persist_min": (0.0, 1.0), "min_slope": (-50.0, 50.0),
    "pullback_max": (0.0, 1.0), "max_quote_age": (0.5, 30.0), "payoff_margin": (-1.0, 0.5),
    "max_venue_gap": (0.0, 0.5),
}
INT_KEYS = {"entry_min_sec", "entry_max_sec", "force_exit_sec", "max_trades_per_window", "max_window_losses",
            "loss_streak_pause", "streak_pause_windows", "struct_sec"}
BOOL_KEYS = ("live_trading", "paused", "winston_veto", "allow_unverified_target")

cfg: Dict[str, Any] = dict(DEFAULT_CONFIG)
cfg_meta = {"version": 0, "updated": time.time()}
SCHEMA = 4  # bump when defaults change meaning; older saved strategy settings are then reset
KEEP_ON_RESET = {"stake_usd", "fee_pct", "max_daily_loss", "max_live_stake", "max_stake_pct_balance",
                 "signal_source", "winston_veto", "live_trading", "paused"}
RESET_KEYS = set(DEFAULT_CONFIG) - KEEP_ON_RESET

st: Dict[str, Any] = {
    "btc": None, "btc_ts": 0.0, "feed_source": "starting", "latency_ms": None,
    "ticks": deque(maxlen=600), "history": [], "odds_hist": [], "twap_hist": [], "model_hist": [],
    "window_start": None, "window_open": None, "open_source": "", "partial": False, "target_rank": 0,
    "win_trades": {}, "win_losses": {}, "side_block": {}, "win_side": {}, "last_exit_loss": False, "basis": 0.0,
    "mid_hist": deque(maxlen=400), "last_exit_ts": 0.0, "entry_block_until": 0.0,
    "loss_streak": 0, "pause_until": 0.0, "confirm": None, "vol": (0, None),
    "terry": None, "terry_status": "starting", "terry_key": None,
    "gamma": {}, "gamma_status": "starting", "clob": None, "clob_status": "starting",
    "books": {}, "pm_last_msg": 0.0, "pm_sub": set(), "pm_status": "starting",
    "us": None, "us_status": "starting", "us_keys": [], "pub_client": None,
    "rtds_spot": deque(maxlen=600), "rtds_twap": deque(maxlen=600), "rtds_status": "starting", "xcheck_ws": None,
    "last_quote": None, "last_move": 0.0, "signal": None, "analytics": None, "gate_report": None,
    "position": None, "trades": [], "chat": [], "logs": [], "now_thinking": "Starting up…", "last_say": {},
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
        st["now_thinking"] = text  # the live "thinking" line always updates
    if st["last_kind"].get(who) == kind:
        return
    now = time.time()
    # chat history is rate-limited for low-importance chatter so flickering gates can't spam it
    if tone in ("wait", "info") and who == "winston" and now - st["last_say"].get(who, 0.0) < 2.5:
        return
    st["last_kind"][who] = kind
    st["last_say"][who] = now
    st["chat"].append({"ts": now, "who": who, "text": text, "tone": tone})
    del st["chat"][:-80]


def think(kind: str, text: str, tone: str = "wait") -> None:
    say("winston", kind, text, tone)


def pc(p: float) -> str:
    return f"{p * 100:.0f}¢"


def window_of(t: float) -> int:
    return int(t // WINDOW) * WINDOW


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


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


# ---- per-second bars: [ts, open, high, low, close] (raw Coinbase prices, basis applied on read) ----------------
def bar_update(bars: List[list], ts: int, p: float) -> bool:
    """Fold a tick into the per-second bar list. Returns True when a new second started."""
    if not bars or bars[-1][0] != ts:
        bars.append([ts, p, p, p, p])
        del bars[:-1800]
        return True
    b = bars[-1]
    b[2], b[3], b[4] = max(b[2], p), min(b[3], p), p
    return False


def series(bars: List[list], t0: int, t1: int) -> Optional[List[float]]:
    """Per-second closes for seconds t0+1 .. t1 (carry-forward fill). None when coverage is too thin to trust."""
    n = t1 - t0
    if n <= 0:
        return None
    got: Dict[int, float] = {}
    prev: Optional[float] = None
    for b in reversed(bars):
        if b[0] > t1:
            continue
        if b[0] <= t0:
            prev = b[4]
            break
        got[b[0]] = b[4]
    out: List[float] = []
    last, seen = prev, 0
    for s in range(t0 + 1, t1 + 1):
        if s in got:
            last, seen = got[s], seen + 1
        if last is not None:
            out.append(last)
    if len(out) < n * 0.9 or seen < n * 0.6:
        return None
    return out


def twap(bars: List[list], t_end: int, win: int = TWAP_WIN) -> Optional[float]:
    """Trailing time-weighted average of the last `win` seconds ending at t_end (raw prices)."""
    s = series(bars, int(t_end) - win, int(t_end))
    return sum(s) / len(s) if s else None


def hist_price_at(ts: int) -> Optional[float]:
    """Raw Coinbase price at a window boundary, from the per-second history."""
    for b in reversed(st["history"]):
        if b[0] <= ts + 1:
            return b[4] if ts - 3 <= b[0] else None
    return None


def realized_var(bars: List[list], ts: int, lookback: int = 180) -> Optional[float]:
    """EWMA of squared 1-second log returns (per second). None until there is enough clean history."""
    s = series(bars, ts - lookback, ts)
    if not s or len(s) < 60:
        return None
    v, lam = None, 0.97
    for i in range(1, len(s)):
        r = math.log(s[i] / s[i - 1])
        v = r * r if v is None else lam * v + (1 - lam) * r * r
    return v


def blend_var(real: Optional[float], est_vol_pct: float) -> float:
    """Per-second return variance: half live realized vol, half the configured prior, kept inside a sane band."""
    est = (est_vol_pct / 100.0) ** 2 / WINDOW
    if real is None:
        return est
    return min(max(0.5 * real + 0.5 * est, 0.4 * est), 6.0 * est)


def ols_slope(xs: List[float], ys: List[float]) -> Optional[float]:
    n = len(xs)
    if n < 3:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    return None if den <= 0 else sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den


def compute_analytics(bars: List[list], basis: float, ptb: float, ws: int, now: float, spot_raw: float,
                      var_ret: float, struct_sec: int) -> Dict[str, Any]:
    """Everything the gates and the chart need, from one consistent snapshot.

    Settlement statistic: final = TWAP60 over the last 60s of the round, compared with the price to beat.
    Let s = per-second price std in USD (sig_s), tau = seconds left, W = 60.
      * tau >= W : the whole closing minute is still in the future. final ~ N(spot, sig_s^2 * V), with
                   V = [ (b^3-a^3)/3 - a^2 W ] / W^2,  a = tau - W, b = tau   (variance of a time-average of BM)
      * tau <  W : a fraction (W-tau)/W of the closing minute is already printed.
                   mean = ((W-tau)*realized_mean + tau*spot)/W,  var = sig_s^2 * tau^3 / (3 W^2)
    z = (mean - ptb) / std,  P(UP) = Phi(z)."""
    end = ws + WINDOW
    tau = max(0.0, end - now)
    spot = spot_raw + basis
    sig_s = math.sqrt(var_ret) * spot
    win = float(TWAP_WIN)
    a_abs = end - win
    if now >= a_abs:
        ser = series(bars, int(a_abs), int(now))
        mean_real = (sum(ser) / len(ser) + basis) if ser else spot
        w_real = (win - tau) / win
        mean_f = w_real * mean_real + (1.0 - w_real) * spot
        var_f = sig_s ** 2 * tau ** 3 / (3.0 * win ** 2)
    else:
        a, b = tau - win, tau
        mean_f = spot
        var_f = sig_s ** 2 * (((b ** 3 - a ** 3) / 3.0) - a * a * win) / (win ** 2)
    std_f = max(math.sqrt(max(var_f, 0.0)), 0.3)
    z = (mean_f - ptb) / std_f
    p_up = min(0.995, max(0.005, norm_cdf(z)))
    tw = twap(bars, int(now), TWAP_WIN)

    # structure over the last n seconds of THIS round (never uses the previous round's prices)
    n = int(max(3, min(struct_sec, now - ws)))
    cut = max(ws, int(now) - n + 1)
    rec = []
    for b in reversed(bars):
        if b[0] < cut:
            break
        rec.append(b)
    rec.reverse()
    frac_above = frac_below = slope = peak_up = peak_dn = None
    if len(rec) >= max(4, int(n * 0.6)):
        vals = [b[4] + basis for b in rec]
        gaps = [v - ptb for v in vals]
        frac_above = sum(1 for g in gaps if g > 0) / len(gaps)
        frac_below = sum(1 for g in gaps if g < 0) / len(gaps)
        slope = ols_slope([float(b[0]) for b in rec], vals)
        peak_up, peak_dn = max(0.0, max(gaps)), max(0.0, -min(gaps))
    return {
        "spot": spot, "ptb": ptb, "gap_usd": spot - ptb, "gap_pct": (spot - ptb) / ptb * 100.0, "tau": tau,
        "sig_s": sig_s, "sigma5m_pct": math.sqrt(var_ret * WINDOW) * 100.0,
        "twap60": None if tw is None else tw + basis,
        "proj_mean": mean_f, "proj_std": std_f, "proj_gap_pct": (mean_f - ptb) / ptb * 100.0,
        "z": z, "p_up": p_up, "slope": slope, "frac_above": frac_above, "frac_below": frac_below,
        "peak_up": peak_up, "peak_dn": peak_dn, "struct_n": n, "struct_sec": struct_sec,
    }


def current_var(now: float) -> float:
    ts = int(now)
    if st["vol"][0] != ts:
        st["vol"] = (ts, blend_var(realized_var(st["history"], ts), EST_VOL_PCT))
    return st["vol"][1]


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


def fill_px(q: Dict[str, Any], side: str, kind: str) -> float:
    """Realistic fill price: buys pay the ask, sells hit the bid. With only a midpoint, assume 1c of slippage."""
    k = side.lower()
    v = q.get(f"{k}_ask") if kind == "buy" else q.get(f"{k}_bid")
    if v:
        return float(v)
    mid = float(q[k])
    return min(0.99, mid + NOBOOK_HAIRCUT) if kind == "buy" else max(0.01, mid - NOBOOK_HAIRCUT)


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
                add_log("Old saved settings found: entry/exit strategy reset to the v7 defaults "
                        "(stake, fees and loss limits kept).")
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


def book_quote(ub: float, ua: float, db: float, da: float, source: str, age: float,
               last_up: Optional[float] = None) -> Optional[Dict[str, Any]]:
    """Build a quote from a real two-sided book. Rejects crossed/empty books.

    up/down        = book midpoints (what the bot uses as 'the odds')
    up_prob        = Polymarket's headline probability: the midpoint of the Up book, or the last trade when the
                     spread is wider than 10c (that is how the Polymarket page itself decides what to show)."""
    if not (0 < ub <= ua < 1 and 0 < db <= da < 1):
        return None
    um, dm = (ub + ua) / 2, (db + da) / 2
    up_prob = (um + (1 - dm)) / 2
    if (ua - ub) > 0.10 and last_up is not None:
        up_prob = last_up
    return {"up": round(um, 4), "down": round(dm, 4), "up_bid": round(ub, 4), "up_ask": round(ua, 4),
            "down_bid": round(db, 4), "down_ask": round(da, 4), "up_prob": round(up_prob, 4),
            "source": source, "age": round(age, 1), "book": True}


def ws_quote(slug: str, now: float) -> Optional[Dict[str, Any]]:
    """International CLOB order-book stream (best bid/ask for both contracts)."""
    if now - st["pm_last_msg"] > FRESH_PM_LINK:  # includes PONGs, so this is connection health
        return None
    tok = (st["gamma"].get(slug) or {}).get("tokens") or {}
    u, d = st["books"].get(tok.get("up")), st["books"].get(tok.get("down"))
    if not u or not d:
        return None
    ub, ua, db, da = u.get("bid"), u.get("ask"), d.get("bid"), d.get("ask")
    if None in (ub, ua, db, da):
        return None
    last_up = u.get("last")
    if last_up is None and d.get("last") is not None:
        last_up = 1 - d["last"]
    return book_quote(ub, ua, db, da, "polymarket-ws", now - min(u["ts"], d["ts"]), last_up)


def clob_quote(slug: str, now: float) -> Optional[Dict[str, Any]]:
    c = st["clob"]
    if not c or c["slug"] != slug or now - c["fetched"] > FRESH_CLOB:
        return None
    return book_quote(c["up_bid"], c["up_ask"], c["down_bid"], c["down_ask"], "polymarket-rest", now - c["fetched"])


def us_quote(now: float) -> Optional[Dict[str, Any]]:
    """Polymarket US best bid/offer. One book (the 'long'/Up side). The short/Down contract is its mirror:
    buy-short pays 1 - best_bid_long, sell-short receives 1 - best_ask_long."""
    u = st["us"]
    if not u or u["slug"] != live_slug() or now - u["fetched"] > FRESH_US:
        return None
    b, a = u["bid"], u["ask"]
    if b is None or a is None:
        return None
    return book_quote(b, a, 1 - a, 1 - b, "polymarket-us", now - u["fetched"], u.get("last"))


def get_quote(A: Dict[str, Any]) -> Dict[str, Any]:
    now = time.time()
    slug = f"btc-updown-5m-{st['window_start']}"
    us, intl = us_quote(now), (ws_quote(slug, now) or clob_quote(slug, now))
    q = us or intl
    if q:
        if us and intl:
            q["xgap"] = round(abs(us["up"] - intl["up"]), 4)
        return q
    # ---- display-only fallbacks: no bid/ask, so entries are never allowed on these ----
    t = terry_fresh()
    if t and now - t["fetched"] <= FRESH_TERRY and terry_matches(t, A["tau"]):
        pr = t["item"].get("prices") or {}
        up, dn = norm_price(pr.get("up")), norm_price(pr.get("down"))
        if up is not None and dn is not None:
            return {"up": up, "down": dn, "up_prob": up / (up + dn), "source": "terrycalls",
                    "age": round(now - t["fetched"], 1), "book": False}
    g = st["gamma"].get(slug)
    if g and now - g["fetched"] <= FRESH_GAMMA and g["up"] is not None and g["down"] is not None:
        return {"up": g["up"], "down": g["down"], "up_prob": g["up"] / (g["up"] + g["down"]),
                "source": "polymarket", "age": round(now - g["fetched"], 1), "book": False}
    p = round(A["p_up"], 3)
    return {"up": p, "down": round(1 - p, 3), "up_prob": p, "source": "estimated", "age": 0.0, "book": False}


# ================================================================ SIGNALS ==
def signal_snapshot(A: Dict[str, Any], left: float) -> Dict[str, Any]:
    src = cfg["signal_source"]
    if src == "momentum":
        lean = "UP" if A["z"] >= 0 else "DOWN"
        p_side = A["p_up"] if lean == "UP" else 1 - A["p_up"]
        conf = round(p_side * 100, 1)
        strong = abs(A["z"]) >= cfg["min_z"] and abs(A["gap_pct"]) >= cfg["min_btc_move"]
        detail = (f"BTC is {A['gap_pct']:+.3f}% from the target (z {A['z']:+.2f}). "
                  f"My model gives {lean} a {conf:.0f}% chance to settle that way." if strong else
                  f"BTC is {A['gap_pct']:+.3f}% from the target (z {A['z']:+.2f}). I need |z| ≥ {cfg['min_z']:.2f} "
                  f"and a ±{cfg['min_btc_move']:.3f}% gap before I consider a trade.")
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


# ================================================================= TARGET ==
def rtds_ptb(ws: int) -> Optional[float]:
    """Chainlink 60s TWAP tick nearest the round boundary (the market's price to beat)."""
    best = None
    for ts, v in st["rtds_twap"]:
        d = abs(ts - ws)
        if d <= 3.0 and (best is None or d < best[0]):
            best = (d, v)
    return best[1] if best else None


def adopt_target() -> None:
    """The market's price-to-beat is a Chainlink 60s TWAP, NOT a Coinbase tick. Prefer Terry's published value,
    then Polymarket's own RTDS TWAP feed. Each gives us the *basis* (market source minus Coinbase TWAP at the
    boundary), so the whole chart and every gap calculation line up with the market's target.
    rank: 2 = Terry, 1 = RTDS, 0 = Coinbase-only (unverified)."""
    ws = st["window_start"]
    if ws is None or st["target_rank"] >= 2:
        return
    cands = []
    t = st["terry"]
    if t and terry_matches(t, WINDOW - (time.time() - ws)):
        try:
            v = float(t["item"].get("price_to_beat"))
            if v > 0:
                cands.append((2, v, "terrycalls price-to-beat"))
        except (TypeError, ValueError):
            pass
    rv = rtds_ptb(ws)
    if rv is not None:
        cands.append((1, rv, "rtds chainlink twap60"))
    if len(cands) == 2 and st["xcheck_ws"] != ws:  # both known: log how they compare so you can verify the maths
        st["xcheck_ws"] = ws
        log_event("ptb_xcheck", ws=ws, terry=cands[0][1], rtds=cands[1][1], diff=round(cands[0][1] - cands[1][1], 2))
    cb = twap(st["history"], ws, TWAP_WIN)
    if cb is None:
        return
    for rank, ptb, src in sorted(cands, reverse=True):
        if rank <= st["target_rank"]:
            continue
        if abs(ptb - cb) / cb * 100 > 0.15:
            continue  # looks like the wrong market/window; don't adopt
        st["window_open"], st["basis"] = ptb, ptb - cb
        st["open_source"], st["partial"], st["target_rank"] = src, False, rank
        add_log(f"Target {ptb:,.2f} from {src}; basis vs Coinbase TWAP60 {ptb - cb:+.2f}")
        break


# ============================================================ LIVE (SDK) ==
def get_client():
    if st["client"] is None:
        k, s = creds()
        st["client"] = PolymarketUS(key_id=k, secret_key=s)
    return st["client"]


def pub_client():
    if st["pub_client"] is None:
        st["pub_client"] = PolymarketUS()  # public endpoints (market data) need no keys
    return st["pub_client"]


def live_slug() -> str:
    ws = st["window_start"] or window_of(time.time())
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
                        move_pct: float, stake: Optional[float] = None, why: str = "",
                        meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    async with st["lock"]:
        if st["position"] is not None:
            raise RuntimeError("a position is already open")
        mode = "live" if cfg["live_trading"] else "paper"
        if quote["source"] == "estimated":
            raise RuntimeError("no real market price available, so I won't place an order")
        price = fill_px(quote, side, "buy")
        stake = float(stake if stake is not None else cfg["stake_usd"])
        order: Dict[str, Any] = {}
        slug = f"btc-updown-5m-{st['window_start']}"
        if mode == "live":
            ok, reason = live_ready()
            if not ok:
                raise RuntimeError(f"live trading not ready: {reason}")
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
        st["win_side"][ws] = side
        st["confirm"] = None  # a fresh confirmation is needed for any re-entry
        pos = {
            "id": f"t{int(now * 1000)}", "mode": mode, "side": side, "slug": slug, "window_start": ws,
            "opened_at": now, "entry": round(price, 4), "shares": shares, "stake": round(shares * price, 2),
            "price_source": quote["source"], "btc_open": st["window_open"], "btc_at_entry": btc_adj(),
            "move_pct_at_entry": round(move_pct, 4), "signal_source": "manual" if manual else cfg["signal_source"],
            "confidence": confidence, "manual": manual, "order_id": order.get("order_id"),
            "fee_pct": cfg["fee_pct"], "peak": round(price, 4), "inval_since": None,
            "n_in_window": st["win_trades"][ws], "target_src": st["open_source"], **(meta or {}),
        }
        st["position"] = pos
        add_log(f"[{mode.upper()}] BUY {side} @ {price:.2f} x{shares:.2f} (${pos['stake']:.2f}) "
                f"#{pos['n_in_window']} {'MANUAL' if manual else cfg['signal_source']}")
        log_event("entry", **{k: pos[k] for k in ("id", "mode", "side", "entry", "shares", "stake", "price_source",
                                                   "move_pct_at_entry", "confidence", "signal_source",
                                                   "n_in_window", "target_src")},
                  z=pos.get("z"), p_win=pos.get("p_win"), edge_px=pos.get("edge_px"), why=why)
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
        if pnl < 0 and not settle:  # a loss: that side is off-limits for the rest of this window
            w = pos["window_start"]
            st["win_losses"][w] = st["win_losses"].get(w, 0) + 1
            st["side_block"].setdefault(w, set()).add(pos["side"])
        note_streak(pos, pnl)
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
        else:  # fall back to the real settlement statistic: the 60s TWAP at the boundary vs the price to beat
            fin = twap(st["history"], ws, TWAP_WIN)
            final = (fin if fin is not None else (price_at(ws) or btc)) + st["basis"]
            won = (pos["side"] == "UP") == (final >= (st["window_open"] or final))
        await close_position(1.0 if won else 0.0, f"window expired ({'won' if won else 'lost'})", settle=True)
    st["window_start"] = ws
    for d in (st["win_trades"], st["win_losses"], st["side_block"], st["win_side"]):
        for k in [k for k in d if k < ws - 3 * WINDOW]:
            del d[k]
    st["basis"], st["target_rank"], st["confirm"] = 0.0, 0, None
    cb = twap(st["history"], ws, TWAP_WIN)
    if cb is None:  # not enough clean history to build the 60s average: we can't trust this round's maths
        open_price, partial = (price_at(ws) or btc), True
    else:
        open_price, partial = cb, False
    st["window_open"], st["open_source"], st["partial"] = open_price, "coinbase twap60 (unverified)", partial
    add_log(f"New window {ws}: provisional target {open_price:,.2f} (coinbase twap60)"
            + (" - not enough history, sitting out" if partial else ""))
    say("sys", f"w{ws}", f"New 5-minute window · target ${open_price:,.2f}", "sys")
    adopt_target()


async def manage_position(q: Dict[str, Any], elapsed: float, A: Dict[str, Any]) -> None:
    """Exits are driven by the projected-gap first (clean take-profit on the BID), contract price second."""
    pos, now = st["position"], time.time()
    side = pos["side"]
    sgn = 1.0 if side == "UP" else -1.0
    mid, bid = q[side.lower()], fill_px(q, side, "sell")
    held = now - pos["opened_at"]
    same = price_class(q["source"]) == price_class(pos["price_source"])
    pos["peak"] = max(pos.get("peak", pos["entry"]), mid)
    edge = sgn * A["proj_gap_pct"]  # % the projected final TWAP is on our side of the target
    if edge < -cfg["inval_buffer_pct"]:
        pos["inval_since"] = pos.get("inval_since") or now
    else:
        pos["inval_since"] = None
    trail = cfg["trail_pct"]
    riding = mid >= cfg["hold_to_settle_price"] and edge > 0  # near-certain winner: let it settle
    armed = pos["peak"] >= pos["entry"] + cfg["take_profit"]
    reason = None
    if same and trail <= 0 and bid >= pos["entry"] + cfg["take_profit"]:
        reason = "take profit"
    elif pos["inval_since"] and now - pos["inval_since"] >= cfg["inval_hold_sec"]:
        reason = "gap invalidated"
    elif same and held >= cfg["min_hold_sec"] and pos["entry"] - mid >= cfg["stop_loss"]:
        reason = "emergency stop"
    elif same and trail > 0 and armed and not riding and (pos["peak"] - mid >= trail or edge <= 0):
        reason = "profit locked" if bid > pos["entry"] else "gave back profit"
    elif elapsed >= cfg["force_exit_sec"] and not riding:
        reason = "time exit"
    if reason:
        if now - st["close_fail_ts"] >= 3:
            await close_position(bid, reason)
        return
    state = "riding to settlement" if riding else ("trailing a winner" if (armed and trail > 0) else "holding")
    tp = pos["entry"] + cfg["take_profit"]
    think(f"hold{pos['id']}:{state}", f"{state.capitalize()} {side} from {pc(pos['entry'])}, bid {pc(bid)}"
                                     + (f", selling at {pc(tp)}" if trail <= 0 else "") + f". The projected final is "
                                     f"{edge:+.3f}% on my side of the target. I exit if that flips for "
                                     f"{cfg['inval_hold_sec']:.0f}s, not on price noise.", "info")


# ========================================================== ENTRY GATES ==
def entry_gates(now: float, ws: int, elapsed: float, q: Dict[str, Any], sig: Dict[str, Any],
                A: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluate every entry condition for the side being considered. Pure read of state: used by the engine to
    decide AND by /state so the dashboard checklist is exactly what the bot is thinking."""
    terry = cfg["signal_source"] == "terrycalls"
    called = sig.get("side") is not None
    side = sig.get("side") or sig.get("lean") or ("UP" if A["z"] >= 0 else "DOWN")
    sgn = 1.0 if side == "UP" else -1.0
    k = side.lower()
    n = st["win_trades"].get(ws, 0)
    enforce = cfg["winston_veto"] or not terry  # with Terry + veto OFF, the structure gates become advisory
    conf = sig.get("confidence")
    p_side = A["p_up"] if side == "UP" else 1 - A["p_up"]
    p_win = min(conf / 100.0, p_side) if conf is not None else p_side
    book = bool(q.get("book"))
    ask = fill_px(q, side, "buy") if book else None
    mid = q.get(k)
    bid_, ask_ = q.get(f"{k}_bid"), q.get(f"{k}_ask")
    view = (f"{'Terry says' if terry else 'My read says'} {side} at {conf:.0f}%"
            if called and conf is not None else sig["detail"])
    items: List[Dict[str, Any]] = []

    def gate(gid: str, label: str, ok: bool, msg: str, adv: bool = False) -> None:
        items.append({"id": gid, "label": label, "ok": bool(ok), "adv": adv, "msg": msg})

    gate("run", "Bot running", not cfg["paused"],
         "I'm paused, so I'm only watching. Hit Resume when you want me trading.")
    gate("target", f"Target verified ({A['target_src']})", A["target_ok"] or cfg["allow_unverified_target"],
         "I can't verify this round's price-to-beat from Terry or Polymarket's feed, so my gap maths could be off by "
         "tens of dollars. I'm sitting this round out.")
    gate("ontime", "History OK for this round", not st["partial"],
         "I don't have a clean minute of price history for this round, so I don't trust the target. Sitting it out.")
    streak_ok = now >= st["pause_until"]
    gate("streak", f"No losing-streak pause ({int(cfg['loss_streak_pause'])} in a row)", streak_ok,
         f"{int(cfg['loss_streak_pause'])} losses in a row, so I'm standing down for a few rounds to stop a bad "
         f"patch compounding.")
    losses = st["win_losses"].get(ws, 0)
    gate("cap", f"Trades this round {n}/{int(cfg['max_trades_per_window'])}",
         n < cfg["max_trades_per_window"] and losses < cfg["max_window_losses"],
         f"That's my {n} trade(s) and {losses} loss(es) for this round. I'm done until the next one.")
    cd = cfg["after_loss_cooldown_sec"] if st["last_exit_loss"] else cfg["reentry_cooldown_sec"]
    since_exit = now - st["last_exit_ts"]
    gate("cool", "Cooldown after last exit", not n or since_exit >= cd,
         "Cooling down after my last exit before I look for another entry.")
    prev_side = st["win_side"].get(ws)
    gate("lock", "No flipping / no re-buying a loser",
         (prev_side is None or prev_side == side) and side not in st["side_block"].get(ws, ()),
         f"I already traded {prev_side or side} this round and I don't flip or re-buy a loser. Waiting for the next round.")
    gate("time", f"Entry window {int(cfg['entry_min_sec'])}–{int(cfg['entry_max_sec'])}s (now {int(elapsed)}s)",
         cfg["entry_min_sec"] <= elapsed <= cfg["entry_max_sec"],
         f"{view}. Only {int(elapsed)}s in, I start at {int(cfg['entry_min_sec'])}s." if elapsed < cfg["entry_min_sec"]
         else "Too late in this round to enter on early momentum. On to the next one.")
    need_us = cfg["live_trading"] and LIVE_REQUIRE_US_BOOK
    age = q.get("age") or 0.0
    q_ok = book and age <= cfg["max_quote_age"] and (not need_us or q["source"] == "polymarket-us")
    gate("quote", f"Real order book ({q['source']}{', ' + format(age, '.1f') + 's' if book else ', none right now'})",
         q_ok, "I don't have a fresh two-sided order book" + (" from Polymarket US" if need_us else "")
         + ", so I can't price a fill. I only trade against real bid/ask, never a model price.")
    xg = q.get("xgap")
    if xg is not None:
        gate("xvenue", f"Venues agree (gap {xg * 100:.1f}¢ ≤ {cfg['max_venue_gap'] * 100:.0f}¢)",
             xg <= cfg["max_venue_gap"],
             "Polymarket US and the international book disagree on the odds, so one of them is stale. Passing.")
    gate("signal", "Signal has a call", called, f"{sig['detail']} I'll keep watching.")
    gate("conf", f"Confidence {'–' if conf is None else format(conf, '.0f')}% ≥ {cfg['min_confidence']:.0f}%",
         conf is not None and conf >= cfg["min_confidence"],
         f"{view}, but I need at least {cfg['min_confidence']:.0f}%. Passing for now.")
    need_z = cfg["min_z"] + (REENTRY_Z_EXTRA if n else 0.0)
    zs, gap_side = sgn * A["z"], sgn * A["gap_pct"]
    gate("z", f"z-score {zs:+.2f} ≥ {need_z:.2f} · gap {gap_side:+.3f}% ≥ {cfg['min_btc_move']:.3f}%",
         zs >= need_z and gap_side >= cfg["min_btc_move"],
         f"{view}, but the projected settlement is only z {zs:+.2f} from the target (I need {need_z:.2f}) with BTC "
         f"{gap_side:+.3f}% on that side. That's inside the noise. Passing.", adv=not enforce)
    frac = A["frac_above"] if side == "UP" else A["frac_below"]
    gate("persist", "Held that side " + ("–" if frac is None else format(frac * 100, ".0f"))
         + f"% of last {A['struct_n']}s ≥ {cfg['persist_min'] * 100:.0f}%",
         frac is not None and frac >= cfg["persist_min"],
         f"{view}, but BTC hasn't stayed on the {side} side of the target consistently, so it's a flicker not a trend.",
         adv=not enforce)
    slope = None if A["slope"] is None else sgn * A["slope"]
    gate("slope", "Slope " + ("–" if slope is None else f"{slope:+.2f}") + f" $/s ≥ {cfg['min_slope']:+.2f}",
         slope is not None and slope >= cfg["min_slope"],
         f"{view}, but the last {A['struct_n']}s are drifting against it. The move is fading.", adv=not enforce)
    peak = A["peak_up"] if side == "UP" else A["peak_dn"]
    pb = None if peak is None else ((peak - sgn * A["gap_usd"]) / peak if peak > 0 else 1.0)
    gate("pullback", "Pullback from peak " + ("–" if pb is None else format(max(pb, 0) * 100, ".0f"))
         + f"% ≤ {cfg['pullback_max'] * 100:.0f}%", pb is not None and pb <= cfg["pullback_max"],
         f"{view}, but BTC has already given back too much of its early move. I don't chase fades.", adv=not enforce)
    gate("price", f"Ask {'–' if ask is None else pc(ask)} within {pc(cfg['min_entry_price'])}–{pc(cfg['max_entry_price'])}",
         ask is not None and cfg["min_entry_price"] <= ask <= cfg["max_entry_price"],
         (f"{view}. But {side} already costs {pc(ask)}, over my {pc(cfg['max_entry_price'])} limit, so the move is "
          f"priced in. Passing." if ask is not None and ask > cfg["max_entry_price"] else
          f"{view}. But the crowd only gives {side} {'–' if ask is None else pc(ask)}, so they strongly disagree. "
          f"Passing."))
    gate("align", f"Market prices {side} {'–' if mid is None else pc(mid)} ≥ {pc(cfg['align_min_odds'])}",
         mid is not None and mid >= cfg["align_min_odds"],
         f"{view}. But the market only prices {side} at {'–' if mid is None else pc(mid)}, under my "
         f"{pc(cfg['align_min_odds'])} agreement floor. Passing.")
    spr = None if (bid_ is None or ask_ is None) else ask_ - bid_
    gate("spread", f"Spread {'–' if spr is None else format(spr * 100, '.0f')}¢ ≤ {cfg['max_spread'] * 100:.0f}¢",
         spr is not None and spr <= cfg["max_spread"],
         f"{view}. But the buy/sell gap on {side} is {'–' if spr is None else format(spr * 100, '.0f')}¢, wider than "
         f"my {cfg['max_spread'] * 100:.0f}¢ limit, so the fill would cost too much.")
    drop = odds_drop(side, cfg["knife_window_sec"])
    gate("knife", f"Odds not falling ({drop * 100:+.0f}pts in {cfg['knife_window_sec']:.0f}s < {cfg['knife_pts'] * 100:.0f})",
         drop < cfg["knife_pts"],
         f"{view}, but {side} just fell {drop * 100:.0f} points in {cfg['knife_window_sec']:.0f}s. The market is moving "
         f"against it, so I won't catch a falling knife.")
    fee_ps = (ask or 0.0) * cfg["fee_pct"] / 100.0
    edge_px = None if ask is None else p_win - ask - fee_ps
    gate("edge", "Edge " + ("–" if edge_px is None else f"{edge_px * 100:+.1f}") + f"¢ ≥ {cfg['min_edge'] * 100:.1f}¢ (P(win) {p_win * 100:.0f}% − ask − fee)",
         edge_px is not None and edge_px >= cfg["min_edge"],
         f"{view}. But {side} costs {'–' if ask is None else pc(ask)} against a {p_win * 100:.0f}% win chance, so I only "
         f"see {'–' if edge_px is None else format(edge_px * 100, '.1f')}¢ of edge after fees and I need "
         f"{cfg['min_edge'] * 100:.1f}¢. The odds haven't lagged enough.")
    sl, tp = cfg["stop_loss"], cfg["take_profit"]
    be = (sl + fee_ps) / (tp + sl) if (tp + sl) > 0 else 1.0
    gate("payoff", f"P(win) {p_win * 100:.0f}% ≥ break-even {be * 100:.0f}% + {cfg['payoff_margin'] * 100:.0f}%",
         p_win >= be + cfg["payoff_margin"],
         f"{view}. With a {tp * 100:.0f}¢ target against a {sl * 100:.0f}¢ stop I need to win about "
         f"{(be + cfg['payoff_margin']) * 100:.0f}% of the time, and my estimate is {p_win * 100:.0f}%. "
         f"The payoff doesn't justify the risk.")
    blocking = next((g["id"] for g in items if not g["ok"] and not g["adv"]), None)
    return {"side": side, "called": called, "items": items, "blocking": blocking, "ready": blocking is None,
            "p_win": round(p_win, 4), "p_model": round(p_side, 4), "ask": ask, "edge_px": edge_px,
            "breakeven": round(be, 4), "confirm_have": 0.0, "confirm_need": cfg["confirm_sec"], "view": view}


async def consider_entry(q: Dict[str, Any], elapsed: float, A: Dict[str, Any], rep: Dict[str, Any]) -> None:
    ws, now = st["window_start"], time.time()
    side = rep["side"]
    prev_conf, st["confirm"] = st["confirm"], None  # any early return below restarts the confirmation timer
    if now < st["entry_block_until"]:
        return
    if not rep["ready"]:
        g = next(x for x in rep["items"] if x["id"] == rep["blocking"])
        return think(f"g:{g['id']}:{side}", g["msg"], "info" if g["id"] in ("z", "persist", "slope", "pullback") else "wait")
    if cfg["confirm_sec"] > 0:
        same = bool(prev_conf and prev_conf["ws"] == ws and prev_conf["side"] == side)
        since = prev_conf["since"] if same else now
        st["confirm"] = {"ws": ws, "side": side, "since": since}
        rep["confirm_have"] = round(now - since, 2)
        if now - since < cfg["confirm_sec"]:
            return think(f"confirm{ws}:{side}", f"{rep['view']}. Every check passes, so I'm confirming it for "
                                                f"{cfg['confirm_sec']:.1f}s before I buy.")
    n = st["win_trades"].get(ws, 0)
    why = (f"{rep['view']} Projected settlement z {sgn_z(A, side):+.2f}, BTC {A['gap_pct']:+.3f}% vs the target, "
           f"{side} costs {pc(rep['ask'])}, win chance {rep['p_win'] * 100:.0f}%, edge {rep['edge_px'] * 100:.1f}¢"
           + (f" (re-entry #{n + 1})" if n else "") + ".")
    meta = {"z": round(sgn_z(A, side), 3), "p_win": rep["p_win"], "edge_px": round(rep["edge_px"], 4),
            "breakeven": rep["breakeven"], "sigma5m_pct": round(A["sigma5m_pct"], 4)}
    try:
        await open_position(side, q, manual=False, confidence=rep["p_win"] * 100, move_pct=A["gap_pct"],
                            why=why, meta=meta)
    except Exception as e:  # noqa: BLE001
        st["entry_block_until"] = time.time() + 10  # don't hammer the exchange every tick
        add_log(f"Entry failed: {e}")
        log_event("error", where="entry", error=str(e))
        think(f"entryfail{ws}:{int(now)}", f"⚠️ I wanted to buy {side} but it failed: {e}", "loss")


def sgn_z(A: Dict[str, Any], side: str) -> float:
    return A["z"] if side == "UP" else -A["z"]


async def tick() -> None:
    now, btc = time.time(), st["btc"]
    if btc is None or now - st["btc_ts"] > 20:
        think("nofeed", "The BTC price feed is down or stale. I won't trade blind.")
        return
    ts = int(now)
    new_second = bar_update(st["history"], ts, btc)
    ws = window_of(now)
    elapsed = now - ws
    if st["window_start"] != ws:
        await on_new_window(ws, elapsed, btc)
    adopt_target()
    open_price = st["window_open"]
    A = compute_analytics(st["history"], st["basis"], open_price, ws, now, btc, current_var(now), int(cfg["struct_sec"]))
    A["target_rank"], A["target_src"], A["target_ok"] = st["target_rank"], st["open_source"], st["target_rank"] >= 1
    st["analytics"], st["last_move"] = A, A["gap_pct"]
    q = get_quote(A)
    st["last_quote"] = q
    if q["source"] != "estimated":
        st["mid_hist"].append((now, q["up"], q["down"]))
    if new_second:
        tw = twap(st["history"], ts, TWAP_WIN)
        if tw is not None:
            st["twap_hist"].append([ts, round(tw, 2)])
            del st["twap_hist"][:-900]
        st["odds_hist"].append([ts, round(q["up_prob"] * 100.0, 2),
                                None if q.get("up_bid") is None else round(q["up_bid"] * 100.0, 2),
                                None if q.get("up_ask") is None else round(q["up_ask"] * 100.0, 2)])
        del st["odds_hist"][:-900]
        st["model_hist"].append([ts, round(A["p_up"] * 100.0, 2)])
        del st["model_hist"][:-900]
    st["signal"] = signal_snapshot(A, A["tau"])
    if st["pause_until"] and now >= st["pause_until"]:
        st["pause_until"], st["loss_streak"] = 0.0, 0
        add_log("Losing-streak pause is over. Trading again.")
    if st["position"] is not None:
        st["gate_report"] = None
        await manage_position(q, elapsed, A)
    else:
        rep = entry_gates(now, ws, elapsed, q, st["signal"], A)
        st["gate_report"] = rep
        await consider_entry(q, elapsed, A, rep)


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


def pm_update(asset: str, bid: Optional[float] = None, ask: Optional[float] = None,
              last: Optional[float] = None) -> None:
    b = st["books"].setdefault(asset, {})
    if bid is not None:
        b["bid"] = bid
    if ask is not None:
        b["ask"] = ask
    if last is not None:
        b["last"] = last
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
    elif et == "last_trade_price" and m.get("asset_id"):
        pm_update(m["asset_id"], last=_f(m.get("price")))
    else:
        return
    st["evt"].set()


def pm_tokens() -> set:
    out = set()
    for g in st["gamma"].values():
        out.update((g.get("tokens") or {}).values())
    return out


async def pm_ws_task() -> None:
    """International order-book stream. Re-subscribes as new windows' markets appear."""
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
                            msg = ({"assets_ids": sorted(new), "type": "market", "custom_feature_enabled": True}
                                   if first else {"assets_ids": sorted(new), "operation": "subscribe"})
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


def rtds_handle(m: Dict[str, Any]) -> None:
    topic, pl = m.get("topic"), m.get("payload")
    if not isinstance(pl, dict) or topic not in ("crypto_prices_chainlink", "crypto_prices_twap_sixty"):
        return
    sym = str(pl.get("symbol", "")).lower()
    if sym and sym != "btc/usd":
        return
    dest = st["rtds_twap"] if topic == "crypto_prices_twap_sixty" else st["rtds_spot"]
    rows = pl["data"] if isinstance(pl.get("data"), list) else [pl]  # snapshots carry a history list
    for r in rows:
        v, t = _f(r.get("value", r.get("price"))), _f(r.get("timestamp"))
        if v and t:
            dest.append((t / 1000.0 if t > 1e11 else t, v))
    if topic == "crypto_prices_twap_sixty":
        adopt_target()


async def rtds_task() -> None:
    """Optional: Polymarket's own Chainlink TWAP feed (the price-to-beat source). If it isn't reachable, the bot
    just relies on Terry's price_to_beat; status shows in /health."""
    delay = 1.0
    while True:
        helper = None
        try:
            async with websockets.connect(RTDS_WS_URL, ping_interval=None, max_size=2 ** 20) as ws:
                flt = json.dumps({"symbol": "btc/usd"})
                await ws.send(json.dumps({"action": "subscribe", "subscriptions": [
                    {"topic": "crypto_prices_chainlink", "type": "*", "filters": flt},
                    {"topic": "crypto_prices_twap_sixty", "type": "*", "filters": flt}]}))
                st["rtds_status"] = "connected"

                async def pinger():
                    while True:
                        await asyncio.sleep(5)
                        await ws.send("PING")

                helper = asyncio.create_task(pinger())
                async for raw in ws:
                    delay = 1.0
                    if not raw or raw in ("PONG", b"PONG"):
                        continue
                    try:
                        data = json.loads(raw)
                    except Exception:  # noqa: BLE001
                        continue
                    for m in (data if isinstance(data, list) else [data]):
                        if isinstance(m, dict):
                            rtds_handle(m)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            st["rtds_status"] = f"reconnecting ({type(e).__name__})"
        finally:
            if helper:
                helper.cancel()
        await asyncio.sleep(delay)
        delay = min(60.0, delay * 2)


def fetch_us_bbo(slug: str) -> Dict[str, Any]:
    raw = to_plain(pub_client().markets.bbo(slug))
    md = raw.get("marketData", raw) if isinstance(raw, dict) else {}
    md = md if isinstance(md, dict) else {}
    return {"slug": slug, "bid": norm_price(_num(md.get("bestBid"))), "ask": norm_price(_num(md.get("bestAsk"))),
            "last": norm_price(_num(md.get("lastTradePx"))), "keys": sorted(md.keys())[:12], "fetched": time.time()}


async def us_task() -> None:
    """Polymarket US best bid/offer for the current round: the venue the SDK actually trades on."""
    while True:
        delay = 0.8
        if not POLY_SDK:
            st["us_status"] = "sdk not installed"
            await asyncio.sleep(15)
            continue
        slug = live_slug()
        try:
            res = await asyncio.to_thread(fetch_us_bbo, slug)
            st["us"], st["us_keys"] = res, res["keys"]
            st["us_status"] = "ok" if (res["bid"] and res["ask"]) else "one-sided book"
            st["evt"].set()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            st["us_status"] = f"error: {str(e)[:80]}"
            delay = 5.0
        await asyncio.sleep(delay)


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
                adopt_target()
                st["evt"].set()
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
    outcomes, prices = market["outcomes"], market["outcomePrices"]
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
    return {"slug": slug, "up": mapped.get("up"), "down": mapped.get("down"), "tokens": tokens, "fetched": time.time()}


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


def fetch_book(token: str) -> tuple:
    """Best bid/ask from the CLOB REST book (levels are not guaranteed sorted, so take max bid / min ask)."""
    r = requests.get("https://clob.polymarket.com/book", params={"token_id": token}, timeout=4)
    r.raise_for_status()
    j = r.json()
    bids = [float(x["price"]) for x in j.get("bids", []) if float(x.get("size", 0)) > 0]
    asks = [float(x["price"]) for x in j.get("asks", []) if float(x.get("size", 0)) > 0]
    return (max(bids) if bids else None, min(asks) if asks else None)


async def clob_task() -> None:
    """REST order-book fallback. Skips itself while the order-book stream is healthy."""
    while True:
        try:
            slug = f"btc-updown-5m-{window_of(time.time())}"
            if ws_quote(slug, time.time()):
                st["clob_status"] = "idle (ws ok)"
            else:
                tok = (st["gamma"].get(slug) or {}).get("tokens") or {}
                if tok.get("up") and tok.get("down"):
                    (ub, ua), (db, da) = await asyncio.gather(asyncio.to_thread(fetch_book, tok["up"]),
                                                              asyncio.to_thread(fetch_book, tok["down"]))
                    if None not in (ub, ua, db, da):
                        st["clob"] = {"slug": slug, "up_bid": ub, "up_ask": ua, "down_bid": db, "down_ask": da,
                                      "fetched": time.time()}
                        st["clob_status"] = "ok"
                        st["evt"].set()
                    else:
                        st["clob_status"] = "one-sided book"
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
             (feed_task, rest_task, pm_ws_task, rtds_task, us_task, terry_task, gamma_task, clob_task,
              account_task, engine)]
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
    # Unknown keys (the page may send extras) are ignored by pydantic, not rejected.
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
    loss_streak_pause: Optional[float] = None
    streak_pause_windows: Optional[float] = None
    min_z: Optional[float] = None
    struct_sec: Optional[float] = None
    persist_min: Optional[float] = None
    min_slope: Optional[float] = None
    pullback_max: Optional[float] = None
    max_quote_age: Optional[float] = None
    payoff_margin: Optional[float] = None
    max_venue_gap: Optional[float] = None
    allow_unverified_target: Optional[bool] = None
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
    view["bid"], view["ask"] = q.get(f"{k}_bid"), q.get(f"{k}_ask")
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
    """Trade markers. `btc` is the (basis-adjusted) BTC price at that moment and `upq` the Up-probability
    equivalent of the fill price, so the chart can place each marker exactly on both panels."""
    def upq(side: str, px: float) -> float:
        return round((px if side == "UP" else 1 - px) * 100, 2)

    out = []
    for t in st["trades"][-60:]:
        if t["opened_at"] >= since:
            out.append({"id": t["id"], "time": int(t["opened_at"]), "kind": "enter", "side": t["side"],
                        "price": t["entry"], "btc": t.get("btc_at_entry"), "upq": upq(t["side"], t["entry"])})
        if t["closed_at"] >= since:
            out.append({"id": t["id"], "time": int(t["closed_at"]), "kind": "exit", "side": t["side"],
                        "price": t["exit"], "pnl": t["pnl"], "btc": t.get("btc_at_exit"),
                        "upq": upq(t["side"], t["exit"]), "reason": t.get("reason")})
    p = st["position"]
    if p and p["opened_at"] >= since:
        out.append({"id": p["id"], "time": int(p["opened_at"]), "kind": "enter", "side": p["side"],
                    "price": p["entry"], "btc": p.get("btc_at_entry"), "upq": upq(p["side"], p["entry"])})
    return out


def balance_view() -> Dict[str, Any]:
    a = st["account"]
    return {"linked": linked(), "status": a.get("status", "not linked"), "cash": a.get("cash"),
            "buying_power": a.get("buying_power"), "portfolio_value": a.get("portfolio_value"),
            "age": round(time.time() - a["ts"], 1) if a.get("ts") else None,
            "error": a.get("error", ""), "fields": a.get("fields", [])}


def _r(v: Any, n: int = 4) -> Any:
    return None if v is None else round(v, n)


def analytics_view(A: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not A:
        return None
    return {"spot": _r(A["spot"], 2), "ptb": _r(A["ptb"], 2), "gap_usd": _r(A["gap_usd"], 2),
            "gap_pct": _r(A["gap_pct"]), "tau": _r(A["tau"], 1), "sig_s": _r(A["sig_s"], 3),
            "sigma5m_pct": _r(A["sigma5m_pct"]), "twap60": _r(A["twap60"], 2), "proj_mean": _r(A["proj_mean"], 2),
            "proj_std": _r(A["proj_std"], 2), "proj_gap_pct": _r(A["proj_gap_pct"]), "z": _r(A["z"], 3),
            "p_up": _r(A["p_up"]), "slope": _r(A["slope"], 3), "frac_above": _r(A["frac_above"], 3),
            "frac_below": _r(A["frac_below"], 3), "struct_n": A["struct_n"], "struct_sec": A["struct_sec"],
            "target_src": A.get("target_src"), "target_ok": A.get("target_ok")}


def gates_view(rep: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not rep:
        return None
    c = st["confirm"]
    have = round(time.time() - c["since"], 2) if (c and c["ws"] == st["window_start"] and c["side"] == rep["side"]) else 0.0
    return {"side": rep["side"], "called": rep["called"], "blocking": rep["blocking"], "ready": rep["ready"],
            "items": [{"id": g["id"], "label": g["label"], "ok": g["ok"], "adv": g["adv"]} for g in rep["items"]],
            "p_win": rep["p_win"], "p_model": rep["p_model"], "ask": rep["ask"], "edge_px": _r(rep["edge_px"]),
            "breakeven": rep["breakeven"], "confirm_have": have, "confirm_need": cfg["confirm_sec"]}


def build_state(since: Optional[int] = None) -> Dict[str, Any]:
    now = time.time()
    ws = st["window_start"] or window_of(now)
    left = max(0.0, WINDOW - (now - ws))
    open_price, btc = st["window_open"], btc_adj()
    q = st["last_quote"] or {}
    ok, why = live_ready()
    mode = "live" if cfg["live_trading"] else "paper"
    floor = ws
    full = since is None or since < floor
    lo = floor if full else since
    b = st["basis"]
    up, dn = q.get("up"), q.get("down")
    n_win = st["win_trades"].get(ws, 0)
    A = st["analytics"]
    return {
        "name": BOT_NAME, "server_time": now, "server_time_ms": int(now * 1000), "mode": mode,
        "btc_price": btc, "btc_raw": st["btc"], "basis": round(b, 2), "window_start": ws, "window_end": ws + WINDOW,
        "window_start_ms": ws * 1000, "window_end_ms": (ws + WINDOW) * 1000, "seconds_left": round(left, 3),
        "window_open": open_price, "open_source": st["open_source"], "partial": st["partial"],
        "target_ok": st["target_rank"] >= 1, "twap_win": TWAP_WIN,
        "delta_pct": round(st["last_move"], 4),
        "delta_usd": round(btc - open_price, 2) if btc and open_price else 0.0,
        "feed": {"source": st["feed_source"], "age": round(now - st["btc_ts"], 1) if st["btc_ts"] else None,
                 "latency_ms": round(st["latency_ms"]) if st["latency_ms"] is not None else None,
                 "polymarket_ws": st["pm_status"], "rtds": st["rtds_status"], "us_bbo": st["us_status"]},
        "prices": {"up": up, "down": dn, "source": q.get("source", "none"), "age": q.get("age"), "book": q.get("book", False),
                   "up_bid": q.get("up_bid"), "up_ask": q.get("up_ask"),
                   "down_bid": q.get("down_bid"), "down_ask": q.get("down_ask"), "xgap": q.get("xgap"),
                   "up_prob": q.get("up_prob"),
                   "norm_up": round(q["up_prob"] * 100, 1) if q.get("up_prob") is not None else None,
                   "norm_down": round((1 - q["up_prob"]) * 100, 1) if q.get("up_prob") is not None else None},
        "signal": st["signal"], "analytics": analytics_view(A), "gates": gates_view(st["gate_report"]),
        "terry": {"enabled": bool(os.environ.get("TERRY_API_KEY", "").strip()), "status": st["terry_status"]},
        "polymarket": {"status": st["gamma_status"], "live": st["clob_status"], "ws": st["pm_status"],
                       "us": st["us_status"], "us_fields": st["us_keys"]},
        "position": position_view(),
        "scoreboard": scoreboard(mode),
        "thinking": st["now_thinking"],
        "chat": st["chat"][-60:],
        "recent_trades": list(reversed(st["trades"][-50:])),
        "markers": markers_view(floor),
        "history_full": full,
        # bars: [t, open, high, low, close] with this round's basis applied so they line up with the target
        "history": [[x[0], x[1] + b, x[2] + b, x[3] + b, x[4] + b] for x in st["history"] if x[0] >= lo],
        "twap_history": [[x[0], x[1] + b] for x in st["twap_hist"] if x[0] >= lo],
        "odds_history": [x for x in st["odds_hist"] if x[0] >= lo],      # [t, mid%, bid%, ask%] (Up side)
        "model_history": [x for x in st["model_hist"] if x[0] >= lo],    # [t, model P(up)%]
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
            "rtds": st["rtds_status"], "us_bbo": st["us_status"], "target": st["open_source"],
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
    if q["source"] == "estimated":
        raise HTTPException(status_code=400, detail="No real market price available, so orders are blocked.")
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
