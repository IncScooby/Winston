#!/usr/bin/env python3
"""
backtest.py -- replays Winston's REAL entry/exit logic (strategy.py) over 5-minute windows.

  python backtest.py --ticks data/ticks.jsonl            # REAL data recorded by the bot (use this one)
  python backtest.py --synthetic --windows 1500 --lag 2  # simulated market, to test the machinery
  python backtest.py --ticks data/ticks.jsonl --sweep    # walk-forward parameter search (train 60% / test 40%)
  python backtest.py --synthetic --compare               # v6-style presets vs v7 defaults, across market lags

READ THIS BEFORE TRUSTING A NUMBER
  * Synthetic results only prove the code behaves. The simulated market has exactly the edge you dial in with
    --lag (how many seconds Polymarket's price trails BTC). lag 0 = perfectly efficient market = no edge.
    They say NOTHING about whether the real market has that lag.
  * Only a run on ticks.jsonl recorded by the live bot (RECORD_TICKS=1) says anything about reality, and even then
    it is in-sample for whatever you tuned. Use --sweep (walk-forward) and look at the TEST column.
  * Fills are modelled as buy at ask / sell at bid / settle at $1 or $0 / fee on stake, same as paper mode. Real
    fills can be worse (queue position, partial fills, your own market impact).
  * A win rate means nothing alone: buying at 65c needs ~67% wins just to break even after fees. Look at ROI and
    the break-even win rate printed next to it.
"""
from __future__ import annotations

import argparse
import copy
import json
import math
import random
import sys
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional

import strategy as S

WINDOW = S.WINDOW
EST_VOL = 0.10  # same default as bot.py (EST_VOL_PCT)

# ---- settings: identical to bot.py DEFAULT_CONFIG (entry/exit part) --------------------------------------------
V7 = {
    "min_confidence": 60.0, "min_btc_move": 0.02, "entry_min_sec": 20, "entry_max_sec": 60,
    "max_entry_price": 0.74, "min_entry_price": 0.40, "confirm_sec": 4.0, "min_edge": 0.04,
    "align_min_odds": 0.55, "max_spread": 0.05, "terry_weight": 0.5, "vol_lookback_sec": 90, "min_win_prob": 0.62,
    "chop_lookback_sec": 30, "max_chop_crosses": 1, "max_trades_per_window": 1, "reentry_cooldown_sec": 6,
    "after_loss_cooldown_sec": 30, "max_window_losses": 2, "knife_pts": 0.05, "knife_window_sec": 6,
    "take_profit": 0.08, "stop_loss": 0.10, "force_exit_sec": 150, "scalp_mode": True, "inval_buffer_pct": 0.015,
    "inval_hold_sec": 4.0, "min_hold_sec": 3.0, "trail_pct": 0.07, "hold_to_settle_price": 0.95, "stake_usd": 10.0,
    "fee_pct": 2.0, "loss_streak_pause": 3, "streak_pause_windows": 2, "signal_source": "momentum",
    "winston_veto": True, "live_trading": False, "paused": False,
}
# The v6 dashboard presets (index.html) for comparison. Gates the old bot lacked are switched off here.
OFF = {"min_win_prob": 0.0, "max_chop_crosses": 99}
PRESETS = {
    "v7 default": V7,
    "v6 patient": {**V7, **OFF, "min_btc_move": .02, "min_confidence": 62, "entry_min_sec": 20, "entry_max_sec": 90,
                   "max_entry_price": .70, "min_entry_price": .40, "confirm_sec": 3, "min_edge": .03,
                   "align_min_odds": .55, "max_spread": .03, "take_profit": .06, "stop_loss": .10, "trail_pct": .04},
    "v6 scalper": {**V7, **OFF, "min_btc_move": .006, "min_confidence": 55, "entry_min_sec": 5, "entry_max_sec": 75,
                   "max_entry_price": .72, "min_entry_price": .40, "confirm_sec": 1.5, "min_edge": .01,
                   "align_min_odds": .50, "max_spread": .04, "max_trades_per_window": 2, "take_profit": .06,
                   "stop_loss": .12, "trail_pct": .04},
    "v6 hunter": {**V7, **OFF, "min_btc_move": .003, "min_confidence": 50, "entry_min_sec": 3, "entry_max_sec": 120,
                  "max_entry_price": .78, "min_entry_price": .30, "confirm_sec": 1, "min_edge": 0, "align_min_odds": .45,
                  "max_spread": .06, "max_trades_per_window": 3, "max_window_losses": 2, "take_profit": .06,
                  "stop_loss": .12, "trail_pct": .04, "force_exit_sec": 150},
}


# ---- data ------------------------------------------------------------------------------------------------------
# A window is a list of 300 per-second dicts {btc, up, down, ub, ua, db, da} plus its `target`.
def load_ticks(path: str) -> List[Dict[str, Any]]:
    by: Dict[int, List[dict]] = defaultdict(list)
    skipped = 0
    with open(path) as f:
        for line in f:
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("partial") or r.get("src") in (None, "estimated") or r.get("up") is None or not r.get("target"):
                skipped += 1
                continue
            by[r["ws"]].append(r)
    wins = []
    for ws, rows in sorted(by.items()):
        rows.sort(key=lambda r: r["ts"])
        if len(rows) < 240 or rows[0]["ts"] - ws > 10:  # need nearly the whole window, starting at the open
            skipped += len(rows)
            continue
        sec = [None] * WINDOW
        for r in rows:
            i = int(r["ts"] - ws)
            if 0 <= i < WINDOW:
                sec[i] = {"btc": r["btc"] + r["basis"], "up": r["up"], "down": r["down"],
                          "ub": r["ub"], "ua": r["ua"], "db": r["db"], "da": r["da"]}
        last = None
        for i in range(WINDOW):  # forward-fill small gaps
            if sec[i] is None:
                sec[i] = last
            last = sec[i]
        if sec[0] is None:
            continue
        wins.append({"ws": ws, "target": rows[0]["target"], "sec": sec})
    print(f"loaded {len(wins)} usable windows from {path} ({skipped} rows skipped)", file=sys.stderr)
    return wins


def synthetic(n: int, lag: int, seed: int, noise: float = 0.012, spread: float = 0.02, drift_sd: float = 0.0):
    """Simulated windows. BTC: random walk with per-window volatility. Market: the *fair* probability of the true
    gap, computed from BTC `lag` seconds ago + noise. lag=0 is an efficient market (no edge available)."""
    rnd = random.Random(seed)
    wins = []
    for w in range(n):
        base = 0.10 / math.sqrt(WINDOW)               # % per second
        sig = base * math.exp(rnd.gauss(0, 0.45))     # volatility regime varies window to window
        drift = rnd.gauss(0, drift_sd) * sig
        open_p = 90000.0 + rnd.uniform(-2000, 2000)
        px = [open_p]
        for _ in range(WINDOW):
            px.append(px[-1] * (1 + (rnd.gauss(drift, sig)) / 100))
        sec = []
        for t in range(WINDOW):
            seen = px[max(0, t - lag)]
            gap = (seen - open_p) / open_p * 100
            left = max(WINDOW - t, 3)
            fair = S.norm_cdf(gap / (sig * math.sqrt(left)))
            up = min(0.97, max(0.03, fair + rnd.gauss(0, noise)))
            ub, ua = max(0.01, up - spread / 2), min(0.99, up + spread / 2)
            dn = 1 - up
            sec.append({"btc": px[t], "up": round(up, 4), "down": round(dn, 4), "ub": round(ub, 4), "ua": round(ua, 4),
                        "db": round(1 - ua, 4), "da": round(1 - ub, 4)})
        wins.append({"ws": 1_700_000_000 + w * WINDOW, "target": open_p, "sec": sec, "final": px[WINDOW]})
    return wins


# ---- simulator -------------------------------------------------------------------------------------------------
def simulate(wins: List[dict], cfg: Dict[str, Any]) -> Dict[str, Any]:
    trades: List[dict] = []
    rejects: Counter = Counter()
    seen_in_window = 0
    loss_streak, pause_until = 0, 0.0
    since_exit, last_exit_loss = 1e9, False
    for W in wins:
        ws, target, sec = W["ws"], W["target"], W["sec"]
        n_tr = n_loss = 0
        blocked: set = set()
        pos: Optional[dict] = None
        side_since = {"side": None, "t": 0}
        prices: List[float] = []
        for t in range(WINDOW):
            r = sec[t]
            prices.append(r["btc"])
            move = (r["btc"] - target) / target * 100
            q = {"up": r["up"], "down": r["down"], "up_bid": r["ub"], "up_ask": r["ua"], "down_bid": r["db"],
                 "down_ask": r["da"], "source": "sim"}
            if not S.valid_book(r["ub"], r["ua"], r["db"], r["da"], 0.08):
                continue
            now = ws + t
            since_exit += 1
            if pos is not None:
                reason, _ = S.exit_reason(cfg, pos, q, t, move, now, True)
                if reason:
                    px = S.fill_px(q, pos["side"], "sell")
                    trades.append(_close(pos, px, reason, cfg, t))
                    last_exit_loss, since_exit = trades[-1]["pnl"] < 0, 0
                    loss_streak, pause_until = _streak(trades[-1], loss_streak, pause_until, cfg, ws)
                    if trades[-1]["pnl"] < 0:
                        n_loss += 1
                        blocked.add(pos["side"])
                    pos = None
                continue
            left = WINDOW - t
            lean = "UP" if move >= cfg["min_btc_move"] else "DOWN" if move <= -cfg["min_btc_move"] else None
            if side_since["side"] != lean:
                side_since = {"side": lean, "t": t}
            if not (cfg["entry_min_sec"] <= t <= cfg["entry_max_sec"]) or n_tr >= cfg["max_trades_per_window"]:
                continue
            if pause_until and now >= pause_until:
                pause_until, loss_streak = 0.0, 0
            sig = S.momentum_signal(cfg, move, left, EST_VOL)
            k = int(cfg["knife_window_sec"])
            drop = {s: ((sec[t - k][s.lower()] - r[s.lower()]) if t >= k else 0.0) for s in ("UP", "DOWN")}
            ctx = {"elapsed": t, "move": move, "signal": sig, "quote": q, "prices": prices[-150:], "target": target,
                   "side_held": (t - side_since["t"]) if side_since["side"] == sig["side"] and sig["side"] else 0.0,
                   "odds_drop": drop, "n_trades": n_tr, "n_losses": n_loss, "last_exit_loss": last_exit_loss,
                   "since_exit": since_exit, "side_blocked": blocked, "position": False, "paused": False,
                   "partial": False, "entry_blocked": False, "pause_left": max(0.0, pause_until - now),
                   "live": False, "est_vol_pct": EST_VOL}
            side, p, edge, G, _ = S.evaluate_entry(cfg, ctx)
            seen_in_window += 1
            bad = next((g["key"] for g in G if not g["ok"]), None)
            if bad:
                rejects[bad] += 1
                continue
            price = S.fill_px(q, side, "buy")
            pos = {"side": side, "entry": price, "shares": cfg["stake_usd"] / price, "stake": cfg["stake_usd"],
                   "opened_at": now, "peak": price, "inval_since": None, "t_in": t, "p": p, "edge": edge}
            n_tr += 1
        if pos is not None:  # settle at the end of the window
            final = W.get("final", sec[-1]["btc"])
            won = (pos["side"] == "UP") == (final >= target)
            trades.append(_close(pos, 1.0 if won else 0.0, "settled " + ("win" if won else "loss"), cfg, WINDOW))
            loss_streak, pause_until = _streak(trades[-1], loss_streak, pause_until, cfg, ws)
            last_exit_loss, since_exit = False, 0
    return {"trades": trades, "rejects": rejects, "windows": len(wins), "evals": seen_in_window}


def _close(pos, px, reason, cfg, t):
    fee = pos["stake"] * cfg["fee_pct"] / 100
    pnl = pos["shares"] * (px - pos["entry"]) - fee
    return {"side": pos["side"], "entry": pos["entry"], "exit": px, "pnl": pnl, "stake": pos["stake"], "reason": reason,
            "t_in": pos["t_in"], "t_out": t, "p": pos["p"]}


def _streak(tr, streak, pause_until, cfg, ws):
    if tr["pnl"] > 0:
        return 0, pause_until
    streak += 1
    if cfg["streak_pause_windows"] > 0 and streak >= cfg["loss_streak_pause"]:
        pause_until = ws + (1 + cfg["streak_pause_windows"]) * WINDOW
    return streak, pause_until


# ---- reporting -------------------------------------------------------------------------------------------------
def wilson(w: int, n: int, z: float = 1.96):
    if n == 0:
        return 0.0, 0.0
    p = w / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return c - h, c + h


def summarize(res: Dict[str, Any]) -> Dict[str, Any]:
    T = res["trades"]
    n = len(T)
    if not n:
        return {"n": 0, "wr": 0, "roi": 0, "pnl": 0, "dd": 0, "be": 0, "ci": (0, 0), "avg_w": 0, "avg_l": 0, "per_win": 0}
    wins = [t for t in T if t["pnl"] > 0]
    losses = [t for t in T if t["pnl"] <= 0]
    stake = sum(t["stake"] for t in T)
    pnl = sum(t["pnl"] for t in T)
    eq = peak = dd = 0.0
    for t in T:
        eq += t["pnl"]
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
    avg_entry = sum(t["entry"] for t in T) / n
    return {"n": n, "wr": len(wins) / n, "roi": pnl / stake, "pnl": pnl, "dd": dd, "be": avg_entry * 1.02,
            "ci": wilson(len(wins), n), "avg_w": sum(t["pnl"] for t in wins) / max(1, len(wins)),
            "avg_l": sum(t["pnl"] for t in losses) / max(1, len(losses)), "per_win": n / max(1, res["windows"])}


def line(name: str, s: Dict[str, Any]) -> str:
    if not s["n"]:
        return f"{name:<14} no trades"
    return (f"{name:<14} trades {s['n']:>4} ({s['per_win'] * 100:>3.0f}% of rounds) | win rate {s['wr'] * 100:>5.1f}% "
            f"[95% CI {s['ci'][0] * 100:.0f}-{s['ci'][1] * 100:.0f}] vs break-even {s['be'] * 100:>4.1f}% | "
            f"ROI {s['roi'] * 100:>+6.1f}% | P&L ${s['pnl']:>+8.2f} | max drawdown ${s['dd']:.2f}")


def report(res, cfg, title=""):
    s = summarize(res)
    print(f"\n=== {title} ===")
    print(line("result", s))
    if s["n"]:
        why = Counter(t["reason"] for t in res["trades"])
        print("exits:", ", ".join(f"{k} {v}" for k, v in why.most_common()))
        print(f"avg win ${s['avg_w']:+.2f}, avg loss ${s['avg_l']:+.2f}")
    tot = sum(res["rejects"].values())
    if tot:
        print("gate that blocked entry first (share of in-window checks): " +
              ", ".join(f"{k} {v / max(1, res['evals']) * 100:.0f}%" for k, v in res["rejects"].most_common(6)))
    return s


GRID = {"min_edge": [0.02, 0.04, 0.06, 0.08], "min_win_prob": [0.55, 0.62, 0.70], "confirm_sec": [2.0, 4.0, 6.0],
        "take_profit": [0.06, 0.08, 0.12], "stop_loss": [0.08, 0.10, 0.15], "entry_max_sec": [45, 60, 90]}


def sweep(wins, base, min_trades=30):
    cut = int(len(wins) * 0.6)
    train, test = wins[:cut], wins[cut:]
    keys = list(GRID)
    combos = [dict()]
    for k in keys:
        combos = [{**c, k: v} for c in combos for v in GRID[k]]
    rnd = random.Random(1)
    rnd.shuffle(combos)
    combos = combos[:150]
    rows = []
    for c in combos:
        cfg = {**base, **c}
        s = summarize(simulate(train, cfg))
        if s["n"] >= min_trades:
            rows.append((s["roi"], c, s))
    rows.sort(key=lambda x: -x[0])
    print(f"\nWalk-forward sweep: {len(combos)} combos tried on {len(train)} TRAIN windows (>= {min_trades} trades only).")
    print("Look at TEST. If TEST ROI is far below TRAIN ROI, the 'best' setting was just curve-fit noise.\n")
    for roi, c, s in rows[:5]:
        t = summarize(simulate(test, {**base, **c}))
        print(f"train ROI {roi * 100:+6.1f}% (n={s['n']:>3}) | TEST ROI {t['roi'] * 100:+6.1f}% win {t['wr'] * 100:4.1f}% (n={t['n']:>3}) | {c}")
    if not rows:
        print("no combination produced enough trades; record more data first")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ticks")
    ap.add_argument("--synthetic", action="store_true")
    ap.add_argument("--windows", type=int, default=1500)
    ap.add_argument("--lag", type=int, default=2, help="synthetic only: seconds the market price trails BTC")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--stake", type=float, default=10.0)
    ap.add_argument("--sweep", action="store_true")
    ap.add_argument("--compare", action="store_true")
    a = ap.parse_args()
    if not a.ticks and not a.synthetic:
        ap.error("give --ticks FILE (real data) or --synthetic")
    V7["stake_usd"] = a.stake
    if a.compare:
        for lag in (0, 1, 2, 4):
            wins = synthetic(a.windows, lag, a.seed)
            print(f"\n##### synthetic market, Polymarket trails BTC by {lag}s ({a.windows} windows) #####")
            for name, cfg in PRESETS.items():
                print(line(name, summarize(simulate(wins, {**cfg, "stake_usd": a.stake}))))
        return
    wins = load_ticks(a.ticks) if a.ticks else synthetic(a.windows, a.lag, a.seed)
    if not wins:
        sys.exit("no usable windows")
    tag = f"REAL ticks ({len(wins)} windows)" if a.ticks else f"SYNTHETIC lag={a.lag}s ({len(wins)} windows)"
    if a.sweep:
        sweep(wins, V7)
    else:
        report(simulate(wins, V7), V7, "v7 default on " + tag)


if __name__ == "__main__":
    main()
