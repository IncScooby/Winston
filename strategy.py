"""
strategy.py -- Winston's decision logic as PURE functions (no I/O, no globals, no clock).

The live bot (bot.py) and the backtester (backtest.py) both call exactly this code, so what you backtest is what
trades. Everything the decision needs is passed in through `cfg` (settings) and `c` (the context snapshot).

Entry context `c` (dict)
  elapsed       seconds since the window opened
  move          % BTC is from the target (basis-adjusted; + means above target)
  signal        {"side": "UP"|"DOWN"|None, "confidence": 0-100|None, "detail": str}
  quote         {"up","down","up_bid","up_ask","down_bid","down_ask","source"}
  prices        list of per-second BTC prices, basis-adjusted, oldest -> newest (last ~120s is enough)
  target        the window's price-to-beat (same basis as `prices`)
  side_held     seconds the BTC gap has stayed on the signal's side of the target
  odds_drop     {"UP": pts, "DOWN": pts} how far each contract fell over the knife window (0 if unknown)
  n_trades, n_losses     trades / losses already taken this window
  last_exit_loss, since_exit   was the last exit a loss, and seconds since it
  side_blocked  set of sides that already lost this window
  position      True when a position is open
  paused, partial, entry_blocked, pause_left      engine flags
  live          True in live mode (requires a real market price)
  est_vol_pct   long-run 5-minute volatility used as the floor for realized vol
"""
from __future__ import annotations

import math
import statistics
from typing import Any, Dict, List, Optional, Tuple

WINDOW = 300


# ------------------------------------------------------------------ maths ----
def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def norm_ppf(p: float) -> float:
    """Inverse normal CDF by bisection (plenty fast for one call per tick)."""
    p = min(0.9999, max(0.0001, p))
    lo, hi = -6.0, 6.0
    for _ in range(40):
        mid = (lo + hi) / 2
        if norm_cdf(mid) < p:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def fill_px(q: Dict[str, Any], side: str, kind: str) -> float:
    """Realistic fill: buys pay the ask, sells hit the bid; falls back to the midpoint."""
    k = side.lower()
    v = q.get(f"{k}_ask") if kind == "buy" else q.get(f"{k}_bid")
    return float(v if v else q[k])


def valid_book(ub, ua, db, da, tol: float) -> bool:
    """Structural check of a two-sided Polymarket quote pair. A binary market obeys:
         0 < bid <= ask < 1 on each side,
         mid_up + mid_down ~ 1,
         ask_up ~ 1 - bid_down and ask_down ~ 1 - bid_up  (buying UP == selling DOWN).
    Anything that breaks these is stale or from a different market, and is rejected (never traded on)."""
    if None in (ub, ua, db, da):
        return False
    if not (0 < ub <= ua < 1 and 0 < db <= da < 1):
        return False
    if abs((ub + ua) / 2 + (db + da) / 2 - 1) > tol:
        return False
    return abs(ua - (1 - db)) <= tol and abs(da - (1 - ub)) <= tol


def sigma_rem_pct(prices: List[float], left: float, lookback: int, est_vol_pct: float) -> float:
    """1-sigma BTC move (in %) expected over the time left, from realized per-second volatility, floored at half the
    long-run vol so one quiet minute can't make the model overconfident."""
    h = prices[-int(lookback):]
    rets = [(h[i] - h[i - 1]) / h[i - 1] * 100 for i in range(1, len(h)) if h[i - 1]]
    long_run = est_vol_pct / math.sqrt(WINDOW)
    s1 = max(statistics.pstdev(rets) if len(rets) >= 15 else long_run, 0.5 * long_run)
    return s1 * math.sqrt(max(left, 3.0))


def target_crosses(prices: List[float], target: float, lookback: int) -> int:
    """How many times price crossed the target in the last `lookback` seconds. A price that keeps stepping
    over the line is noise, not momentum."""
    h = prices[-int(lookback):]
    n, prev = 0, 0
    for p in h:
        s = 1 if p > target else -1 if p < target else 0
        if s and prev and s != prev:
            n += 1
        if s:
            prev = s
    return n


def win_probability(edge_move_pct: float, sigma_pct: float, conf: Optional[float], terry: bool,
                    terry_weight: float) -> Tuple[float, float]:
    """(blended p, model p). Drift-free random-walk model: P(win) = Phi(gap / sigma_remaining). In Terry mode it is
    blended with Terry's own confidence. This is a sanity check on the price, not a proven edge."""
    p_model = min(0.98, max(0.02, norm_cdf(edge_move_pct / sigma_pct)))
    p = terry_weight * conf / 100 + (1 - terry_weight) * p_model if terry and conf is not None else p_model
    return p, p_model


# ------------------------------------------------------------------ entry ----
def evaluate_entry(cfg: Dict[str, Any], c: Dict[str, Any]):
    """Every entry rule as a named gate. A trade needs ALL gates green.
    Returns (side, p_win, edge, gates, info). `gates` is also what the dashboard renders."""
    elapsed, move, sig, q = c["elapsed"], c["move"], c["signal"], c["quote"]
    side, conf = sig.get("side"), sig.get("confidence")
    terry = cfg["signal_source"] == "terrycalls"
    left = WINDOW - elapsed
    G: List[Dict[str, Any]] = []

    def gate(key: str, label: str, ok: bool, note: str = "") -> None:
        G.append({"key": key, "label": label, "ok": bool(ok), "note": note})

    n, losses = c["n_trades"], c["n_losses"]
    cd = cfg["after_loss_cooldown_sec"] if c["last_exit_loss"] else cfg["reentry_cooldown_sec"]
    block = ("paused" if c["paused"] else "a position is already open" if c["position"] else
             "joined late, target unreliable" if c["partial"] else
             f"standing down after a losing streak ({c['pause_left']:.0f}s)" if c.get("pause_left", 0) > 0 else
             f"already took {n} trade(s) this round (max {int(cfg['max_trades_per_window'])})"
             if n >= cfg["max_trades_per_window"] else
             f"{losses} losses this round" if losses >= cfg["max_window_losses"] else
             "cooling down after the last exit" if n and c["since_exit"] < cd else
             "recent order error, pausing briefly" if c["entry_blocked"] else
             f"{side} already lost this round" if side in c["side_blocked"] else "")
    gate("allowed", "Free to trade", not block, block)
    gate("window", f"Entry window {cfg['entry_min_sec']}-{cfg['entry_max_sec']}s (now {elapsed:.0f}s)",
         cfg["entry_min_sec"] <= elapsed <= cfg["entry_max_sec"])
    ok_sig = side is not None and conf is not None and conf >= cfg["min_confidence"]
    gate("signal", f"Signal {side or '-'} {'' if conf is None else f'{conf:.0f}%'} (needs {cfg['min_confidence']:.0f}%)",
         ok_sig, "" if ok_sig else sig.get("detail", ""))
    if side is None or conf is None:
        return None, None, None, G, {}

    k = side.lower()
    edge_move = move if side == "UP" else -move
    gate("gap", f"BTC {edge_move:+.3f}% on {side}'s side (needs {cfg['min_btc_move']:.3f}%)",
         edge_move >= cfg["min_btc_move"] or (terry and not cfg["winston_veto"]))

    prices = c["prices"]
    slope_ok = len(prices) < 9 or (prices[-1] - prices[-9]) * (1 if side == "UP" else -1) >= 0
    gate("confirm", f"Held {c['side_held']:.0f}s of {cfg['confirm_sec']:.0f}s, last 8s slope "
                    f"{'with' if slope_ok else 'AGAINST'} {side}", c["side_held"] >= cfg["confirm_sec"] and slope_ok)

    crosses = target_crosses(prices, c["target"], int(cfg["chop_lookback_sec"]))
    gate("chop", f"Crossed the target {crosses}x in {cfg['chop_lookback_sec']:.0f}s (max {int(cfg['max_chop_crosses'])})",
         crosses <= cfg["max_chop_crosses"])

    mid, ask, bid = q[k], fill_px(q, side, "buy"), fill_px(q, side, "sell")
    spread = (ask - bid) if q.get(k + "_ask") and q.get(k + "_bid") else None
    gate("odds", f"Market agrees: {side} at {round(mid * 100)}% (needs {round(cfg['align_min_odds'] * 100)}%+)",
         mid >= cfg["align_min_odds"])
    gate("price", f"Entry price {round(ask * 100)}% inside {round(cfg['min_entry_price'] * 100)}-"
                  f"{round(cfg['max_entry_price'] * 100)}%", cfg["min_entry_price"] <= ask <= cfg["max_entry_price"])
    gate("spread", "Spread " + ("n/a" if spread is None else f"{spread * 100:.0f} pts") + f" (max {cfg['max_spread'] * 100:.0f})",
         spread is None or spread <= cfg["max_spread"])
    drop = c["odds_drop"].get(side, 0.0)
    gate("knife", f"{side} moved {-drop * 100:+.0f} pts in {cfg['knife_window_sec']:.0f}s (max drop {cfg['knife_pts'] * 100:.0f})",
         drop < cfg["knife_pts"])

    sigma = sigma_rem_pct(prices, left, int(cfg["vol_lookback_sec"]), c["est_vol_pct"])
    p, p_model = win_probability(edge_move, sigma, conf, terry, cfg["terry_weight"])
    cost = ask * (1 + cfg["fee_pct"] / 100)
    edge = p - cost
    gate("edge", f"Win chance {p * 100:.0f}% vs cost {cost * 100:.0f}% = {edge * 100:+.1f} pts "
                 f"(needs +{cfg['min_edge'] * 100:.0f} and {cfg['min_win_prob'] * 100:.0f}%+)",
         edge >= cfg["min_edge"] and p >= cfg["min_win_prob"],
         f"model {p_model * 100:.0f}%" + (f", Terry {conf:.0f}%" if terry else ""))
    if c["live"]:
        gate("real", "Real market price (live)", q["source"] != "estimated")

    # market-implied gap: the BTC distance the contract's own price is pricing in (diagnostic only)
    implied_z = norm_ppf(q["up"] / max(1e-9, q["up"] + q["down"]))
    info = {"side": side, "p": round(p, 3), "p_model": round(p_model, 3), "edge": round(edge, 4),
            "sigma_rem_pct": round(sigma, 4), "confirm_sec": round(c["side_held"], 1), "crosses": crosses,
            "market_gap_pct": round(implied_z * sigma, 4), "our_gap_pct": round(move, 4), "cost": round(cost, 4)}
    return side, p, edge, G, info


# ------------------------------------------------------------------- exit ----
def exit_reason(cfg: Dict[str, Any], pos: Dict[str, Any], q: Dict[str, Any], elapsed: float, move: float,
                now: float, same_class: bool = True) -> Tuple[Optional[str], str]:
    """Exit rules in priority order. Mutates pos['peak'] / pos['inval_since']. Returns (reason|None, state label).
      1. gap invalidated   BTC on the wrong side of the target by > inval_buffer_pct for inval_hold_sec
      2. emergency stop    contract mid fell stop_loss below entry (after min_hold_sec)
      3. take profit       (scalp_mode) bid >= entry + take_profit
      4. profit locked     (trailing mode) after the peak armed, price dropped trail_pct or the gap flipped
      5. time exit         at force_exit_sec, unless riding a near-certain winner to settlement"""
    side = pos["side"]
    mid, bid = q[side.lower()], fill_px(q, side, "sell")
    held = now - pos["opened_at"]
    pos["peak"] = max(pos.get("peak", pos["entry"]), mid)
    edge = move if side == "UP" else -move
    if edge < -cfg["inval_buffer_pct"]:
        pos["inval_since"] = pos.get("inval_since") or now
    else:
        pos["inval_since"] = None
    riding = mid >= cfg["hold_to_settle_price"] and edge > 0
    armed = pos["peak"] >= pos["entry"] + cfg["take_profit"] and not cfg["scalp_mode"]
    reason = None
    if pos["inval_since"] and now - pos["inval_since"] >= cfg["inval_hold_sec"]:
        reason = "gap invalidated"
    elif same_class and held >= cfg["min_hold_sec"] and pos["entry"] - mid >= cfg["stop_loss"]:
        reason = "emergency stop"
    elif same_class and cfg["scalp_mode"] and bid >= pos["entry"] + cfg["take_profit"]:
        reason = "take profit"
    elif same_class and armed and not riding and (pos["peak"] - mid >= cfg["trail_pct"] or edge <= 0):
        reason = "profit locked" if bid > pos["entry"] else "gave back profit"
    elif elapsed >= cfg["force_exit_sec"] and not riding:
        reason = "time exit"
    label = "riding to settlement" if riding else ("trailing a winner" if armed else "holding")
    return reason, label


# ----------------------------------------------------------------- signal ----
def momentum_signal(cfg: Dict[str, Any], move: float, left: float, est_vol_pct: float) -> Dict[str, Any]:
    """Momentum source: BTC's move from the target plus a drift-free probability model."""
    sigma = est_vol_pct * math.sqrt(max(left, 3.0) / WINDOW)
    p_up = min(0.98, max(0.02, norm_cdf(move / sigma)))
    lean = "UP" if move >= 0 else "DOWN"
    conf = round(max(p_up, 1 - p_up) * 100, 1)
    strong = abs(move) >= cfg["min_btc_move"]
    detail = (f"BTC is {move:+.3f}% from the target. My model gives {lean} a {conf:.0f}% chance." if strong else
              f"BTC is only {move:+.3f}% from the target. I need at least +/-{cfg['min_btc_move']:.3f}% "
              f"before I consider a trade.")
    return {"source": "momentum", "side": lean if strong else None, "lean": lean, "confidence": conf,
            "status": "ready" if strong else "waiting", "detail": detail}
