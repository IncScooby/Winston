"""
Polymarket 5-minute BTC Up/Down  --  PAPER TRADING bot with a live browser dashboard.

* Uses only the Python standard library (no pip installs needed).
* NEVER places real orders. It simulates trades so you can measure real edge first.
* Run:   python polymarket_paper_bot.py
* Open:  http://localhost:8000

Strategy v0 (every number lives in CONFIG and can also be changed live from the
dashboard sliders):
  - Every 5-minute window, remember BTC's price at the start (the "open").
  - Between ENTRY_MIN_SEC and ENTRY_MAX_SEC into the window, if BTC has moved
    more than MIN_MOVE_PCT from the open, consider buying the matching side
    (Up if BTC is above the open, Down if below) -- but only if the market price
    for that side is still inside the allowed range, so there is room to profit.
  - Exit early on take-profit, stop-loss, or a hard time exit. Never holds to
    the end of the window.
  - A daily loss cap stops it from trading for the rest of the day.
"""

import csv
import json
import os
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ----------------------------------------------------------------- CONFIG --
CONFIG = {
    "WINDOW_SEC": 300,          # 5-minute markets
    "ENTRY_MIN_SEC": 60,        # don't enter in the first minute (too noisy)
    "ENTRY_MAX_SEC": 200,       # don't enter late (no time to exit)
    "FORCE_EXIT_SEC": 250,      # always be out by this point
    "MIN_MOVE_PCT": 0.04,       # BTC must have moved this % from window open
    "MAX_ENTRY_PRICE": 0.78,    # skip if the side already costs more than this
    "MIN_ENTRY_PRICE": 0.35,    # skip if the side is too cheap (market disagrees)
    "TAKE_PROFIT": 0.08,        # exit when price is up this much (in $ per share)
    "STOP_LOSS": 0.06,          # exit when price is down this much
    "STAKE_USD": 10.0,          # paper stake per trade
    "FEE_PCT": 2.0,             # pessimistic round-trip fee/slippage guess (%)
    "DAILY_LOSS_CAP_USD": 30.0, # stop trading for the day after losing this much
    "POLL_SEC": 2.0,            # how often to check prices
    "PORT": 8000,
    "LOG_FILE": "paper_trades.csv",
}

# Settings the dashboard is allowed to change while the bot runs.
TUNABLE = {"MIN_MOVE_PCT", "MAX_ENTRY_PRICE", "MIN_ENTRY_PRICE", "TAKE_PROFIT",
           "STOP_LOSS", "STAKE_USD", "DAILY_LOSS_CAP_USD"}

# ------------------------------------------------------------------ STATE --
state = {
    "config": CONFIG,
    "btc_price": None,
    "window_start": None,
    "window_open_price": None,
    "elapsed": 0,
    "partial": False,           # True if we joined a window late (no honest open price)
    "market": None,             # {"up": price, "down": price, "slug": ...}
    "market_error": None,
    "position": None,           # dict while a trade is open
    "trades": [],               # closed trades
    "markers": [],              # chart markers for entries/exits
    "history": [],              # [unix_ts, btc_price]
    "mkt_history": [],          # [unix_ts, up, down]
    "pnl_today": 0.0,
    "pnl_total": 0.0,
    "paused": False,
    "paused_reason": "",
    "mood": {"score": 0, "label": "Warming up", "emoji": "😴"},
    "now_thinking": "Starting up...",
    "thoughts": [],             # feed of decisions, newest last
    "last_update": None,
    "log": [],
}
# RLock (re-entrant): functions that hold the lock may safely call log()/think().
lock = threading.RLock()
_last_thought_kind = [None]


def log(msg):
    stamp = time.strftime("%H:%M:%S")
    with lock:
        state["log"].append(f"{stamp}  {msg}")
        state["log"] = state["log"][-60:]
    print(f"{stamp}  {msg}", flush=True)


def think(kind, text, tone="wait"):
    """Update the 'what I'm thinking right now' line; add to the feed when the kind changes."""
    with lock:
        state["now_thinking"] = text
        if kind != _last_thought_kind[0]:
            _last_thought_kind[0] = kind
            state["thoughts"].append({"time": time.strftime("%H:%M:%S"), "text": text, "tone": tone})
            state["thoughts"] = state["thoughts"][-40:]


# ------------------------------------------------------------ DATA FEEDS --
def http_json(url, timeout=6):
    req = urllib.request.Request(url, headers={"User-Agent": "paper-bot/0.2"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def get_btc_price():
    """Coinbase public spot price (works from the US, no API key)."""
    data = http_json("https://api.coinbase.com/v2/prices/BTC-USD/spot")
    return float(data["data"]["amount"])


def get_market_prices(window_start):
    """
    Best-effort lookup of the 5-minute BTC Up/Down market on Polymarket.
    ASSUMPTION (verify): slug looks like  btc-updown-5m-<window_start_unix>
    These are Gamma API 'last price' values, not live order-book quotes, so
    treat paper results as optimistic until we switch to the CLOB order book.
    """
    slug = f"btc-updown-5m-{window_start}"
    data = http_json(f"https://gamma-api.polymarket.com/events?slug={slug}")
    if not data:
        raise RuntimeError(f"no market found for slug {slug}")
    market = data[0]["markets"][0]
    outcomes = json.loads(market["outcomes"]) if isinstance(market["outcomes"], str) else market["outcomes"]
    prices = json.loads(market["outcomePrices"]) if isinstance(market["outcomePrices"], str) else market["outcomePrices"]
    mapped = {o.lower(): float(p) for o, p in zip(outcomes, prices)}
    return {"up": mapped.get("up"), "down": mapped.get("down"), "slug": slug}


# --------------------------------------------------------------- TRADING --
def append_trade_csv(t):
    new = not os.path.exists(CONFIG["LOG_FILE"])
    with open(CONFIG["LOG_FILE"], "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["time", "slug", "side", "entry", "exit", "reason",
                        "btc_move_pct", "stake", "pnl_usd"])
        w.writerow([t["time"], t["slug"], t["side"], t["entry"], t["exit"],
                    t["reason"], t["btc_move_pct"], t["stake"], round(t["pnl"], 4)])


def add_marker(kind, side):
    state["markers"].append({"ts": int(time.time()), "price": state["btc_price"],
                             "kind": kind, "side": side})
    state["markers"] = state["markers"][-60:]


def close_position(exit_price, reason):
    pos = state["position"]
    shares = pos["stake"] / pos["entry"]
    gross = (exit_price - pos["entry"]) * shares
    fees = pos["stake"] * CONFIG["FEE_PCT"] / 100.0
    pnl = gross - fees
    trade = {**pos, "exit": round(exit_price, 4), "reason": reason, "pnl": pnl,
             "time": time.strftime("%Y-%m-%d %H:%M:%S")}
    state["trades"].append(trade)
    state["trades"] = state["trades"][-200:]
    state["pnl_today"] += pnl
    state["pnl_total"] += pnl
    state["position"] = None
    add_marker("exit", pos["side"])
    append_trade_csv(trade)
    log(f"EXIT {pos['side']} @ {exit_price:.3f} ({reason})  pnl ${pnl:+.2f}")
    emoji = "✅" if pnl > 0 else "❌"
    think(f"exit{len(state['trades'])}",
          f"{emoji} Sold {pos['side'].upper()} at {exit_price:.2f} ({reason}). "
          f"Result {pnl:+.2f} USD after fees.", "win" if pnl > 0 else "loss")
    if state["pnl_today"] <= -CONFIG["DAILY_LOSS_CAP_USD"]:
        state["paused"] = True
        state["paused_reason"] = "daily loss cap hit"
        log("Daily loss cap hit -- trading paused until the next day.")


def update_mood(move_pct):
    thr = max(CONFIG["MIN_MOVE_PCT"], 1e-9)
    strength = min(abs(move_pct) / thr, 1.5) / 1.5          # 0..1
    score = round((1 if move_pct >= 0 else -1) * strength * 100)
    if abs(score) < 25:
        label, emoji = "Undecided", "😐"
    elif score > 0:
        label, emoji = ("Leaning UP", "🐂") if score < 66 else ("Strongly bullish", "🚀")
    else:
        label, emoji = ("Leaning DOWN", "🐻") if score > -66 else ("Strongly bearish", "📉")
    state["mood"] = {"score": score, "label": label, "emoji": emoji}


def step():
    now = int(time.time())
    W = CONFIG["WINDOW_SEC"]
    window_start = now - (now % W)
    elapsed = now - window_start

    btc = get_btc_price()
    with lock:
        state["btc_price"] = btc
        state["last_update"] = time.strftime("%H:%M:%S")
        state["elapsed"] = elapsed
        state["history"].append([now, btc])
        state["history"] = state["history"][-900:]

        # new window -> reset window tracking, force-close anything left open
        if state["window_start"] != window_start:
            if state["position"] is not None:
                side = state["position"]["side"]
                p = state["market"][side] if state["market"] else state["position"]["entry"]
                close_position(p, "window rolled")
            first_window = state["window_start"] is None
            state["window_start"] = window_start
            state["window_open_price"] = btc
            state["market"] = None
            # if we only noticed the window late, the 'open' price isn't honest
            state["partial"] = elapsed > 8
            log(f"New window {window_start}, BTC open {btc:,.2f}"
                + ("  (joined late - sitting this one out)" if state["partial"] else ""))
            if first_window and state["partial"]:
                think("partial", "Just started up mid-window, so I missed the real open price. "
                                 "I'll sit this one out and trade from the next window.", "wait")

    # market prices (outside the lock: network call)
    try:
        market = get_market_prices(window_start)
        err = None
    except Exception as e:  # noqa: BLE001
        market, err = None, str(e)

    with lock:
        state["market"] = market
        state["market_error"] = err
        move_pct = (btc - state["window_open_price"]) / state["window_open_price"] * 100.0
        update_mood(move_pct)

        if market is None or market["up"] is None or market["down"] is None:
            think("nodata", "I can't see a Polymarket price for this window, so I'm just watching BTC. "
                            "I never trade blind.", "wait")
            return
        state["mkt_history"].append([now, market["up"], market["down"]])
        state["mkt_history"] = state["mkt_history"][-300:]

        pos = state["position"]

        # ----- manage open position
        if pos is not None:
            price = market[pos["side"]]
            delta = price - pos["entry"]
            if delta >= CONFIG["TAKE_PROFIT"]:
                close_position(price, "take profit")
            elif -delta >= CONFIG["STOP_LOSS"]:
                close_position(price, "stop loss")
            elif elapsed >= CONFIG["FORCE_EXIT_SEC"]:
                close_position(price, "time exit")
            else:
                think("holding" + pos["slug"],
                      f"Holding {pos['side'].upper()} from {pos['entry']:.2f}. Now {price:.2f} "
                      f"({delta:+.2f}). Selling at +{CONFIG['TAKE_PROFIT']:.2f} or -{CONFIG['STOP_LOSS']:.2f}, "
                      f"or at {CONFIG['FORCE_EXIT_SEC']}s.", "info")
            return

        # ----- look for an entry
        thr = CONFIG["MIN_MOVE_PCT"]
        if state["paused"]:
            think("paused", f"Paused ({state['paused_reason'] or 'by you'}). Watching only.", "wait")
            return
        if state["partial"]:
            think("partial", "Joined this window late, so my open price isn't reliable. "
                             "Waiting for the next window.", "wait")
            return
        if any(t["slug"] == market["slug"] for t in state["trades"]):
            think("done" + market["slug"], "Already traded this window. Watching until the next one.", "wait")
            return
        if elapsed < CONFIG["ENTRY_MIN_SEC"]:
            think("early", f"Only {elapsed}s into the window, which is too noisy. I'll start looking at "
                           f"{CONFIG['ENTRY_MIN_SEC']}s. BTC is {move_pct:+.3f}% so far.", "wait")
            return
        if elapsed > CONFIG["ENTRY_MAX_SEC"]:
            think("late", "Too late in this window to enter safely. Waiting for the next one.", "wait")
            return
        if abs(move_pct) < thr:
            think("small", f"BTC is only {move_pct:+.3f}% from the open. I need at least ±{thr:.2f}% "
                           f"before I'll consider a trade.", "wait")
            return

        side = "up" if move_pct > 0 else "down"
        price = market[side]
        if price > CONFIG["MAX_ENTRY_PRICE"]:
            think("pricey", f"BTC points {side.upper()}, but the market already charges {price:.2f} "
                            f"(my limit is {CONFIG['MAX_ENTRY_PRICE']:.2f}). Move is priced in, so I'm skipping.", "wait")
            return
        if price < CONFIG["MIN_ENTRY_PRICE"]:
            think("cheap", f"BTC points {side.upper()}, but the market only prices it at {price:.2f}. "
                           f"The crowd disagrees, so I'm skipping.", "wait")
            return

        state["position"] = {"slug": market["slug"], "side": side, "entry": round(price, 4),
                             "stake": CONFIG["STAKE_USD"], "btc_move_pct": round(move_pct, 4)}
        add_marker("enter", side)
        log(f"ENTER {side.upper()} @ {price:.3f}  (BTC {move_pct:+.3f}% from open)")
        think("entered" + market["slug"],
              f"🟢 Bought {side.upper()} at {price:.2f} with ${CONFIG['STAKE_USD']:.0f} (paper). "
              f"BTC is {move_pct:+.3f}% from the open.", "go")


def bot_loop():
    last_day = time.strftime("%Y-%m-%d")
    while True:
        try:
            today = time.strftime("%Y-%m-%d")
            if today != last_day:
                with lock:
                    state["pnl_today"] = 0.0
                    state["paused"] = False
                    state["paused_reason"] = ""
                last_day = today
            step()
        except Exception as e:  # noqa: BLE001
            log(f"loop error: {e}")
            think("err", f"Having trouble reaching price data ({e}). Retrying.", "wait")
        time.sleep(CONFIG["POLL_SEC"])


# ------------------------------------------------------------- DASHBOARD --
PAGE = r"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Paper Bot</title>
<style>
 :root{--bg:#0b0e14;--card:#141a26;--line:#222b3d;--txt:#e8ecf5;--mut:#8791a7;--up:#3ddc84;--dn:#ff5c5c;--acc:#7c9cff}
 *{box-sizing:border-box} body{font-family:system-ui,Segoe UI,sans-serif;background:var(--bg);color:var(--txt);margin:0;padding:14px;max-width:1100px;margin:0 auto}
 h2{font-size:13px;letter-spacing:.08em;text-transform:uppercase;color:var(--mut);margin:0 0 10px}
 .top{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:12px}
 .top h1{font-size:19px;margin:0} .pill{padding:3px 10px;border-radius:99px;font-size:12px;background:#1f2740;color:var(--acc)}
 .pill.run{background:#14301f;color:var(--up)} .pill.pause{background:#3a2a14;color:#ffb454}
 .grid{display:grid;gap:12px} .g2{grid-template-columns:1.6fr 1fr} @media(max-width:820px){.g2{grid-template-columns:1fr}}
 .card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:14px}
 .big{font-size:34px;font-weight:700;letter-spacing:-.02em} .up{color:var(--up)} .dn{color:var(--dn)} .mut{color:var(--mut)}
 canvas{width:100%;height:290px;display:block}
 .row{display:flex;gap:12px;align-items:center;justify-content:space-between;flex-wrap:wrap}
 .stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px}
 .stat .k{font-size:11px;color:var(--mut)} .stat .v{font-size:20px;margin-top:2px;font-weight:600}
 .gauge{height:14px;border-radius:99px;background:linear-gradient(90deg,#ff5c5c,#2a3042 50%,#3ddc84);position:relative;margin:10px 0 6px}
 .gauge i{position:absolute;top:-4px;width:6px;height:22px;border-radius:4px;background:#fff;box-shadow:0 0 10px #fff;transition:left .6s}
 .bar{height:26px;border-radius:8px;background:#1b2231;position:relative;overflow:hidden;margin:6px 0}
 .bar b{position:absolute;left:0;top:0;bottom:0;transition:width .5s} .bar span{position:relative;z-index:1;line-height:26px;padding-left:10px;font-size:13px}
 .think{font-size:16px;line-height:1.45;padding:12px;border-radius:12px;background:#1a2133;border:1px solid var(--line);min-height:70px}
 .feed{max-height:230px;overflow:auto;margin-top:10px;font-size:13px}
 .feed div{padding:7px 0;border-bottom:1px solid var(--line)} .feed small{color:var(--mut);margin-right:6px}
 .tone-go{color:var(--up)} .tone-win{color:var(--up)} .tone-loss{color:var(--dn)}
 button{background:#1f2740;color:var(--txt);border:1px solid #2d3858;border-radius:10px;padding:9px 14px;font-size:14px;cursor:pointer}
 button:hover{background:#27315a} button.danger{border-color:#5a2d38}
 .sl{margin:10px 0} .sl label{display:flex;justify-content:space-between;font-size:13px;color:var(--mut)} input[type=range]{width:100%;accent-color:#7c9cff}
 table{width:100%;border-collapse:collapse;font-size:13px} td,th{padding:7px 6px;text-align:left;border-bottom:1px solid var(--line)} th{color:var(--mut);font-weight:500}
 .ring{position:relative;width:110px;height:110px} .ring svg{transform:rotate(-90deg)} .ring div{position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center}
 .ring .t{font-size:22px;font-weight:700} .ring .s{font-size:11px;color:var(--mut)}
 @keyframes pulse{0%{opacity:1}50%{opacity:.4}100%{opacity:1}} .live{animation:pulse 1.4s infinite}
</style></head><body>

<div class="top">
 <h1>BTC 5-min Paper Bot</h1><span class="pill">PAPER ONLY · no real money</span>
 <span class="pill" id="status">starting…</span>
 <span class="mut" style="font-size:12px;margin-left:auto" id="upd"></span>
</div>

<div class="grid g2">
 <div class="card">
  <div class="row" style="margin-bottom:6px">
   <div><div class="mut" style="font-size:12px">BITCOIN <span class="live" style="color:var(--up)">● live</span></div>
    <div class="big" id="btc">…</div><div id="move" class="mut"></div></div>
   <div class="ring"><svg width="110" height="110"><circle cx="55" cy="55" r="48" stroke="#222b3d" stroke-width="9" fill="none"/>
    <circle id="arc" cx="55" cy="55" r="48" stroke="#7c9cff" stroke-width="9" fill="none" stroke-linecap="round" stroke-dasharray="301.6" stroke-dashoffset="0"/></svg>
    <div><span class="t" id="cd">5:00</span><span class="s">window left</span></div></div>
  </div>
  <canvas id="chart"></canvas>
  <div class="mut" style="font-size:12px;margin-top:6px">Green zone = move big enough to buy UP · Red zone = buy DOWN · Blue band = the time slot when the bot is allowed to enter · Dashed line = window open price</div>
 </div>

 <div class="grid">
  <div class="card"><h2>What the bot is thinking</h2>
   <div class="think" id="think">…</div>
   <div class="gauge"><i id="gm" style="left:50%"></i></div>
   <div class="row"><span class="mut" style="font-size:12px">Bearish</span><b id="mood">…</b><span class="mut" style="font-size:12px">Bullish</span></div>
   <div class="feed" id="feed"></div>
  </div>
  <div class="card"><h2>Polymarket prices (what the crowd thinks)</h2>
   <div class="bar"><b id="bu" style="background:#1f6b44;width:0"></b><span id="tu">Up –</span></div>
   <div class="bar"><b id="bd" style="background:#7a2f37;width:0"></b><span id="td">Down –</span></div>
   <div class="mut" style="font-size:12px" id="merr"></div>
  </div>
 </div>
</div>

<div class="grid g2" style="margin-top:12px">
 <div class="grid">
  <div class="card"><h2>Open position</h2><div id="pos"></div></div>
  <div class="card"><h2>Scoreboard</h2><div class="stats" id="stats"></div></div>
  <div class="card"><h2>Recent trades</h2><table id="trades"></table></div>
 </div>
 <div class="grid">
  <div class="card"><h2>Controls</h2>
   <div class="row"><button id="bpause">Pause</button><button id="breset" class="danger">Reset stats</button></div>
   <div id="sliders" style="margin-top:6px"></div>
   <div class="mut" style="font-size:12px;margin-top:8px">Changes apply instantly. Try different values and watch the log fill up.</div>
  </div>
  <div class="card"><h2>Log</h2><pre id="log" style="margin:0;font-size:11px;max-height:200px;overflow:auto;white-space:pre-wrap;color:var(--mut)"></pre></div>
 </div>
</div>

<script>
const $=id=>document.getElementById(id);
let S=null, slidersBuilt=false;
const money=x=>(x>=0?'+':'-')+'$'+Math.abs(x).toFixed(2);
const cls=x=>x>=0?'up':'dn';
const post=(p,b)=>fetch(p,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)});

const SL=[['MIN_MOVE_PCT','Min BTC move to enter (%)',0.01,0.2,0.01],['STAKE_USD','Paper stake per trade ($)',1,100,1],
 ['TAKE_PROFIT','Take profit (price gain)',0.02,0.2,0.01],['STOP_LOSS','Stop loss (price drop)',0.02,0.2,0.01],
 ['MAX_ENTRY_PRICE','Max price I will pay',0.5,0.95,0.01]];
function buildSliders(){
 $('sliders').innerHTML=SL.map(([k,l,mn,mx,st])=>`<div class="sl"><label><span>${l}</span><b id="v_${k}">${S.config[k]}</b></label>
  <input type="range" id="s_${k}" min="${mn}" max="${mx}" step="${st}" value="${S.config[k]}"></div>`).join('');
 SL.forEach(([k])=>{const el=$('s_'+k);
  el.oninput=()=>$('v_'+k).textContent=el.value;
  el.onchange=()=>post('/config',{[k]:el.value});});
 slidersBuilt=true;
}
$('bpause').onclick=()=>post('/control',{action:S&&S.paused?'resume':'pause'});
$('breset').onclick=()=>{if(confirm('Clear paper trades and PnL?'))post('/control',{action:'reset'});};

function draw(){
 const c=$('chart'),dpr=window.devicePixelRatio||1,w=c.clientWidth,h=c.clientHeight;
 if(c.width!==Math.round(w*dpr)){c.width=Math.round(w*dpr);c.height=Math.round(h*dpr);}
 const g=c.getContext('2d');g.setTransform(dpr,0,0,dpr,0,0);g.clearRect(0,0,w,h);
 g.font='11px system-ui';g.textBaseline='middle';
 if(!S||!S.window_open_price){g.fillStyle='#8791a7';g.fillText('Waiting for BTC data…',20,h/2);return;}
 const W=S.config.WINDOW_SEC,ws=S.window_start,open=S.window_open_price,thr=S.config.MIN_MOVE_PCT/100;
 const pts=S.history.filter(p=>p[0]>=ws);
 let lo=open*(1-thr*1.8),hi=open*(1+thr*1.8);
 pts.forEach(p=>{lo=Math.min(lo,p[1]);hi=Math.max(hi,p[1]);});
 const pad=(hi-lo)*0.06;lo-=pad;hi+=pad;
 const L=62,R=10,T=8,B=22,X=t=>L+(t-ws)/W*(w-L-R),Y=p=>T+(hi-p)/(hi-lo)*(h-T-B);
 const yu=Y(open*(1+thr)),yd=Y(open*(1-thr));
 g.fillStyle='rgba(61,220,132,.10)';g.fillRect(L,T,w-L-R,yu-T);
 g.fillStyle='rgba(255,92,92,.10)';g.fillRect(L,yd,w-L-R,h-B-yd);
 const x1=X(ws+S.config.ENTRY_MIN_SEC),x2=X(ws+S.config.ENTRY_MAX_SEC);
 g.fillStyle='rgba(124,156,255,.09)';g.fillRect(x1,T,x2-x1,h-T-B);
 g.fillStyle='#8791a7';g.fillText('buy UP zone',L+6,T+10);g.fillText('buy DOWN zone',L+6,h-B-10);
 g.setLineDash([5,5]);g.strokeStyle='#8791a7';g.beginPath();g.moveTo(L,Y(open));g.lineTo(w-R,Y(open));g.stroke();g.setLineDash([]);
 g.fillStyle='#8791a7';g.textAlign='right';
 [hi-pad,open,lo+pad].forEach(p=>g.fillText(Math.round(p).toLocaleString(),L-6,Y(p)));
 g.textAlign='center';[0,60,120,180,240,300].forEach(s=>g.fillText(Math.floor(s/60)+':'+String(s%60).padStart(2,'0'),X(ws+s),h-8));
 g.textAlign='left';
 if(pts.length>1){g.strokeStyle='#9db4ff';g.lineWidth=2;g.lineJoin='round';g.beginPath();
  pts.forEach((p,i)=>i?g.lineTo(X(p[0]),Y(p[1])):g.moveTo(X(p[0]),Y(p[1])));g.stroke();g.lineWidth=1;
  const lp=pts[pts.length-1];g.fillStyle=lp[1]>=open?'#3ddc84':'#ff5c5c';g.beginPath();g.arc(X(lp[0]),Y(lp[1]),5,0,7);g.fill();}
 (S.markers||[]).filter(m=>m.ts>=ws).forEach(m=>{const x=X(m.ts),y=Y(m.price);
  g.fillStyle=m.kind==='enter'?(m.side==='up'?'#3ddc84':'#ff5c5c'):'#ffd166';g.beginPath();
  if(m.kind==='enter'){g.moveTo(x,y-9);g.lineTo(x-7,y+5);g.lineTo(x+7,y+5);g.closePath();}else{g.arc(x,y,6,0,7);}g.fill();
  g.fillStyle='#e8ecf5';g.fillText(m.kind==='enter'?'BUY '+m.side.toUpperCase():'SELL',x+9,y-8);});
}

function render(){
 if(!S)return;
 if(!slidersBuilt)buildSliders();
 const m=S.market||{},open=S.window_open_price,btc=S.btc_price;
 $('btc').textContent=btc?'$'+btc.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}):'…';
 if(open&&btc){const mv=(btc-open)/open*100;$('move').innerHTML=`<span class="${cls(mv)}">${mv>=0?'▲':'▼'} ${mv.toFixed(3)}%</span> since this window opened`;}
 const st=$('status');st.textContent=S.paused?('PAUSED · '+(S.paused_reason||'by you')):'RUNNING';st.className='pill '+(S.paused?'pause':'run');
 $('bpause').textContent=S.paused?'Resume':'Pause';$('upd').textContent='updated '+(S.last_update||'');
 $('think').textContent=S.now_thinking;
 $('mood').textContent=S.mood.emoji+' '+S.mood.label;$('gm').style.left=(50+S.mood.score/2)+'%';
 $('feed').innerHTML=S.thoughts.slice().reverse().map(t=>`<div class="tone-${t.tone}"><small>${t.time}</small>${t.text}</div>`).join('');
 const up=m.up,dn=m.down;
 $('bu').style.width=up!=null?up*100+'%':'0';$('bd').style.width=dn!=null?dn*100+'%':'0';
 $('tu').textContent='Up '+(up!=null?(up*100).toFixed(0)+'¢':'–');$('td').textContent='Down '+(dn!=null?(dn*100).toFixed(0)+'¢':'–');
 $('merr').textContent=S.market_error?('No market data: '+S.market_error):'';
 const p=S.position;
 if(p){const cur=m[p.side]; const u=cur!=null?(cur-p.entry)/p.entry*p.stake:null;
  $('pos').innerHTML=`<div class="row"><b style="font-size:20px" class="${p.side==='up'?'up':'dn'}">${p.side.toUpperCase()}</b>
   <span>bought at <b>${p.entry}</b> · stake $${p.stake}</span><span>now <b>${cur!=null?cur.toFixed(2):'–'}</b></span>
   <b class="${u==null?'':cls(u)}" style="font-size:20px">${u==null?'':money(u)}</b></div>
   <div class="mut" style="font-size:12px;margin-top:8px">Sells at ${(p.entry+S.config.TAKE_PROFIT).toFixed(2)} (profit) or ${(p.entry-S.config.STOP_LOSS).toFixed(2)} (stop), or by ${S.config.FORCE_EXIT_SEC}s.</div>`;
 }else $('pos').innerHTML='<span class="mut">Flat. Waiting for a clean setup…</span>';
 const T=S.trades,wins=T.filter(t=>t.pnl>0).length,avg=T.length?S.pnl_total/T.length:0;
 const sv=(k,v,c='')=>`<div class="stat"><div class="k">${k}</div><div class="v ${c}">${v}</div></div>`;
 $('stats').innerHTML=sv('PnL today',money(S.pnl_today),cls(S.pnl_today))+sv('PnL total',money(S.pnl_total),cls(S.pnl_total))+
  sv('Trades',T.length)+sv('Win rate',T.length?Math.round(wins/T.length*100)+'%':'–')+sv('Avg / trade',T.length?money(avg):'–');
 $('trades').innerHTML='<tr><th>Time</th><th>Side</th><th>In</th><th>Out</th><th>Why</th><th>PnL</th></tr>'+
  T.slice(-10).reverse().map(t=>`<tr><td>${t.time.slice(11)}</td><td>${t.side}</td><td>${t.entry}</td><td>${t.exit}</td><td>${t.reason}</td><td class="${cls(t.pnl)}">${money(t.pnl)}</td></tr>`).join('');
 const lg=$('log');lg.textContent=S.log.join('\n');
 draw();
}
async function tick(){try{S=await (await fetch('/state',{cache:'no-store'})).json();render();}catch(e){$('status').textContent='bot not reachable';}}
setInterval(()=>{const W=(S&&S.config.WINDOW_SEC)||300,e=(Date.now()/1000)%W,left=W-e;
 $('cd').textContent=Math.floor(left/60)+':'+String(Math.floor(left%60)).padStart(2,'0');
 $('arc').style.strokeDashoffset=301.6*(e/W);},250);
tick();setInterval(tick,1000);window.addEventListener('resize',draw);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, body, ctype):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/state"):
            with lock:
                body = json.dumps(state).encode()
            self._send(body, "application/json")
        else:
            self._send(PAGE.encode(), "text/html; charset=utf-8")

    def do_POST(self):
        try:
            n = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(n) or b"{}")
        except Exception:  # noqa: BLE001
            body = {}
        with lock:
            if self.path == "/control":
                a = body.get("action")
                if a == "pause":
                    state["paused"], state["paused_reason"] = True, "paused by you"
                    log("Paused from dashboard")
                elif a == "resume":
                    state["paused"], state["paused_reason"] = False, ""
                    log("Resumed from dashboard")
                elif a == "reset":
                    state["trades"], state["markers"] = [], []
                    state["pnl_today"] = state["pnl_total"] = 0.0
                    log("Stats reset from dashboard")
            elif self.path == "/config":
                for k, v in body.items():
                    if k in TUNABLE:
                        try:
                            CONFIG[k] = float(v)
                            log(f"Setting {k} = {CONFIG[k]}")
                        except ValueError:
                            pass
        self._send(b'{"ok":true}', "application/json")


def main():
    threading.Thread(target=bot_loop, daemon=True).start()
    log(f"Dashboard at http://localhost:{CONFIG['PORT']}  (paper trading only)")
    ThreadingHTTPServer(("127.0.0.1", CONFIG["PORT"]), Handler).serve_forever()


if __name__ == "__main__":
    main()