import sys
sys.stdout.flush()
print("BOT STARTING NOW", flush=True)

import os
import json
import websocket
import threading
import time
import http.server
import socketserver
from datetime import datetime, timezone

print("Imports successful", flush=True)

# =========================
# CONFIG
# =========================

TOKEN      = os.getenv("DERIV_TOKEN")
APP_ID     = "1089"
SYMBOL     = "frxXAUUSD"
PORT       = int(os.getenv("PORT", 8080))

STAKE      = 5.0       # USD per trade
SL_PIPS    = 20
TP_PIPS    = 60        # 1:3 ratio
PIP_SIZE   = 0.01

SR_MIN     = 50
SR_MAX     = 80
EMA_SLOPE_MIN  = 0.003
CANDLE_SECONDS = 60

# Sessions in UTC (GMT+2 minus 2)
# London open 08:00-11:00 GMT+2 = 06:00-09:00 UTC
# NY open 15:00-18:00 GMT+2 = 13:00-16:00 UTC
SESSIONS_UTC = [(6, 9), (13, 16)]

print(f"TOKEN set: {bool(TOKEN)}", flush=True)
print(f"Token: {TOKEN}", flush=True)

if not TOKEN:
    print("ERROR: DERIV_TOKEN not set. Exiting.", flush=True)
    sys.exit(1)

# =========================
# SHARED STATE
# =========================

prices        = []
active_trade  = None
stop_heartbeat = False
ema_history   = []

candle_open_time  = None
candle_open_price = None
candle_prices_buf = []
last_candle_open  = None
last_candle_close = None

balance      = 0.0
equity_curve = []   # list of {"time": str, "balance": float}
trade_log    = []   # list of trade dicts
drawdown_max = 0.0
peak_balance = 0.0

state_lock = threading.Lock()

# =========================
# SESSION CHECK
# =========================

def is_valid_session():
    hour = datetime.now(timezone.utc).hour
    for start, end in SESSIONS_UTC:
        if start <= hour < end:
            return True
    print(f"[SESSION] Outside sessions. UTC hour={hour}. Skipping.", flush=True)
    return False

# =========================
# CANDLE TRACKER
# =========================

def update_candle(price, epoch):
    global candle_open_time, candle_open_price, candle_prices_buf
    global last_candle_open, last_candle_close

    bucket = (epoch // CANDLE_SECONDS) * CANDLE_SECONDS

    if candle_open_time is None:
        candle_open_time  = bucket
        candle_open_price = price
        candle_prices_buf = [price]
        return False, None, None

    if bucket == candle_open_time:
        candle_prices_buf.append(price)
        return False, None, None

    close_p = candle_prices_buf[-1]
    open_p  = candle_open_price
    last_candle_open  = open_p
    last_candle_close = close_p

    print(f"[CANDLE] Open={open_p} Close={close_p}", flush=True)

    candle_open_time  = bucket
    candle_open_price = price
    candle_prices_buf = [price]

    return True, open_p, close_p

# =========================
# EMA + SLOPE
# =========================

def calc_ema(vals, period=200):
    if len(vals) < period:
        return None
    k = 2 / (period + 1)
    v = vals[0]
    for x in vals[1:]:
        v = x * k + v * (1 - k)
    return v

def ema_sloping_up():
    if len(ema_history) < 2:
        return False
    slope = ema_history[-1] - ema_history[-2]
    if slope < EMA_SLOPE_MIN:
        print(f"[RULE 1] EMA flat/down slope={slope:.5f}. Skip BUY.", flush=True)
        return False
    return True

def ema_sloping_down():
    if len(ema_history) < 2:
        return False
    slope = ema_history[-1] - ema_history[-2]
    if slope > -EMA_SLOPE_MIN:
        print(f"[RULE 1] EMA flat/up slope={slope:.5f}. Skip SELL.", flush=True)
        return False
    return True

# =========================
# SUPPORT / RESISTANCE
# =========================

def get_sr_levels():
    if len(prices) < 30:
        return [], []
    w = prices[-100:] if len(prices) >= 100 else prices[:]
    supports, resistances = [], []
    for i in range(2, len(w) - 2):
        if w[i] < w[i-1] and w[i] < w[i-2] and w[i] < w[i+1] and w[i] < w[i+2]:
            supports.append(w[i])
        if w[i] > w[i-1] and w[i] > w[i-2] and w[i] > w[i+1] and w[i] > w[i+2]:
            resistances.append(w[i])
    return supports, resistances

def near_support(price):
    supports, _ = get_sr_levels()
    for lvl in supports:
        dist = abs(price - lvl)
        if SR_MIN <= dist <= SR_MAX:
            print(f"[RULE 4] Support confluence: price={price} level={lvl} dist={dist:.2f}", flush=True)
            return True
    print(f"[RULE 4] No support near {price}. Skip.", flush=True)
    return False

def near_resistance(price):
    _, resistances = get_sr_levels()
    for lvl in resistances:
        dist = abs(price - lvl)
        if SR_MIN <= dist <= SR_MAX:
            print(f"[RULE 4] Resistance confluence: price={price} level={lvl} dist={dist:.2f}", flush=True)
            return True
    print(f"[RULE 4] No resistance near {price}. Skip.", flush=True)
    return False

# =========================
# SL / TP
# =========================

def calc_sl_tp(direction, entry):
    sl_amt = SL_PIPS * PIP_SIZE
    tp_amt = TP_PIPS * PIP_SIZE
    if direction == "CALL":
        return round(entry - sl_amt, 5), round(entry + tp_amt, 5)
    return round(entry + sl_amt, 5), round(entry - tp_amt, 5)

# =========================
# TRADE MONITOR
# =========================

def monitor_trade(ws_conn, entry, direction, sl, tp, trade_id):
    global active_trade, balance, peak_balance, drawdown_max

    while active_trade is not None:
        if not prices:
            time.sleep(1)
            continue

        cur = prices[-1]
        hit = None

        if direction == "CALL":
            if cur <= sl:
                hit = "SL"
            elif cur >= tp:
                hit = "TP"
        else:
            if cur >= sl:
                hit = "SL"
            elif cur <= tp:
                hit = "TP"

        if hit:
            pnl = (TP_PIPS * PIP_SIZE * 100) if hit == "TP" else -(SL_PIPS * PIP_SIZE * 100)
            result = "WIN" if hit == "TP" else "LOSS"

            with state_lock:
                balance += pnl
                peak_balance = max(peak_balance, balance)
                dd = ((peak_balance - balance) / peak_balance * 100) if peak_balance > 0 else 0
                drawdown_max = max(drawdown_max, dd)

                for t in trade_log:
                    if t["id"] == trade_id:
                        t["result"] = result
                        t["pnl"]    = round(pnl, 2)
                        t["close"]  = cur
                        t["closed_at"] = datetime.now(timezone.utc).strftime("%H:%M:%S")
                        break

                equity_curve.append({
                    "time":    datetime.now(timezone.utc).strftime("%H:%M"),
                    "balance": round(balance, 2)
                })

            print(f"[{hit} HIT] Result={result} PnL={pnl:.2f} Balance={balance:.2f}", flush=True)
            active_trade = None
            break

        time.sleep(1)

# =========================
# OPEN TRADE
# =========================

def open_trade(ws_conn, direction, price):
    global active_trade, trade_log

    if active_trade is not None:
        return

    sl, tp = calc_sl_tp(direction, price)
    trade_id = len(trade_log) + 1

    trade = {
        "id":        trade_id,
        "direction": direction,
        "entry":     price,
        "sl":        sl,
        "tp":        tp,
        "stake":     STAKE,
        "result":    "OPEN",
        "pnl":       0,
        "close":     None,
        "opened_at": datetime.now(timezone.utc).strftime("%H:%M:%S"),
        "closed_at": None
    }

    with state_lock:
        active_trade = trade
        trade_log.append(trade)

    print(f"\n=== TRADE OPENED ===", flush=True)
    print(f"Direction : {direction}", flush=True)
    print(f"Entry     : {price}", flush=True)
    print(f"SL        : {sl}  (-{SL_PIPS} pips)", flush=True)
    print(f"TP        : {tp}  (+{TP_PIPS} pips | 1:3 RR)", flush=True)
    print(f"====================\n", flush=True)

    order = {
        "buy": 1,
        "price": STAKE,
        "parameters": {
            "amount":        STAKE,
            "basis":         "stake",
            "contract_type": direction,
            "currency":      "USD",
            "duration":      1,
            "duration_unit": "m",
            "symbol":        SYMBOL
        }
    }
    ws_conn.send(json.dumps(order))

    threading.Thread(
        target=monitor_trade,
        args=(ws_conn, price, direction, sl, tp, trade_id),
        daemon=True
    ).start()

# =========================
# STRATEGY — ALL 5 RULES
# =========================

def strategy_on_candle_close(ws_conn, close_price):
    global ema_history

    ema_val = calc_ema(prices, 200)
    if ema_val is None:
        print(f"[WAIT] Need 200 prices. Have {len(prices)}.", flush=True)
        return

    ema_history.append(ema_val)
    if len(ema_history) > 50:
        ema_history.pop(0)

    print(f"[EMA] {ema_val:.3f} Price={close_price:.3f}", flush=True)

    # Rule 5 — session
    if not is_valid_session():
        return

    above = close_price > ema_val
    below = close_price < ema_val

    if above:
        # Rule 1 — slope up
        if not ema_sloping_up():
            return
        # Rule 2 — price above EMA (already confirmed)
        # Rule 3 — bullish candle close
        if last_candle_open and close_price <= last_candle_open:
            print(f"[RULE 3] Candle not bullish. Skip.", flush=True)
            return
        print(f"[RULE 3] Bullish candle confirmed.", flush=True)
        # Rule 4 — near support
        if not near_support(close_price):
            return
        print(f"[ALL RULES PASSED] BUY confirmed.", flush=True)
        open_trade(ws_conn, "CALL", close_price)

    elif below:
        # Rule 1 — slope down
        if not ema_sloping_down():
            return
        # Rule 3 — bearish candle close
        if last_candle_open and close_price >= last_candle_open:
            print(f"[RULE 3] Candle not bearish. Skip.", flush=True)
            return
        print(f"[RULE 3] Bearish candle confirmed.", flush=True)
        # Rule 4 — near resistance
        if not near_resistance(close_price):
            return
        print(f"[ALL RULES PASSED] SELL confirmed.", flush=True)
        open_trade(ws_conn, "PUT", close_price)

# =========================
# HEARTBEAT
# =========================

def heartbeat_loop(ws_conn):
    global stop_heartbeat
    while not stop_heartbeat:
        try:
            if ws_conn and ws_conn.sock and ws_conn.sock.connected:
                ws_conn.send(json.dumps({"ping": 1}))
                print("[HEARTBEAT] Ping sent", flush=True)
        except Exception as e:
            print(f"[HEARTBEAT] Error: {e}", flush=True)
        time.sleep(25)

# =========================
# WEBSOCKET
# =========================

def on_open(ws_conn):
    global stop_heartbeat, balance, peak_balance
    print("[WS] Connected", flush=True)
    stop_heartbeat = False
    threading.Thread(target=heartbeat_loop, args=(ws_conn,), daemon=True).start()
    ws_conn.send(json.dumps({"authorize": TOKEN}))

def on_message(ws_conn, message):
    print("RAW MESSAGE:", message, flush=True)
    global prices, balance
    data = json.loads(message)

    if "error" in data:
        print(f"[DERIV ERROR] {data['error']}", flush=True)

    if "authorize" in data:
        bal = data["authorize"].get("balance", 0)
        with state_lock:
            balance      = float(bal)
            peak_balance = balance
            equity_curve.append({
                "time":    datetime.now(timezone.utc).strftime("%H:%M"),
                "balance": round(balance, 2)
            })
        print(f"[AUTH] Authenticated. Balance={balance}", flush=True)
        ws_conn.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))

    if "pong" in data:
        print("[HEARTBEAT] Pong", flush=True)

    if "tick" in data:
        price = data["tick"]["quote"]
        epoch = data["tick"]["epoch"]
        prices.append(price)
        if len(prices) > 1000:
            prices.pop(0)
        print(f"[TICK] {price}", flush=True)
        closed, open_p, close_p = update_candle(price, epoch)
        if closed:
            strategy_on_candle_close(ws_conn, close_p)

    if "buy" in data:
        print(f"[ORDER] {data['buy']}", flush=True)

def on_error(ws_conn, error):
    print(f"[WS ERROR] {error}", flush=True)

def on_close(ws_conn, *args):
    global stop_heartbeat
    stop_heartbeat = True
    print(f"[WS CLOSED] {args}", flush=True)

# =========================
# DASHBOARD (web server)
# =========================

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Deriv Bot Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:system-ui,sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh}
  header{background:#1a1f2e;padding:16px 24px;border-bottom:1px solid #2d3748;display:flex;align-items:center;gap:16px}
  header h1{font-size:18px;font-weight:600;color:#fff}
  .badge{padding:4px 10px;border-radius:20px;font-size:12px;font-weight:500}
  .badge.live{background:#1a3a2a;color:#48bb78}
  .badge.waiting{background:#2d2d1a;color:#ecc94b}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:16px;padding:24px}
  .card{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;padding:16px}
  .card .label{font-size:12px;color:#718096;margin-bottom:6px}
  .card .value{font-size:24px;font-weight:600}
  .card .value.green{color:#48bb78}
  .card .value.red{color:#fc8181}
  .card .value.yellow{color:#ecc94b}
  .section{padding:0 24px 24px}
  .section h2{font-size:14px;font-weight:600;color:#a0aec0;margin-bottom:12px;text-transform:uppercase;letter-spacing:.05em}
  .chart-wrap{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;padding:16px;height:220px}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th{text-align:left;padding:8px 12px;color:#718096;font-weight:500;border-bottom:1px solid #2d3748}
  td{padding:8px 12px;border-bottom:1px solid #1a1f2e}
  tr:hover td{background:#1e2535}
  .pill{padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
  .pill.win{background:#1a3a2a;color:#48bb78}
  .pill.loss{background:#3a1a1a;color:#fc8181}
  .pill.open{background:#2d2d1a;color:#ecc94b}
  .tbl-wrap{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;overflow:hidden}
</style>
</head>
<body>
<header>
  <h1>Deriv Bot — XAU/USD</h1>
  <span class="badge live" id="status-badge">● LIVE</span>
</header>

<div class="grid" id="stats">
  <div class="card"><div class="label">Balance</div><div class="value" id="balance">—</div></div>
  <div class="card"><div class="label">Total Trades</div><div class="value" id="total">—</div></div>
  <div class="card"><div class="label">Win Rate</div><div class="value green" id="winrate">—</div></div>
  <div class="card"><div class="label">Total P&L</div><div class="value" id="pnl">—</div></div>
  <div class="card"><div class="label">Max Drawdown</div><div class="value red" id="dd">—</div></div>
  <div class="card"><div class="label">Active Trade</div><div class="value yellow" id="active">None</div></div>
</div>

<div class="section">
  <h2>Equity Curve</h2>
  <div class="chart-wrap"><canvas id="chart"></canvas></div>
</div>

<div class="section">
  <h2>Trade History</h2>
  <div class="tbl-wrap">
    <table>
      <thead><tr><th>#</th><th>Direction</th><th>Entry</th><th>SL</th><th>TP</th><th>Close</th><th>P&L</th><th>Result</th><th>Time</th></tr></thead>
      <tbody id="trade-tbody"></tbody>
    </table>
  </div>
</div>

<script>
const ctx = document.getElementById('chart').getContext('2d');
const chart = new Chart(ctx, {
  type: 'line',
  data: { labels: [], datasets: [{ label: 'Balance', data: [], borderColor: '#48bb78', backgroundColor: 'rgba(72,187,120,0.08)', tension: 0.3, pointRadius: 2, fill: true }] },
  options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { ticks: { color: '#718096' }, grid: { color: '#2d3748' } }, y: { ticks: { color: '#718096' }, grid: { color: '#2d3748' } } } }
});

async function refresh() {
  try {
    const r = await fetch('/api/state');
    const d = await r.json();

    document.getElementById('balance').textContent  = '$' + d.balance.toFixed(2);
    document.getElementById('total').textContent    = d.total_trades;
    document.getElementById('winrate').textContent  = d.win_rate + '%';

    const pnlEl = document.getElementById('pnl');
    pnlEl.textContent = (d.total_pnl >= 0 ? '+' : '') + '$' + d.total_pnl.toFixed(2);
    pnlEl.className   = 'value ' + (d.total_pnl >= 0 ? 'green' : 'red');

    document.getElementById('dd').textContent    = d.max_drawdown.toFixed(1) + '%';
    document.getElementById('active').textContent = d.active_trade ? d.active_trade.direction + ' @ ' + d.active_trade.entry : 'None';

    chart.data.labels   = d.equity.map(e => e.time);
    chart.data.datasets[0].data = d.equity.map(e => e.balance);
    chart.update('none');

    const tbody = document.getElementById('trade-tbody');
    tbody.innerHTML = d.trades.slice().reverse().map(t => `
      <tr>
        <td>${t.id}</td>
        <td>${t.direction}</td>
        <td>${t.entry}</td>
        <td>${t.sl}</td>
        <td>${t.tp}</td>
        <td>${t.close || '—'}</td>
        <td class="${t.pnl > 0 ? 'green' : t.pnl < 0 ? 'red' : ''}">${t.pnl !== 0 ? (t.pnl > 0 ? '+' : '') + '$' + t.pnl.toFixed(2) : '—'}</td>
        <td><span class="pill ${t.result.toLowerCase()}">${t.result}</span></td>
        <td>${t.opened_at}</td>
      </tr>`).join('');
  } catch(e) {
    console.error(e);
  }
}

refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>"""

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass  # suppress access logs

    def do_GET(self):
        if self.path == "/api/state":
            with state_lock:
                trades    = list(trade_log)
                equity    = list(equity_curve)
                bal       = balance
                dd        = drawdown_max
                at        = dict(active_trade) if active_trade else None

            wins  = [t for t in trades if t["result"] == "WIN"]
            losses= [t for t in trades if t["result"] == "LOSS"]
            total = len([t for t in trades if t["result"] != "OPEN"])
            wr    = round(len(wins) / total * 100, 1) if total > 0 else 0
            pnl   = sum(t["pnl"] for t in trades)

            payload = json.dumps({
                "balance":       round(bal, 2),
                "total_trades":  total,
                "win_rate":      wr,
                "total_pnl":     round(pnl, 2),
                "max_drawdown":  round(dd, 2),
                "active_trade":  at,
                "equity":        equity[-100:],
                "trades":        trades[-50:]
            }).encode()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(payload)

        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(DASHBOARD_HTML.encode())

def start_dashboard():
    server = socketserver.TCPServer(("0.0.0.0", PORT), Handler)
    print(f"[DASHBOARD] Running on port {PORT}", flush=True)
    server.serve_forever()

# =========================
# MAIN
# =========================

# Start dashboard in background
threading.Thread(target=start_dashboard, daemon=True).start()

# WebSocket loop with reconnect
retry_count    = 0
max_retry_wait = 60

while True:
    try:
        ws = websocket.WebSocketApp(
            f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}",
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever(ping_interval=25, ping_timeout=10)
    except Exception as e:
        print(f"[RECONNECT] {e}", flush=True)
        retry_count += 1
        wait = min(5 * (2 ** retry_count), max_retry_wait)
        print(f"[RECONNECT] Retry in {wait}s (attempt {retry_count})...", flush=True)
        time.sleep(wait)
